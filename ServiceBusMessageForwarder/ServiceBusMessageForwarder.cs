using System;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Configuration;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Forms;
using ServiceBusMessageForwarder.Forwarders;
using ServiceBusMessageForwarder.Logging;

namespace ServiceBusMessageForwarder
{
    public class ServiceBusMessageForwarder
    {
        private readonly string _sourceConnectionString;
        private readonly string _destinationConnectionString;

        private readonly NamespaceManager _sourceNamespaceManager;
        private readonly NamespaceManager _destinationNamespaceManager;

        private readonly string[] _ignoreQueuesPatterns;
        private readonly string[] _ignoreTopicPatterns;
        private readonly string[] _ignoreSubscriptionsPatterns;


        private readonly ILogger _activityLogger;
        private readonly ILogger _messageLogger;

        private readonly SubscriptionMessageForwarder _subscriptionMessageForwarder;
        private readonly QueueMessageForwarder _queueMessageForwarder;
        
        public ServiceBusMessageForwarder(ILogger activityLogger, ILogger messageLogger, string sourceConnectionString, string destinationConnectionString, 
            string ignoreQueues, string ignoreTopics, string ignoreSubscriptions, int messagesToHandle = 10)
        {
            _activityLogger = activityLogger;
            _messageLogger = messageLogger;

            _subscriptionMessageForwarder = new SubscriptionMessageForwarder(_activityLogger, _messageLogger, messagesToHandle);
            _queueMessageForwarder = new QueueMessageForwarder(_activityLogger, _messageLogger, messagesToHandle);

            _sourceConnectionString = sourceConnectionString;
            _destinationConnectionString = destinationConnectionString;

            _sourceNamespaceManager = NamespaceManager.CreateFromConnectionString(_sourceConnectionString);
            _destinationNamespaceManager = NamespaceManager.CreateFromConnectionString(_destinationConnectionString);

            _ignoreQueuesPatterns = ignoreQueues.Split(',').Where(_ => !string.IsNullOrWhiteSpace(_)).ToArray();
            _ignoreTopicPatterns = ignoreTopics.Split(',').Where(_ => !string.IsNullOrWhiteSpace(_)).ToArray();
            _ignoreSubscriptionsPatterns = ignoreSubscriptions.Split(',').Where(_ => !string.IsNullOrWhiteSpace(_)).ToArray();
        }
        
        public void Run()
        {
            try
            {
                _activityLogger.Log("Running");

                ProcessQueues();

                ProcessTopics();

                _activityLogger.Log("Finished running");
            }
            catch (Exception e)
            {
                _activityLogger.Log($"! Exception: {e.Message}\n\n", 0, 2);
            }
        }

        private void ProcessQueues()
        {
            var queues = _sourceNamespaceManager.GetQueues();
            var destinationQueues = _destinationNamespaceManager.GetQueues().Select(queue => queue.Path);
            
            _activityLogger.Log($"{queues.Count()} queue(s) found");

            foreach (var queue in queues)
            {
                if (IsQueueIgnored(queue.Path))
                    _activityLogger.Log($"Ignoring queue: [{queue}]");

                else if (!destinationQueues.Contains(queue.Path))
                    _activityLogger.Log($"Skipping queue, which does not exist in destination: [{queue}]");

                else
                {
                    QueueClient sourceClient = null;
                    QueueClient destinationClient = null;

                    try
                    {
                        sourceClient = QueueClient.CreateFromConnectionString(_sourceConnectionString, queue.Path);
                        destinationClient = QueueClient.CreateFromConnectionString(_destinationConnectionString, queue.Path);

                        if (queue.RequiresSession)
                            _queueMessageForwarder.ProcessSessionQueue(sourceClient, destinationClient);
                        else
                            _queueMessageForwarder.ProcessQueue(sourceClient, destinationClient);
                    }
                    catch (Exception e)
                    {
                        _activityLogger.Log($"! Exception processing [{queue.Path}] queue: {e.Message}\n\n", 0, 2);
                    }
                    finally
                    {
                        sourceClient?.Close();
                        destinationClient?.Close();
                    }
                }
            }
        }
        
        private void ProcessTopics()
        {
            var topics = _sourceNamespaceManager.GetTopics().Select(topic => topic.Path);
            var destinationTopics = _destinationNamespaceManager.GetTopics().Select(topic => topic.Path);

            _activityLogger.Log($"{topics.Count()} topic(s) found");

            foreach (var topic in topics)
            {
                if (IsTopicIgnored(topic))
                    _activityLogger.Log($"Ignoring topic: [{topic}]");

                else if (!destinationTopics.Contains(topic))
                    _activityLogger.Log($"Skipping topic, which does not exist in destination: [{topic}]");

                else
                    ProcessTopic(topic);
            }
        }

        private void ProcessTopic(string topic)
        {
            _activityLogger.Log($"[{topic}] - Processing topic ", 0, 1);
            
            // get subscriptions in source topic
            var subscriptions = _sourceNamespaceManager.GetSubscriptions(topic);

            _activityLogger.Log($"[{topic}] - {subscriptions.Count()} subscription(s) found");

            // forward messages in each subscription to destination topic
            foreach (var subscription in subscriptions)
            {
                SubscriptionClient subscriptionClient = null;
                TopicClient destinationTopicClient = null;

                try
                {
                    if (IsSubscriptionIgnored(subscription.Name))
                    {
                        _activityLogger.Log($"Ignoring subscription: [{topic}].[{subscription.Name}]");
                    }
                    else
                    {
                        subscriptionClient =
                            SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topic, subscription.Name);
                        destinationTopicClient =
                            TopicClient.CreateFromConnectionString(_destinationConnectionString, topic);

                        if (subscription.RequiresSession)
                            _subscriptionMessageForwarder.ProcessSessionSubscription(subscriptionClient, destinationTopicClient);
                        else
                            _subscriptionMessageForwarder.ProcessSubscription(subscriptionClient, destinationTopicClient);
                    }
                }
                catch (Exception e)
                {
                    _activityLogger.Log($"! Exception processing [{topic}].[{subscription.Name}] subscription: {e.Message}\n\n",
                        0, 2);
                }
                finally
                {
                    subscriptionClient?.Close();
                    destinationTopicClient?.Close();
                }
            }

            _activityLogger.Log($"[{topic}] - Completed processing topic");
        }
        
        private bool IsQueueIgnored(string queue) =>
            _ignoreQueuesPatterns.Any(pattern => Regex.IsMatch(queue, pattern, RegexOptions.IgnoreCase));

        private bool IsTopicIgnored(string topic) => 
            _ignoreTopicPatterns.Any(pattern => Regex.IsMatch(topic, pattern, RegexOptions.IgnoreCase));

        private bool IsSubscriptionIgnored(string subscription) => 
            _ignoreSubscriptionsPatterns.Any(pattern => Regex.IsMatch(subscription, pattern, RegexOptions.IgnoreCase));


        private static void Main(string[] args)
        {
            if (!int.TryParse(ConfigurationManager.AppSettings["MessagesToHandleAtOnce"], out int messagesToHandle))
                messagesToHandle = 10;

            if (!int.TryParse(ConfigurationManager.AppSettings["ServiceSleepTimeSeconds"], out int serviceSleepTimeSeconds))
                serviceSleepTimeSeconds = 10;

            var messageLogger = bool.TryParse(ConfigurationManager.AppSettings["LogMessages"], out bool logMessages) && logMessages ?
                new Logger($"SBMF_MESSAGE_LOG_{DateTime.UtcNow:yyyyMMdd}.log") : null;

            using (var activityLogger = new Logger($"SBMF_ACTIVITY_LOG_{DateTime.UtcNow:yyyyMMdd}.log"))
            {
                var service = new ServiceBusMessageForwarder(
                    activityLogger,
                    messageLogger,
                    ConfigurationManager.AppSettings["SourceConnectionString"],
                    ConfigurationManager.AppSettings["DestinationConnectionString"],
                    ConfigurationManager.AppSettings["IgnoreQueues"],
                    ConfigurationManager.AppSettings["IgnoreTopics"],
                    ConfigurationManager.AppSettings["IgnoreSubscriptions"],
                    messagesToHandle);

                var keepRunning = true;

                while (keepRunning)
                {
                    service.Run();

                    Console.WriteLine($"\n\n -- Sleeping for {serviceSleepTimeSeconds} seconds - Press a key to exit --\n\n");

                    var keyWasPressed = Task.Factory.StartNew(Console.ReadKey).Wait(TimeSpan.FromSeconds(serviceSleepTimeSeconds));

                    if (keyWasPressed)
                        keepRunning = false;
                    else
                        SendKeys.SendWait("{ENTER}"); // clear the uncaptured Console.ReadKey()
                }
            }

            messageLogger?.Dispose();
        }
        
    }
}

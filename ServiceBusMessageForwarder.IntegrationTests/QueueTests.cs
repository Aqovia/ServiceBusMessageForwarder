using System;
using System.Configuration;
using System.Linq;
using FluentAssertions;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using ServiceBusMessageForwarder.IntegrationTests.Logging;
using ServiceBusMessageForwarder.IntegrationTests.Models;
using ServiceBusMessageForwarder.Logging;
using Xbehave;
using Xunit;

namespace ServiceBusMessageForwarder.IntegrationTests
{
    [Collection("TestCollection")]
    public class QueueTests
    {
        private readonly NamespaceManager _sourceNamespaceManager;
        private readonly NamespaceManager _destinationNamespaceManager;
        private readonly ILogger _logger = new MockLogger();
        private readonly string _sourceConnectionString = ConfigurationManager.AppSettings["SourceConnectionString"];
        private readonly string _destinationConnectionString = ConfigurationManager.AppSettings["DestinationConnectionString"];
        private readonly string _ignoreTopics = "";
        private readonly string _ignoreSubscriptions = "";
        private readonly Message _testMessage = new Message { Id = 3011, Content = "Hello, World!" };

        public QueueTests()
        {
            _sourceNamespaceManager = NamespaceManager.CreateFromConnectionString(_sourceConnectionString);
            _destinationNamespaceManager = NamespaceManager.CreateFromConnectionString(_destinationConnectionString);
        }
        
        [Scenario]
        public void ForwardMessageWhenQueueExistsInDestination()
        {
            var queueName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var ignoreQueues = "";

            var sourceClient = QueueClient.CreateFromConnectionString(_sourceConnectionString, queueName);
            var destinationClient = QueueClient.CreateFromConnectionString(_destinationConnectionString, queueName);

            "Given a queue exists on the destination bus".x(() =>
            {
                _destinationNamespaceManager.CreateQueue(queueName);
            });
            "And a message is posted on the source queue".x(() =>
            {
                _sourceNamespaceManager.CreateQueue(queueName);
                sourceClient.Send(new BrokeredMessage(_testMessage));
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, ignoreQueues, _ignoreTopics, _ignoreSubscriptions).Run();
            });
            "Then the message is forwarded to the destination queue".x(() =>
            {
                var messages = destinationClient.PeekBatch(10);

                messages.Count().Should().Be(1);
                messages.First().GetBody<Message>().Id.Should().Be(_testMessage.Id);
            });
            "And the message no longer exists on the source queue".x(() =>
            {
                var messages = sourceClient.PeekBatch(10);
                messages.Count().Should().Be(0);
            });

            CleanupQueues(queueName);
        }

        [Scenario]
        public void DoNotForwardMessageWhenQueueDoesNotExistInDestination()
        {
            var queueName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var ignoreQueues = "";

            var sourceClient = QueueClient.CreateFromConnectionString(_sourceConnectionString, queueName);

            "Given a queue does not exist on the destination bus".x(() =>
            {
                _destinationNamespaceManager.DeleteQueue(queueName);
            });
            "And a message is posted on the source queue".x(() =>
            {
                _sourceNamespaceManager.CreateQueue(queueName);
                sourceClient.Send(new BrokeredMessage(_testMessage));
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, ignoreQueues, _ignoreTopics, _ignoreSubscriptions).Run();
            });
            "Then the message is not forwarded to the destination".x(() =>
            {
                _destinationNamespaceManager.QueueExists(queueName).Should().BeFalse();
            });
            "And the message still exists on the source queue".x(() =>
            {
                var messages = sourceClient.PeekBatch(10);
                messages.Count().Should().Be(1);
            });

            CleanupQueues(queueName);
        }

        [Scenario]
        public void DoNotForwardMessagesFromIgnoredQueues()
        {
            var queueName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var ignoreQueues = "";

            var sourceClient = QueueClient.CreateFromConnectionString(_sourceConnectionString, queueName);
            var destinationClient = QueueClient.CreateFromConnectionString(_destinationConnectionString, queueName);

            "Given a queue exists on the destination bus".x(() =>
            {
                _destinationNamespaceManager.CreateQueue(queueName);
            });
            "And the queue is included in the ignored queues list".x(() =>
            {
                ignoreQueues = queueName;
            });
            "And a message is posted on the source queue".x(() =>
            {
                _sourceNamespaceManager.CreateQueue(queueName);
                sourceClient.Send(new BrokeredMessage(_testMessage));
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, ignoreQueues, _ignoreTopics, _ignoreSubscriptions).Run();
            });
            "Then the message is not forwarded to the destination".x(() =>
            {
                var messages = destinationClient.PeekBatch(10);
                messages.Count().Should().Be(0);
            });
            "And the message still exists on the source queue".x(() =>
            {
                var messages = sourceClient.PeekBatch(10);
                messages.Count().Should().Be(1);
            });

            CleanupQueues(queueName);
        }

        [Scenario]
        public void ForwardMessagesOnAQueueRequiringSessions()
        {
            var queueName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var sessionId1 = "101";
            var sessionId2 = "201";
            var ignoreQueues = "";

            var sourceClient = QueueClient.CreateFromConnectionString(_sourceConnectionString, queueName);

            "Given a queue exists on the destination bus which requires sessions".x(() =>
            {
                _destinationNamespaceManager.CreateQueue(new QueueDescription(queueName) { RequiresSession = true });
            });
            "And 2 messages are sent to the source queue with different session IDs".x(() =>
            {
                _sourceNamespaceManager.CreateQueue(new QueueDescription(queueName) { RequiresSession = true });
                sourceClient.Send(new BrokeredMessage(_testMessage) { SessionId = sessionId1 });
                sourceClient.Send(new BrokeredMessage(_testMessage) { SessionId = sessionId2 });
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, ignoreQueues, _ignoreTopics, _ignoreSubscriptions).Run();
            });
            "Then the messages are forwarded to the destination queue".x(() =>
            {
                var destinationQueueClient = QueueClient.CreateFromConnectionString(_destinationConnectionString, queueName);

                var messageSession1 = destinationQueueClient.AcceptMessageSession(sessionId1);
                var session1Messages = messageSession1.PeekBatch(10);
                session1Messages.Count().Should().Be(1);
                session1Messages.First().GetBody<Message>().Id.Should().Be(_testMessage.Id);

                var messageSession2 = destinationQueueClient.AcceptMessageSession(sessionId2);
                var session2Messages = messageSession2.PeekBatch(10);
                session2Messages.Count().Should().Be(1);
                session2Messages.First().GetBody<Message>().Id.Should().Be(_testMessage.Id);
            });
            "And the message no longer exists in the source queue's queue".x(() =>
            {
                var sourceQueuenClient = QueueClient.CreateFromConnectionString(_sourceConnectionString, queueName);

                var messageSession1 = sourceQueuenClient.AcceptMessageSession(sessionId1);
                var session1Messages = messageSession1.PeekBatch(10);
                session1Messages.Count().Should().Be(0);

                var messageSession2 = sourceQueuenClient.AcceptMessageSession(sessionId2);
                var session2Messages = messageSession2.PeekBatch(10);
                session2Messages.Count().Should().Be(0);
            });

            CleanupQueues(queueName);
        }

        private void CleanupQueues(string queueName)
        {
            _sourceNamespaceManager.DeleteQueue(queueName);
            _destinationNamespaceManager.DeleteQueue(queueName);
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.ServiceBus.Messaging;
using ServiceBusMessageForwarder.Extensions;
using ServiceBusMessageForwarder.Logging;

namespace ServiceBusMessageForwarder.Forwarders
{
    public class SubscriptionMessageForwarder
    {
        private readonly ILogger _activityLogger;
        private readonly ILogger _messageLogger;
        private readonly TimeSpan _serverWaitTime = TimeSpan.FromSeconds(0.2);
        private readonly int _messagesToHandle;
        private readonly List<string> _forwardedMessageIds;

        public SubscriptionMessageForwarder(ILogger activityLogger, ILogger messageLogger,  int messagesToHandle = 10)
        {
            _activityLogger = activityLogger;
            _messageLogger = messageLogger;

            _messagesToHandle = messagesToHandle;
            _forwardedMessageIds = new List<string>();
        }

        public void ProcessSessionSubscription(SubscriptionClient subscriptionClient, TopicClient destinationTopicClient)
        {
            _activityLogger.Log($"[{subscriptionClient.TopicPath}].[{subscriptionClient.Name}] - Processing subscription requiring a session");
            
            var totalMessagesForwarded = 0;

            var messageSessions = subscriptionClient.GetMessageSessions();
            if (messageSessions.Any())
            {
                var sessionIds = new List<string>();
                
                // obtain session IDs and then close these sessions, as they are browse-only
                foreach (var messageSession in messageSessions)
                {
                    sessionIds.Add(messageSession.SessionId);
                    messageSession.Close();
                }

                foreach (var sessionId in sessionIds)
                {
                    _activityLogger.Log($"[{subscriptionClient.TopicPath}].[{subscriptionClient.Name}] - Processing subscription session ID: {sessionId} ");
                    
                    var messagesRemaining = true;
                    
                    while (messagesRemaining)
                    {
                        var messageSession = subscriptionClient.AcceptMessageSession(sessionId, _serverWaitTime);
                        var messages = messageSession.ReceiveBatch(_messagesToHandle, _serverWaitTime);

                        var messageCount = messages.Count();
                        if (messageCount > 0)
                        {
                            _activityLogger.Log($"Batch of {messageCount} message(s) received for processing", 1);

                            var messagesForwarded = 0;
                            
                            foreach (var message in messages)
                            {
                                if (!_forwardedMessageIds.Contains(message.MessageId)) // ignore duplicate messages, which have already been forwarded
                                {
                                    // log message
                                    _messageLogger?.Log($"Subscription: [{subscriptionClient.TopicPath}].[{subscriptionClient.Name}]\n" +
                                                        $"{message.GetSingleLineContent()}\n");

                                    // send messages to destination topic    
                                    destinationTopicClient.Send(message.Clone());
                                    
                                    _forwardedMessageIds.Add(message.MessageId);

                                    messagesForwarded++;
                                    totalMessagesForwarded++;
                                }

                                message.Complete();
                            }
                            
                            _activityLogger.Log($"Processing complete: {messagesForwarded} message(s) forwarded " +
                                        $"({messageCount - messagesForwarded} duplicate(s) from other subscriptions)", 1);
                        }
                        else
                        {
                            messagesRemaining = false;
                            _activityLogger.Log($"No {(totalMessagesForwarded > 0 ? "more " : "")}messages to process in this session", 1);
                        }

                        messageSession.Close();
                    }
                }
            }
            else
            {
                _activityLogger.Log("No sessions exist - no messages to forward", 1);
            }

            _activityLogger.Log($"[{subscriptionClient.TopicPath}].[{subscriptionClient.Name}] - Completed processing subscription - {totalMessagesForwarded} message(s) forwarded");
        }

        public void ProcessSubscription(SubscriptionClient subscriptionClient, TopicClient destinationTopicClient)
        {
            _activityLogger.Log($"[{subscriptionClient.TopicPath}].[{subscriptionClient.Name}] - Processing subscription");
            
            var totalMessagesForwarded = 0;

            var messagesRemaining = true;

            while (messagesRemaining)
            {
                // get messages in source subscription
                var messages = subscriptionClient.ReceiveBatch(_messagesToHandle, _serverWaitTime);

                var messageCount = messages.Count();
                if (messageCount > 0)
                {
                    _activityLogger.Log($"Batch of {messageCount} message(s) received for processing", 1);

                    var messagesForwarded = 0;

                    foreach (var message in messages)
                    {
                        if (!_forwardedMessageIds.Contains(message.MessageId)) // ignore duplicate messages, which have already been forwarded
                        {
                            // log message
                            _messageLogger?.Log($"Subscription: [{subscriptionClient.TopicPath}].[{subscriptionClient.Name}]\n" +
                                                $"{message.GetSingleLineContent()}\n");

                            // send messages to destination topic    
                            destinationTopicClient.Send(message.Clone());

                            _forwardedMessageIds.Add(message.MessageId);

                            messagesForwarded++;
                            totalMessagesForwarded++;
                        }

                        message.Complete();
                    }

                    _activityLogger.Log($"Processing complete: {messagesForwarded} message(s) forwarded " +
                                $"({messageCount - messagesForwarded} duplicate(s) from other subscriptions)", 1);
                }
                else
                {
                    messagesRemaining = false;
                    _activityLogger.Log($"No {(totalMessagesForwarded > 0 ? "more " : "")}messages to process", 1);
                }
            }

            _activityLogger.Log($"[{subscriptionClient.TopicPath}].[{subscriptionClient.Name}] - Completed processing subscription - {totalMessagesForwarded} message(s) forwarded");
        }
    }
}

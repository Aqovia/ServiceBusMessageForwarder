using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.ServiceBus.Messaging;
using ServiceBusMessageForwarder.Logging;

namespace ServiceBusMessageForwarder.Forwarders
{
    public class SubscriptionMessageForwarder
    {
        private readonly ILogger _logger;
        private readonly TimeSpan _serverWaitTime = TimeSpan.FromSeconds(0.2);
        private readonly int _messagesToHandle;

        public SubscriptionMessageForwarder(ILogger logger,  int messagesToHandle = 10)
        {
            _logger = logger;
            _messagesToHandle = messagesToHandle;
        }
        
        public void ProcessSessionSubscription(SubscriptionClient subscriptionClient, TopicClient destinationTopicClient, List<string> messageIds)
        {
            _logger.Log($"[{subscriptionClient.TopicPath}].[{subscriptionClient.Name}] - Processing subscription requiring a session");
            
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
                    _logger.Log($"[{subscriptionClient.TopicPath}].[{subscriptionClient.Name}] - Processing subscription session ID: {sessionId} ");
                    
                    var messagesRemaining = true;
                    
                    while (messagesRemaining)
                    {
                        var messageSession = subscriptionClient.AcceptMessageSession(sessionId, _serverWaitTime);
                        var messages = messageSession.ReceiveBatch(_messagesToHandle, _serverWaitTime);

                        var messageCount = messages.Count();
                        if (messageCount > 0)
                        {
                            _logger.Log($"Batch of {messageCount} message(s) received for processing", 1);

                            var messagesForwarded = 0;
                            
                            foreach (var message in messages)
                            {
                                if (!messageIds.Contains(message.MessageId)) // ignore duplicate messages, which have already been forwarded
                                {
                                    // send messages to destination topic    
                                    destinationTopicClient.Send(message.Clone());

                                    messageIds.Add(message.MessageId);

                                    messagesForwarded++;
                                    totalMessagesForwarded++;
                                }

                                message.Complete();
                            }
                            
                            _logger.Log($"Processing complete: {messagesForwarded} message(s) forwarded " +
                                        $"({messageCount - messagesForwarded} duplicate(s) from other subscriptions)", 1);
                        }
                        else
                        {
                            messagesRemaining = false;
                            _logger.Log($"No {(totalMessagesForwarded > 0 ? "more " : "")}messages to process in this session", 1);
                        }

                        messageSession.Close();
                    }
                }
            }
            else
            {
                _logger.Log("No sessions exist - no messages to forward", 1);
            }

            _logger.Log($"[{subscriptionClient.TopicPath}].[{subscriptionClient.Name}] - Completed processing subscription - {totalMessagesForwarded} message(s) forwarded");
        }

        public void ProcessSubscription(SubscriptionClient subscriptionClient, TopicClient destinationTopicClient, List<string> messageIds)
        {
            _logger.Log($"[{subscriptionClient.TopicPath}].[{subscriptionClient.Name}] - Processing subscription");
            
            var totalMessagesForwarded = 0;

            var messagesRemaining = true;

            while (messagesRemaining)
            {
                // get messages in source subscription
                var messages = subscriptionClient.ReceiveBatch(_messagesToHandle, _serverWaitTime);

                var messageCount = messages.Count();
                if (messageCount > 0)
                {
                    _logger.Log($"Batch of {messageCount} message(s) received for processing", 1);

                    var messagesForwarded = 0;

                    foreach (var message in messages)
                    {
                        if (!messageIds.Contains(message.MessageId)) // ignore duplicate messages, which have already been forwarded
                        {
                            // send messages to destination topic    
                            destinationTopicClient.Send(message.Clone());

                            messageIds.Add(message.MessageId);

                            messagesForwarded++;
                            totalMessagesForwarded++;
                        }

                        message.Complete();
                    }

                    _logger.Log($"Processing complete: {messagesForwarded} message(s) forwarded " +
                                $"({messageCount - messagesForwarded} duplicate(s) from other subscriptions)", 1);
                }
                else
                {
                    messagesRemaining = false;
                    _logger.Log($"No {(totalMessagesForwarded > 0 ? "more " : "")}messages to process", 1);
                }
            }

            _logger.Log($"[{subscriptionClient.TopicPath}].[{subscriptionClient.Name}] - Completed processing subscription - {totalMessagesForwarded} message(s) forwarded");
        }
    }
}

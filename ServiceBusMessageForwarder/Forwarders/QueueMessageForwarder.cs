using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.ServiceBus.Messaging;
using ServiceBusMessageForwarder.Logging;

namespace ServiceBusMessageForwarder.Forwarders
{
    public class QueueMessageForwarder
    {
        private readonly ILogger _logger;
        private readonly TimeSpan _serverWaitTime = TimeSpan.FromSeconds(0.2);
        private readonly int _messagesToHandle;

        public QueueMessageForwarder(ILogger logger,  int messagesToHandle = 10)
        {
            _logger = logger;
            _messagesToHandle = messagesToHandle;
        }

        public void ProcessSessionQueue(QueueClient sourceClient, QueueClient destinationClient)
        {
            _logger.Log($"[{sourceClient.Path}] - Processing queue requiring a session", 0, 1);

            var totalMessagesForwarded = 0;

            var messageSessions = sourceClient.GetMessageSessions();
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
                    _logger.Log($"[{sourceClient.Path}] - Processing queue session ID: {sessionId} ");
                    
                    var messagesRemaining = true;
                    
                    while (messagesRemaining)
                    {
                        var messageSession = sourceClient.AcceptMessageSession(sessionId, _serverWaitTime);
                        var messages = messageSession.ReceiveBatch(_messagesToHandle, _serverWaitTime);

                        var messageCount = messages.Count();
                        if (messageCount > 0)
                        {
                            _logger.Log($"Batch of {messageCount} message(s) received for processing", 1);

                            var messagesForwarded = 0;

                            foreach (var message in messages)
                            {
                                destinationClient.Send(message.Clone());
                                    
                                messagesForwarded++;
                                totalMessagesForwarded++;

                                message.Complete();
                            }

                            _logger.Log($"Processing complete: {messagesForwarded} message(s) forwarded", 1);
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

            _logger.Log($"[{sourceClient.Path}] - Completed processing queue - {totalMessagesForwarded} message(s) forwarded");
        }

        public void ProcessQueue(QueueClient sourceClient, QueueClient destinationClient)
        {
            _logger.Log($"[{sourceClient.Path}] - Processing queue");
            
            var totalMessagesForwarded = 0;

            var messagesRemaining = true;

            while (messagesRemaining)
            {
                // get messages in source queue
                var messages = sourceClient.ReceiveBatch(_messagesToHandle, _serverWaitTime);

                var messageCount = messages.Count();
                if (messageCount > 0)
                {
                    _logger.Log($"Batch of {messageCount} message(s) received for processing", 1);

                    var messagesForwarded = 0;

                    foreach (var message in messages)
                    {
                        // send messages to destination queue    
                        destinationClient.Send(message.Clone());

                        messagesForwarded++;
                        totalMessagesForwarded++;

                        message.Complete();
                    }

                    _logger.Log($"Processing complete: {messagesForwarded} message(s) forwarded", 1);
                }
                else
                {
                    messagesRemaining = false;
                    _logger.Log($"No {(totalMessagesForwarded > 0 ? "more " : "")}messages to process", 1);
                }
            }

            _logger.Log($"[{sourceClient.Path}] - Completed processing queue - {totalMessagesForwarded} message(s) forwarded");
        }
    }
}

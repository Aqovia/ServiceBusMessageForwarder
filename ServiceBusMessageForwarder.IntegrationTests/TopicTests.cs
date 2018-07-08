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
    public class TopicTests
    {
        private readonly NamespaceManager _sourceNamespaceManager;
        private readonly NamespaceManager _destinationNamespaceManager;
        private readonly ILogger _logger = new MockLogger();
        private readonly string _sourceConnectionString = ConfigurationManager.AppSettings["SourceConnectionString"];
        private readonly string _destinationConnectionString = ConfigurationManager.AppSettings["DestinationConnectionString"];
        private readonly string _ignoreQueues = "";
        private readonly Message _testMessage = new Message { Id = 3011, Content = "Hello, World!" };

        public TopicTests()
        {
            _sourceNamespaceManager = NamespaceManager.CreateFromConnectionString(_sourceConnectionString);
            _destinationNamespaceManager = NamespaceManager.CreateFromConnectionString(_destinationConnectionString);
        }

        [Scenario]
        public void ForwardMessageWhenTopicExists()
        {
            var topicName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var subscriptionName = "subscription1";
            var ignoreTopics = "";
            var ignoreSubscriptions = "";

            var sourceClient = TopicClient.CreateFromConnectionString(_sourceConnectionString, topicName);

            "Given a topic exists on the destination bus with 1 subscription".x(() =>
            {
                _destinationNamespaceManager.CreateTopic(topicName);
                _destinationNamespaceManager.CreateSubscription(topicName, subscriptionName);
            });
            "And the source topic has 1 subscription".x(() =>
            {
                _sourceNamespaceManager.CreateTopic(topicName);
                _sourceNamespaceManager.CreateSubscription(topicName, subscriptionName);
            });
            "And a message is sent to the source topic".x(() =>
            {
                sourceClient.Send(new BrokeredMessage(_testMessage));
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, _ignoreQueues, ignoreTopics, ignoreSubscriptions).Run();
            });
            "Then the message is forwarded to the destination topic's subscription".x(() =>
            {
                var destinationSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_destinationConnectionString, topicName, subscriptionName);
                var messages = destinationSubscriptionClient.PeekBatch(10);

                messages.Count().Should().Be(1);
                messages.First().GetBody<Message>().Id.Should().Be(3011);
            });
            "And the message no longer exists in the source topic's subscription".x(() =>
            {
                var sourceSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topicName, subscriptionName);
                var messages = sourceSubscriptionClient.PeekBatch(10);
                messages.Count().Should().Be(0);
            });

            CleanupTopics(topicName);
        }

        [Scenario]
        public void DoNotForwardMessageWhenTopicDoesNotExist()
        {
            var topicName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var subscriptionName = "subscription1";
            var ignoreTopics = "";
            var ignoreSubscriptions = "";

            var sourceClient = TopicClient.CreateFromConnectionString(_sourceConnectionString, topicName);

            "Given a topic does not exist on the destination bus".x(() =>
            {
                _destinationNamespaceManager.DeleteTopic(topicName);
            });
            "And the source topic has 1 subscription".x(() =>
            {
                _sourceNamespaceManager.CreateTopic(topicName);
                _sourceNamespaceManager.CreateSubscription(topicName, subscriptionName);
            });
            "And a message is sent to the source topic".x(() =>
            {
                sourceClient.Send(new BrokeredMessage(_testMessage));
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, _ignoreQueues, ignoreTopics, ignoreSubscriptions).Run();
            });
            "Then the message is not forwarded to the destination topic".x(() =>
            {
                _destinationNamespaceManager.TopicExists(topicName).Should().BeFalse();
            });
            "And the message still exists in the source topic's subscription".x(() =>
            {
                var sourceSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topicName, subscriptionName);
                var messages = sourceSubscriptionClient.PeekBatch(10);
                messages.Count().Should().Be(1);
                messages.First().GetBody<Message>().Id.Should().Be(3011);
            });

            CleanupTopics(topicName);
        }

        [Scenario]
        public void DoNotForwardMessageWhenTopicIsInIgnoredList()
        {
            var topicName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var subscriptionName = "subscription1";
            var ignoreTopics = "";
            var ignoreSubscriptions = "";

            var sourceClient = TopicClient.CreateFromConnectionString(_sourceConnectionString, topicName);

            "Given a topic exists on the destination bus with 1 subscription".x(() =>
            {
                _destinationNamespaceManager.CreateTopic(topicName);
                _destinationNamespaceManager.CreateSubscription(topicName, subscriptionName);
            });
            "And the source topic has 1 subscription".x(() =>
            {
                _sourceNamespaceManager.CreateTopic(topicName);
                _sourceNamespaceManager.CreateSubscription(topicName, subscriptionName);
            });
            "And the topic is in the ignored topics list".x(() =>
            {
                ignoreTopics = topicName;
            });
            "And a message is sent to the source topic".x(() =>
            {
                sourceClient.Send(new BrokeredMessage(_testMessage));
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, _ignoreQueues, ignoreTopics, ignoreSubscriptions).Run();
            });
            "Then the message is not forwarded to the destination topic".x(() =>
            {
                var destinationSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_destinationConnectionString, topicName, subscriptionName);
                var messages = destinationSubscriptionClient.PeekBatch(10);

                messages.Count().Should().Be(0);
            });
            "And the message still exists in the source topic's subscription".x(() =>
            {
                var sourceSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topicName, subscriptionName);
                var messages = sourceSubscriptionClient.PeekBatch(10);
                messages.Count().Should().Be(1);
                messages.First().GetBody<Message>().Id.Should().Be(3011);
            });

            CleanupTopics(topicName);
        }

        [Scenario]
        public void DoNotForwardMessageWhenSubscriptionIsInIgnoredList()
        {
            var topicName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var subscriptionName = "subscription1";
            var ignoreTopics = "";
            var ignoreSubscriptions = "";

            var sourceClient = TopicClient.CreateFromConnectionString(_sourceConnectionString, topicName);

            "Given a topic exists on the destination bus with 1 subscription".x(() =>
            {
                _destinationNamespaceManager.CreateTopic(topicName);
                _destinationNamespaceManager.CreateSubscription(topicName, subscriptionName);
            });
            "And the source topic has 1 subscription".x(() =>
            {
                _sourceNamespaceManager.CreateTopic(topicName);
                _sourceNamespaceManager.CreateSubscription(topicName, subscriptionName);
            });
            "And the subscription is in the ignored topics list".x(() =>
            {
                ignoreSubscriptions = subscriptionName;
            });
            "And a message is sent to the source topic".x(() =>
            {
                sourceClient.Send(new BrokeredMessage(_testMessage));
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, _ignoreQueues, ignoreTopics, ignoreSubscriptions).Run();
            });
            "Then the message is not forwarded to the destination topic".x(() =>
            {
                var destinationSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_destinationConnectionString, topicName, subscriptionName);
                var messages = destinationSubscriptionClient.PeekBatch(10);

                messages.Count().Should().Be(0);
            });
            "And the message still exists in the source topic's subscription".x(() =>
            {
                var sourceSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topicName, subscriptionName);
                var messages = sourceSubscriptionClient.PeekBatch(10);
                messages.Count().Should().Be(1);
                messages.First().GetBody<Message>().Id.Should().Be(3011);
            });

            CleanupTopics(topicName);
        }

        [Scenario]
        public void ForwardMessageWhenWhenNoSubscriptionsExist()
        {
            var topicName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var subscriptionName = "subscription1";
            var ignoreTopics = "";
            var ignoreSubscriptions = "";

            var sourceClient = TopicClient.CreateFromConnectionString(_sourceConnectionString, topicName);

            "Given a topic exists on the destination bus with no subscriptions".x(() =>
            {
                _destinationNamespaceManager.CreateTopic(topicName);

                var subscriptions = _destinationNamespaceManager.GetSubscriptions(topicName);
                foreach(var subscription in subscriptions)
                    _destinationNamespaceManager.DeleteSubscription(topicName, subscription.Name);
            });
            "And the source topic has 1 subscription".x(() =>
            {
                _sourceNamespaceManager.CreateTopic(topicName);
                _sourceNamespaceManager.CreateSubscription(topicName, subscriptionName);
            });
            "And a message is sent to the source topic".x(() =>
            {
                sourceClient.Send(new BrokeredMessage(_testMessage));
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, _ignoreQueues, ignoreTopics, ignoreSubscriptions).Run();
            });
            "Then the message is still forwarded to the destination topic and no longer exists in the source topic's subscription".x(() =>
            {
                var sourceSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topicName, subscriptionName);
                var messages = sourceSubscriptionClient.PeekBatch(10);
                messages.Count().Should().Be(0);
            });

            CleanupTopics(topicName);
        }

        [Scenario]
        public void DoNotSendDuplicateMessagesFromMultipleSubscriptions()
        {
            var topicName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var subscriptionName = "subscription1";
            var secondSubscriptionName = "subscription2";
            var ignoreTopics = "";
            var ignoreSubscriptions = "";

            var sourceClient = TopicClient.CreateFromConnectionString(_sourceConnectionString, topicName);

            "Given a topic exists on the destination bus with 1 subscription".x(() =>
            {
                _destinationNamespaceManager.CreateTopic(topicName);
                _destinationNamespaceManager.CreateSubscription(topicName, subscriptionName);
            });
            "And the source topic has 2 subscriptions".x(() =>
            {
                _sourceNamespaceManager.CreateTopic(topicName);
                _sourceNamespaceManager.CreateSubscription(topicName, subscriptionName);
                _sourceNamespaceManager.CreateSubscription(topicName, secondSubscriptionName);
            });
            "And a message is sent to the source topic".x(() =>
            {
                var message = new BrokeredMessage(_testMessage) {MessageId = "uniquemessageid"};
                sourceClient.Send(message);
            });
            "And the message is in both of the source topic's subscriptions".x(() =>
            {
                var sourceSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topicName, subscriptionName);
                var messages = sourceSubscriptionClient.PeekBatch(10);
                messages.Count().Should().Be(1);
                messages.First().GetBody<Message>().Id.Should().Be(3011);

                var sourceSecondSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topicName, secondSubscriptionName);
                var secondMessages = sourceSecondSubscriptionClient.PeekBatch(10);
                secondMessages.Count().Should().Be(1);
                secondMessages.First().GetBody<Message>().Id.Should().Be(3011);
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, _ignoreQueues, ignoreTopics, ignoreSubscriptions).Run();
            });
            "Then the message is forwarded to the destination topic's subscription once".x(() =>
            {
                var destinationSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_destinationConnectionString, topicName, subscriptionName);
                var messages = destinationSubscriptionClient.PeekBatch(10);

                messages.Count().Should().Be(1);
                messages.First().GetBody<Message>().Id.Should().Be(3011);
            });
            "And the message no longer exists in the source topic's subscriptions".x(() =>
            {
                var sourceSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topicName, subscriptionName);
                var messages = sourceSubscriptionClient.PeekBatch(10);
                messages.Count().Should().Be(0);

                var sourceSecondSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topicName, secondSubscriptionName);
                var secondMessages = sourceSecondSubscriptionClient.PeekBatch(10);
                secondMessages.Count().Should().Be(0);
            });

            CleanupTopics(topicName);
        }

        [Scenario]
        public void ForwardMessagesOnASubscriptionRequiringSessions()
        {
            var topicName = $"_sbmf-{DateTime.UtcNow:yyyyMMddHHmmss}-{new Random().Next(10000, 99999)}";
            var subscriptionName = "subscription1";
            var sessionId1 = "101";
            var sessionId2 = "201";
            var ignoreTopics = "";
            var ignoreSubscriptions = "";

            var sourceClient = TopicClient.CreateFromConnectionString(_sourceConnectionString, topicName);

            "Given a topic exists on the destination bus with 1 subscription which requires sessions".x(() =>
            {
                _destinationNamespaceManager.CreateTopic(topicName);
                _destinationNamespaceManager.CreateSubscription(new SubscriptionDescription(topicName, subscriptionName) { RequiresSession = true });
            });
            "And the source topic has 1 subscription".x(() =>
            {
                _sourceNamespaceManager.CreateTopic(topicName);
                _sourceNamespaceManager.CreateSubscription(new SubscriptionDescription(topicName, subscriptionName) { RequiresSession = true });
            });
            "And 2 messages are sent to the source topic with different session IDs".x(() =>
            {
                sourceClient.Send(new BrokeredMessage(_testMessage) {SessionId = sessionId1});
                sourceClient.Send(new BrokeredMessage(_testMessage) {SessionId = sessionId2});
            });
            "When the service has run".x(() =>
            {
                new ServiceBusMessageForwarder(_logger, null, _sourceConnectionString, _destinationConnectionString, _ignoreQueues, ignoreTopics, ignoreSubscriptions).Run();
            });
            "Then the messages are forwarded to the destination topic's subscriptions".x(() =>
            {
                var destinationSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_destinationConnectionString, topicName, subscriptionName);

                var messageSession1 = destinationSubscriptionClient.AcceptMessageSession(sessionId1);
                var session1Messages = messageSession1.PeekBatch(10);
                session1Messages.Count().Should().Be(1);
                session1Messages.First().GetBody<Message>().Id.Should().Be(3011);

                var messageSession2 = destinationSubscriptionClient.AcceptMessageSession(sessionId2);
                var session2Messages = messageSession2.PeekBatch(10);
                session2Messages.Count().Should().Be(1);
                session2Messages.First().GetBody<Message>().Id.Should().Be(3011);
            });
            "And the messages no longer exist in the source topic's subscription".x(() =>
            {
                var sourceSubscriptionClient = SubscriptionClient.CreateFromConnectionString(_sourceConnectionString, topicName, subscriptionName);

                var messageSession1 = sourceSubscriptionClient.AcceptMessageSession(sessionId1);
                var session1Messages = messageSession1.PeekBatch(10);
                session1Messages.Count().Should().Be(0);

                var messageSession2 = sourceSubscriptionClient.AcceptMessageSession(sessionId2);
                var session2Messages = messageSession2.PeekBatch(10);
                session2Messages.Count().Should().Be(0);
            });

            CleanupTopics(topicName);
        }

        private void CleanupTopics(string topicName)
        {
            _sourceNamespaceManager.DeleteTopic(topicName);
            _destinationNamespaceManager.DeleteTopic(topicName);
        }
    }
}

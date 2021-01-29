using Azure.Messaging.ServiceBus;
using FailoverRmq.Connection.ConnectionManagers.AzureServiceBus;
using FailoverRmq.Serialization;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Producers.BaseProducers.AzureServiceBus
{
    internal class AzureServiceBusBaseProducer<TMessage> : IProducer<TMessage>
    {
        private const int _senderIndex = -1;
        private readonly string _queueName;
        private readonly int? _messageDelay;

        private ILogger _logger;
        private ISerializer<TMessage> _serializer;
        private readonly ServiceBusClient _client;
        private readonly Dictionary<int, ServiceBusSender> _senders;

        protected AzureServiceBusBaseProducer(IAzureServiceBusConnectionManager azureServiceBusConnectionManager, ILogger logger, ISerializer<TMessage> serializer,
            Func<string, Dictionary<int, string>> getSenders)
        {
            var (QueueName, Delay) = ModelBuilder.GetQueueName<TMessage>();
            _queueName = QueueName.ToLower();
            _messageDelay = Delay;
            _client = azureServiceBusConnectionManager.GetClient();
            _logger = logger;
            _serializer = serializer;
            _senders = getSenders(_queueName).Select(i => new { i.Key, Value = _client.CreateSender(i.Value) }).ToDictionary(i => i.Key, i => i.Value);
        }

        public AzureServiceBusBaseProducer(IAzureServiceBusConnectionManager azureServiceBusConnectionManager, ILogger logger, ISerializer<TMessage> serializer)
            : this(azureServiceBusConnectionManager, logger, serializer, GetSenders)
        { }

        private static Dictionary<int, string> GetSenders(string queueName)
        {
            return new Dictionary<int, string>
            {
                { _senderIndex,  queueName}
            };

        }

        protected virtual int GetSenderIndex(TMessage message, ILogger logger)
        {
            return _senderIndex;
        }

        public async Task Send(TMessage message, CancellationToken cancellationToken)
        {
            var dataBytes = _serializer.Serialize(message);
            var serviceBusMessage = new ServiceBusMessage(dataBytes);
            if (_messageDelay.HasValue)
                serviceBusMessage.ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddMilliseconds(_messageDelay.Value);
            await _senders[GetSenderIndex(message, _logger)].SendMessageAsync(serviceBusMessage, cancellationToken);
        }

        public void Dispose()
        {
            Task.WaitAll(_client.DisposeAsync().AsTask());
        }
    }
}
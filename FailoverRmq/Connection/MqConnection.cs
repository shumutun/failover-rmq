using FailoverRmq.Connection.ConnectionManagers.AzureServiceBus;
using FailoverRmq.Connection.ConnectionManagers.RabbitMQ;
using FailoverRmq.Consumers;
using FailoverRmq.Consumers.BaseConsumers.AzureServiceBus;
using FailoverRmq.Consumers.RabbitMq;
using FailoverRmq.Producers;
using FailoverRmq.Producers.BaseProducers.AzureServiceBus;
using FailoverRmq.Producers.BaseProducers.RabbitMq;
using FailoverRmq.Serialization;
using NLog;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Connection
{
    public class MqConnection
    {
        public IRabbitMqConnectionManager RabbitMqConnectionManager { get; }

        public IAzureServiceBusConnectionManager AzureServiceBusConnectionManager { get; }

        public MqConnection(MqConnectionConfig config)
        {
            if ((config.RabbitMqConfig == null && config.AzureServiceBusConfig == null)
                || (config.RabbitMqConfig != null && config.AzureServiceBusConfig != null))
                throw new ArgumentException("One (only!) config must be not null");

            if (config.RabbitMqConfig != null)
                RabbitMqConnectionManager = new RabbitMqConnectionManager(config.RabbitMqConfig);

            if (config.AzureServiceBusConfig != null)
                AzureServiceBusConnectionManager = new AzureServiceBusConnectionManager(config.AzureServiceBusConfig);
        }

        internal IConsumer<TMessage> CreateConsumer<TMessage>(ILogger logger, ISerializer<TMessage> serializer, Func<TMessage, CancellationToken, Task> procMessage)
        {
            if (RabbitMqConnectionManager != null)
                return new RabbitMqBaseConsumer<TMessage>(RabbitMqConnectionManager, logger, serializer, procMessage);

            if (AzureServiceBusConnectionManager != null)
                return new AzureServiceBusBaseConsumer<TMessage>(AzureServiceBusConnectionManager, logger, serializer, procMessage);

            throw new NotImplementedException();
        }

        internal IProducer<TMessage> CreateProducer<TMessage>(ILogger logger, ISerializer<TMessage> serializer)
        {
            if (RabbitMqConnectionManager != null)
                return new RabbitMqBaseProducer<TMessage>(RabbitMqConnectionManager, logger, serializer);

            if (AzureServiceBusConnectionManager != null)
                return new AzureServiceBusBaseProducer<TMessage>(AzureServiceBusConnectionManager, logger, serializer);

            throw new NotImplementedException();
        }

        internal IProducer<TMessage> CreateDistributedProducer<TMessage>(ILogger logger, ISerializer<TMessage> serializer, DistributedProducerConfig config)
            where TMessage : IDistributableMessage
        {
            if (RabbitMqConnectionManager != null)
                return new RabbitMqBaseDistributedProducer<TMessage>(RabbitMqConnectionManager, logger, serializer, config);

            if (AzureServiceBusConnectionManager != null)
                return new AzureServiceBusBaseDistributedProducer<TMessage>(AzureServiceBusConnectionManager, logger, serializer, config);

            throw new NotImplementedException();
        }
    }
}

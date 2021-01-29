using FailoverRmq.Connection.ConnectionManagers.AzureServiceBus;
using FailoverRmq.Serialization;
using NLog;
using System.Collections.Generic;

namespace FailoverRmq.Producers.BaseProducers.AzureServiceBus
{
    internal class AzureServiceBusBaseDistributedProducer<TMessage> : AzureServiceBusBaseProducer<TMessage>
        where TMessage : IDistributableMessage
    {
        private DistributedProducerConfig _config;

        public AzureServiceBusBaseDistributedProducer(IAzureServiceBusConnectionManager azureServiceBusConnectionManager, ILogger logger, ISerializer<TMessage> serializer, DistributedProducerConfig config)
            : base(azureServiceBusConnectionManager, logger, serializer, queueName =>
            {
                var result = new Dictionary<int, string>();
                for (int i = 0; i < config.ParallelizeQueueTo; i++)
                    result.Add(i, ModelBuilder.GetQueueName(queueName, i));
                return result;
            })
        {
            _config = config;
        }

        protected override int GetSenderIndex(TMessage message, ILogger logger)
        {
            return ModelBuilder.GetDomainIndex<TMessage>(message, _config.ParallelizeQueueTo, logger);
        }
    }
}
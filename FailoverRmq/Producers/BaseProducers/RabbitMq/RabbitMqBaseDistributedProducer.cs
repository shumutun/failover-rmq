using FailoverRmq.Connection.ConnectionManagers.RabbitMQ;
using FailoverRmq.Serialization;
using NLog;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Producers.BaseProducers.RabbitMq
{
    internal class RabbitMqBaseDistributedProducer<TMessage> : RabbitMqBaseProducer<TMessage>, IDistributedProducer<TMessage>
        where TMessage : IDistributableMessage
    {
        private readonly DistributedProducerConfig _config;
        private readonly ILogger _logger;

        public RabbitMqBaseDistributedProducer(IRabbitMqConnectionManager connectionManager, ILogger logger, ISerializer<TMessage> serializer, DistributedProducerConfig config)
            : base(connectionManager, logger, serializer)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        protected override Task Send(TMessage message, string queueName, CancellationToken cancellationToken)
        {
            var index = ModelBuilder.GetDomainIndex(message, _config.ParallelizeQueueTo, _logger);
            return base.Send(message, ModelBuilder.GetQueueName(queueName, index), cancellationToken);
        }
    }
}
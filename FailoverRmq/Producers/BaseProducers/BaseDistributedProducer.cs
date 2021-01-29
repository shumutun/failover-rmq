using FailoverRmq.Connection;
using FailoverRmq.Serialization;
using NLog;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Producers.BaseProducers
{
    public class BaseDistributedProducer<TMessage> : IProducer<TMessage>
        where TMessage : IDistributableMessage
    {
        private readonly IProducer<TMessage> _currentProducer;

        protected BaseDistributedProducer(MqConnection connectionManager, ILogger logger, ISerializer<TMessage> serializer, DistributedProducerConfig config)
        {
            _currentProducer = connectionManager.CreateDistributedProducer(logger, serializer, config);
        }

        public void Dispose()
        {
            _currentProducer.Dispose();
        }

        public Task Send(TMessage message, CancellationToken cancellationToken)
        {
            return _currentProducer.Send(message, cancellationToken);
        }
    }
}

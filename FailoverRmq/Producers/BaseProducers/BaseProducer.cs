using FailoverRmq.Connection;
using FailoverRmq.Serialization;
using NLog;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Producers.BaseProducers
{
    public class BaseProducer<TMessage> : IProducer<TMessage>
    {
        private readonly IProducer<TMessage> _currentProducer;

        protected BaseProducer(MqConnection connectionManager, ILogger logger, ISerializer<TMessage> serializer)
        {
            _currentProducer = connectionManager.CreateProducer(logger, serializer);
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

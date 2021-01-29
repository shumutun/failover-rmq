using FailoverRmq.Connection;
using FailoverRmq.Serialization;
using NLog;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Consumers.BaseConsumers
{
    public abstract class BaseConsumer<TMessage> : IConsumer<TMessage>
    {
        private readonly IConsumer<TMessage> _currentConsumer;

        protected BaseConsumer(MqConnection connection, ILogger logger, ISerializer<TMessage> serializer)
        {
            _currentConsumer = connection.CreateConsumer(logger, serializer, ProcMessage);
        }

        public abstract Task ProcMessage(TMessage message, CancellationToken cancellationToken);

        public bool Run(Action<IAutorepairConsumer> consumerStoped, int? queueParallelizedTo, CancellationToken cancellationToken)
        {
            return _currentConsumer.Run(consumerStoped, queueParallelizedTo, cancellationToken);
        }

        public void Stop()
        {
            _currentConsumer.Stop();
        }
    }
}

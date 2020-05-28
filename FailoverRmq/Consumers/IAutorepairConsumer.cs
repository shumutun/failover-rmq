using System;
using System.Threading;

namespace FailoverRmq.Consumers
{
    public interface IAutorepairConsumer
    {
        bool Run(Action<IAutorepairConsumer> consumerStoped, int? queueParallelizedTo, CancellationToken cancellationToken);

        void Stop();
    }
}

using System;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Producers
{
    public interface IProducer<TMessage> : IDisposable
    {
        Task Send(TMessage message, CancellationToken cancellationToken);
    }
}

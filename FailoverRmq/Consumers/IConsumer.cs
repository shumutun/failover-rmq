using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Consumers
{
    public interface IConsumer<TMessage>: IAutorepairConsumer
    {
        Task ProcMessage(TMessage message, CancellationToken cancellationToken);
    }
}

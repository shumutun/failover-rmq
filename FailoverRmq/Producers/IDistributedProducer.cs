namespace FailoverRmq.Producers
{
    public interface IDistributedProducer<TMessage> : IProducer<TMessage>
        where TMessage : IDistributableMessage
    { }
}

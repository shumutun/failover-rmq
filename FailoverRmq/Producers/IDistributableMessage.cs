namespace FailoverRmq.Producers
{
    public interface IDistributableMessage
    {
        string Url { get; }
    }
}

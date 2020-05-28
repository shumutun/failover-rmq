
namespace FailoverRmq.Consumers
{
    public class AutorepairConsumersConfig
    {
        public int? ConsumersCountLimit { get; set; }

        public int? QueueParallelizedTo { get; set; }
    }
}

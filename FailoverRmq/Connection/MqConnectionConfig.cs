using FailoverRmq.Connection.Configs;

namespace FailoverRmq.Connection
{
    public class MqConnectionConfig
    {
        public AzureServiceBusConfig AzureServiceBusConfig { get; set; }

        public RabbitMqConfig RabbitMqConfig { get; set; }
    }
}

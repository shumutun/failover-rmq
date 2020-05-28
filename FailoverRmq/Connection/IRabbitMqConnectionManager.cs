using RabbitMQ.Client;

namespace FailoverRmq.Connection
{
    public interface IRabbitMqConnectionManager
    {
        IConnection GetConnection();
    }
}

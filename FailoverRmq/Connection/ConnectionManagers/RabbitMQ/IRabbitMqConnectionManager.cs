using RabbitMQ.Client;

namespace FailoverRmq.Connection.ConnectionManagers.RabbitMQ
{
    public interface IRabbitMqConnectionManager
    {
        IConnection GetConnection();
    }
}
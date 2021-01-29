using Azure.Messaging.ServiceBus;

namespace FailoverRmq.Connection.ConnectionManagers.AzureServiceBus
{
    public interface IAzureServiceBusConnectionManager
    {
        ServiceBusClient GetClient();
    }
}

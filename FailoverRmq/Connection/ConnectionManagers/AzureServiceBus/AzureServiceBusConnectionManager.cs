using Azure.Messaging.ServiceBus;
using FailoverRmq.Connection.Configs;

namespace FailoverRmq.Connection.ConnectionManagers.AzureServiceBus
{
    public class AzureServiceBusConnectionManager : IAzureServiceBusConnectionManager
    {
        private readonly AzureServiceBusConfig _config;

        public AzureServiceBusConnectionManager(AzureServiceBusConfig config)
        {
            _config = config;
        }

        public ServiceBusClient GetClient()
        {
            return new ServiceBusClient(_config.ConnectionString);
        }
    }
}

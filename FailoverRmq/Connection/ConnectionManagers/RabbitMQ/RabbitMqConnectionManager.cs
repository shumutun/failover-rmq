using FailoverRmq.Connection.Configs;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FailoverRmq.Connection.ConnectionManagers.RabbitMQ
{
    public class RabbitMqConnectionManager : IRabbitMqConnectionManager
    {
        readonly IConnectionFactory _connectionFactory;
        readonly IList<AmqpTcpEndpoint> _endpoints;

        public RabbitMqConnectionManager(RabbitMqConfig config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));
            if (config.Vhost == null)
                throw new ArgumentNullException(nameof(config.Vhost));
            if (config.User == null)
                throw new ArgumentNullException(nameof(config.User));
            if (config.Password == null)
                throw new ArgumentNullException(nameof(config.Password));
            if (config.Nodes == null)
                throw new ArgumentNullException(nameof(config.Nodes));

            _connectionFactory = new ConnectionFactory
            {
                VirtualHost = config.Vhost,
                UserName = config.User,
                Password = config.Password,
                DispatchConsumersAsync = true,
                AutomaticRecoveryEnabled = true
            };
            _endpoints = config.Nodes.Select(n => new AmqpTcpEndpoint(n)).ToList();
        }

        public IConnection GetConnection()
        {
            return _connectionFactory.CreateConnection(_endpoints);
        }
    }
}
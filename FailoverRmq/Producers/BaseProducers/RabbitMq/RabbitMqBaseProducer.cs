using FailoverRmq.Connection;
using FailoverRmq.Connection.ConnectionManagers.RabbitMQ;
using FailoverRmq.Serialization;
using NLog;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Producers.BaseProducers.RabbitMq
{
    internal class RabbitMqBaseProducer<TMessage> : IProducer<TMessage>
    {
        private readonly int _sendTryCount = 5;
        private readonly int _sendTryDelay = 1000;

        private readonly string _queueName;
        private readonly int? _messageDelay;
        private readonly IRabbitMqConnectionManager _connectionManager;
        private readonly ILogger _logger;
        private readonly ISerializer<TMessage> _serializer;

        private IModel _model;
        private readonly Dictionary<string, string> _declaredQueues = new Dictionary<string, string>();
        private readonly ReaderWriterLockSlim _declaredQueuesLock = new ReaderWriterLockSlim();

        public RabbitMqBaseProducer(IRabbitMqConnectionManager connectionManager, ILogger logger, ISerializer<TMessage> serializer)
        {
            _connectionManager = connectionManager;
            _logger = logger;
            _serializer = serializer;

            var (QueueName, Delay) = ModelBuilder.GetQueueName<TMessage>();
            _queueName = QueueName;
            _messageDelay = Delay;
        }

        private IModel GetModel()
        {
            if (_model != null && _model.IsClosed)
                Dispose();
            if (_model == null)
            {
                var connection = _connectionManager.GetConnection();
                _model = connection.CreateModel();
            }
            return _model;
        }

        private string GetRoutingKey(IModel model, string queueName)
        {
            _declaredQueuesLock.EnterUpgradeableReadLock();
            try
            {
                if (!_declaredQueues.ContainsKey(queueName))
                {
                    _declaredQueuesLock.EnterWriteLock();
                    try
                    {
                        if (!ModelBuilder.QueueDeclare(model, queueName))
                            throw new Exception($"Queue {queueName} is not declared");
                        _declaredQueues.Add(queueName, null);
                        if (_messageDelay.HasValue)
                        {
                            var queueNameDlx = $"{queueName}-dlx";
                            if (!ModelBuilder.QueueDeclare(model, queueNameDlx, new Dictionary<string, object>
                                {
                                    {"x-dead-letter-exchange", string.Empty },
                                    {"x-message-ttl", _messageDelay.Value },
                                    {"x-dead-letter-routing-key", queueName }
                                }))
                                throw new Exception($"Queue {queueNameDlx} is not declared");
                            _declaredQueues[queueName] = queueNameDlx;
                        }
                    }
                    finally
                    {
                        _declaredQueuesLock.ExitWriteLock();
                    }
                }
                return _declaredQueues[queueName] ?? queueName;
            }
            finally
            {
                _declaredQueuesLock.ExitUpgradeableReadLock();
            }
        }

        protected virtual async Task Send(TMessage message, string queueName, CancellationToken cancellationToken)
        {
            var dataBytes = _serializer.Serialize(message);
            var tryCount = 0;
            while (tryCount < _sendTryCount)
            {
                try
                {
                    var model = GetModel();
                    var routingKey = GetRoutingKey(model, queueName);
                    model.BasicPublish(string.Empty, routingKey, true, null, dataBytes);
                    return;
                }
                catch (Exception e)
                {
                    _logger.Error(e, "Message produce error");
                    tryCount++;
                    await Task.Delay(_sendTryDelay, cancellationToken);
                }
            }
            throw new Exception($"Can't produce message to queue {queueName} after {_sendTryCount} iterations");
        }

        public async Task Send(TMessage message, CancellationToken cancellationToken)
        {
            await Send(message, _queueName, cancellationToken);
        }

        public void Dispose()
        {
            _model?.Dispose();
            _model = null;
        }
    }
}

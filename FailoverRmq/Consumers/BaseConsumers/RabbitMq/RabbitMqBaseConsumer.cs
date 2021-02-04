using FailoverRmq.Connection;
using FailoverRmq.Connection.ConnectionManagers.RabbitMQ;
using FailoverRmq.Serialization;
using NLog;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Consumers.RabbitMq
{
    internal class RabbitMqBaseConsumer<TMessage> : IConsumer<TMessage>
    {
        private class BasicConsumer : AsyncDefaultBasicConsumer
        {
            private readonly ILogger _logger;
            private readonly Func<ReadOnlyMemory<byte>, Task> _procmessage;
            private readonly Action _stop;
            private readonly string _queueName;
            private string _consumerTag;

            public BasicConsumer(IModel model, int? index, ILogger logger, Func<ReadOnlyMemory<byte>, Task> procmessage, Action stop)
                : base(model)
            {
                _logger = logger ?? throw new ArgumentNullException(nameof(logger));
                _procmessage = procmessage ?? throw new ArgumentNullException(nameof(procmessage));
                _stop = stop ?? throw new ArgumentNullException(nameof(stop));
                _logger.Info("New RMQConsumer starting...");
                _queueName = ModelBuilder.GetQueueName<TMessage>(index);
                ConfigureChannel();
                _logger.Info("New RMQConsumer started");
            }

            private void ConfigureChannel()
            {
                Model.BasicQos(0, 1, false); //Only one task at the same time
            }

            public bool BindToQueue()
            {
                if (ModelBuilder.QueueDeclare(Model, _queueName))
                {
                    _consumerTag = Model.BasicConsume(_queueName, false, this);
                    _logger.Info($"New RMQConsumer {_consumerTag} binded to queue {_queueName}");
                    return true;
                }
                return false;
            }

            public override async Task HandleBasicConsumeOk(string consumerTag)
            {
                _logger.Info($"RMQConsumer {consumerTag} is conected to message broker");
                await base.HandleBasicConsumeOk(consumerTag);
            }

            public override async Task OnCancel(params string[] consumerTags)
            {
                _logger.Info($"RMQConsumer {consumerTags.Aggregate(new StringBuilder(), (a, i) => a.Append($"{i};")).ToString().TrimEnd(';')} is canceled");
                await base.OnCancel();
                Stop(true);
            }

            public override async Task HandleModelShutdown(object model, ShutdownEventArgs reason)
            {
                _logger.Error($"RMQConsumer {_consumerTag} model is shutdown ({reason})");
                await base.HandleModelShutdown(model, reason);
                Stop(true);
            }

            public override async Task HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, ReadOnlyMemory<byte> body)
            {
                try
                {
                    await _procmessage(body);
                    Model.BasicAck(deliveryTag, false);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, $"RMQConsumer {consumerTag} processed message {deliveryTag} with error");
                    Model.BasicNack(deliveryTag, false, true);
                }
            }

            public void Stop(bool initializedByRMQ)
            {
                if (initializedByRMQ)
                    _stop();
                _logger.Info($"RMQConsumer {_consumerTag} is stoped");
                Model?.Dispose();
            }
        }

        private readonly IRabbitMqConnectionManager _connectionManager;
        private readonly ILogger _logger;
        private readonly ISerializer<TMessage> _serializer;
        private readonly Func<TMessage, CancellationToken, Task> _procMessage;
        private readonly List<BasicConsumer> _basicConsumers = new List<BasicConsumer>();

        private Action<IAutorepairConsumer> _consumerStoped;
        private CancellationToken _cancellationToken;

        public RabbitMqBaseConsumer(IRabbitMqConnectionManager connectionManager, ILogger logger, ISerializer<TMessage> serializer, Func<TMessage, CancellationToken, Task> procMessage)
        {
            _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _serializer = serializer;
            _procMessage = procMessage;
        }

        protected bool Prepare(Action<IAutorepairConsumer> consumerStoped, int? queueParallelizedTo, CancellationToken cancellationToken)
        {
            _consumerStoped = consumerStoped ?? throw new ArgumentNullException(nameof(consumerStoped));
            _cancellationToken = cancellationToken;
            try
            {
                var connection = _connectionManager.GetConnection();
                if (queueParallelizedTo.HasValue)
                    for (int i = 1; i <= queueParallelizedTo.Value; i++)
                        _basicConsumers.Add(new BasicConsumer(connection.CreateModel(), i, _logger, ProcMessage, ConsumerStoped));
                else
                    _basicConsumers.Add(new BasicConsumer(connection.CreateModel(), null, _logger, ProcMessage, ConsumerStoped));
                _logger.Info("RMQConsumer prepeared");
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Can't prepare RMQConsumer");
                return false;
            }
        }

        public virtual bool Run(Action<IAutorepairConsumer> consumerStoped, int? queueParallelizedTo, CancellationToken cancellationToken)
        {
            _logger.Info("Trying to run RMQConsumer...");
            if (!Prepare(consumerStoped, queueParallelizedTo, cancellationToken))
                return false;
            var run = _basicConsumers.All(c => c.BindToQueue());
            if (run)
                _logger.Info("RMQConsumer is running");
            return run;
        }

        public void Stop()
        {
            _logger.Info("Trying to stop RMQConsumer...");
            foreach (var basicConsumer in _basicConsumers)
                basicConsumer.Stop(false);
            _logger.Info("RMQConsumer stoped");
        }

        public Task ProcMessage(TMessage message, CancellationToken cancellationToken)
        {
            return _procMessage(message, cancellationToken);
        }

        private void ConsumerStoped()
        {
            _consumerStoped?.Invoke(this);
        }

        private Task ProcMessage(ReadOnlyMemory<byte> body)
        {
            return ProcMessage(_serializer.Deserialize(body), _cancellationToken);
        }
    }
}

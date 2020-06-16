using FailoverRmq.Connection;
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
    public abstract class RabbitMqConsumerBase<TMessage> : IConsumer<TMessage>
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
                _logger.Info("New consumer starting");

                var dc = (DataContractAttribute)typeof(TMessage).GetCustomAttributes(typeof(DataContractAttribute), false).FirstOrDefault() ?? throw new Exception("Data contract must have a DataContract attribute");
                _queueName = index.HasValue ? $"{dc.QueueName}-domain{index}" : dc.QueueName;

                ConfigureChannel();
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
                    _logger.Info($"New consumer {_consumerTag} started and binded to queue {_queueName}");
                    return true;
                }
                return false;
            }

            public override async Task HandleBasicConsumeOk(string consumerTag)
            {
                _logger.Info($"Consumer {consumerTag} is conected to message broker");
                await base.HandleBasicConsumeOk(consumerTag);
            }

            public override async Task OnCancel(params string[] consumerTags)
            {
                _logger.Info($"Consumer {consumerTags.Aggregate(new StringBuilder(), (a, i) => a.Append($"{i};")).ToString().TrimEnd(';')} is canceled");
                await base.OnCancel();
                Stop(true);
            }

            public override async Task HandleModelShutdown(object model, ShutdownEventArgs reason)
            {
                _logger.Error($"Consumer {_consumerTag} model is shutdown ({reason})");
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
                    _logger.Error(ex, $"Consumer {consumerTag} processed message {deliveryTag} with error");
                    Model.BasicNack(deliveryTag, false, true);
                }
            }

            public void Stop(bool initializedByRMQ)
            {
                if (initializedByRMQ)
                    _stop();
                _logger.Info($"Consumer {_consumerTag} is stoped");
                Model?.Dispose();
            }
        }

        private readonly IRabbitMqConnectionManager _connectionManager;
        private readonly ILogger _logger;
        private readonly ISerializer<TMessage> _serializer;
        private readonly List<BasicConsumer> _basicConsumers = new List<BasicConsumer>();

        private Action<IAutorepairConsumer> _consumerStoped;

        private CancellationToken _cancellationToken;

        protected RabbitMqConsumerBase(IRabbitMqConnectionManager connectionManager, ILogger logger, ISerializer<TMessage> serializer)
        {
            _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _serializer = serializer;
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
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Can't create consumer");
                return false;
            }
        }

        public virtual bool Run(Action<IAutorepairConsumer> consumerStoped, int? queueParallelizedTo, CancellationToken cancellationToken)
        {
            if (!Prepare(consumerStoped, queueParallelizedTo, cancellationToken))
                return false;
            return _basicConsumers.All(c => c.BindToQueue());
        }

        public void Stop()
        {
            foreach (var basicConsumer in _basicConsumers)
                basicConsumer.Stop(false);
        }

        public abstract Task ProcMessage(TMessage message, CancellationToken cancellationToken);

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

using Azure.Messaging.ServiceBus;
using FailoverRmq.Connection.ConnectionManagers.AzureServiceBus;
using FailoverRmq.Serialization;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Consumers.BaseConsumers.AzureServiceBus
{
    internal class AzureServiceBusBaseConsumer<TMessage> : IConsumer<TMessage>
    {
        internal class BasicConsumer
        {
            private readonly ILogger _logger;
            private readonly Func<ReadOnlyMemory<byte>, Task> _procmessage;
            private readonly Action _stop;
            private readonly string _queueName;
            private readonly ServiceBusProcessor _processor;

            public BasicConsumer(ServiceBusClient client, int? index, ILogger logger, Func<ReadOnlyMemory<byte>, Task> procmessage, Action stop)
            {
                _logger = logger ?? throw new ArgumentNullException(nameof(logger));
                _procmessage = procmessage ?? throw new ArgumentNullException(nameof(procmessage));
                _stop = stop ?? throw new ArgumentNullException(nameof(stop));
                _logger.Info("New ServiceBusProcessor starting...");
                _queueName = ModelBuilder.GetQueueName<TMessage>(index).ToLower();
                _processor = client.CreateProcessor(_queueName, new ServiceBusProcessorOptions
                {
                    AutoCompleteMessages = false,
                    PrefetchCount = 1, //Only one task at the same time
                    MaxConcurrentCalls = 1,
                    ReceiveMode = ServiceBusReceiveMode.PeekLock
                });
                _processor.ProcessMessageAsync += _processor_ProcessMessageAsync;
                _processor.ProcessErrorAsync += _processor_ProcessErrorAsync;
                _logger.Info("New ServiceBusProcessor started");
            }

            public async Task Run(CancellationToken cancellationToken)
            {
                await _processor.StartProcessingAsync(cancellationToken);
                _logger.Info($"New ServiceBusProcessor binded to queue {_queueName}");
            }

            public async Task Stop(bool initializedByAzureServiceBus)
            {
                if (initializedByAzureServiceBus)
                    _stop();
                _logger.Info($"ServiceBusProcessor for queue {_queueName} is stoped");
                await _processor.StopProcessingAsync();
                await _processor.DisposeAsync();
            }

            private async Task _processor_ProcessErrorAsync(ProcessErrorEventArgs arg)
            {
                _logger.Error(arg.Exception, $"Error ocurred at ServiceBusProcessor for queue {_queueName}. ErrorSource: {arg.ErrorSource}");
                await Stop(true);
            }

            private async Task _processor_ProcessMessageAsync(ProcessMessageEventArgs arg)
            {
                try
                {
                    var body = arg.Message.Body.ToMemory();
                    await _procmessage(body);
                    await arg.CompleteMessageAsync(arg.Message);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, $"ServiceBusProcessor for queue {_queueName} processed message {arg.Message.MessageId} with error");
                    await arg.AbandonMessageAsync(arg.Message);
                }
            }
        }

        private readonly IAzureServiceBusConnectionManager _connectionManager;
        private readonly ILogger _logger;
        private readonly ISerializer<TMessage> _serializer;
        private readonly Func<TMessage, CancellationToken, Task> _procMessage;
        private readonly List<BasicConsumer> _basicConsumers = new List<BasicConsumer>();

        private Action<IAutorepairConsumer> _consumerStoped;
        private CancellationToken _cancellationToken;

        public AzureServiceBusBaseConsumer(IAzureServiceBusConnectionManager connectionManager, ILogger logger, ISerializer<TMessage> serializer, Func<TMessage, CancellationToken, Task> procMessage)
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
                var client = _connectionManager.GetClient();
                if (queueParallelizedTo.HasValue)
                    for (int i = 1; i <= queueParallelizedTo.Value; i++)
                        _basicConsumers.Add(new BasicConsumer(client, i, _logger, ProcMessage, ConsumerStoped));
                else
                    _basicConsumers.Add(new BasicConsumer(client, null, _logger, ProcMessage, ConsumerStoped));
                _logger.Info("ServiceBusProcessor prepeared");
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Can't prepare ServiceBusProcessor");
                return false;
            }
        }

        public Task ProcMessage(TMessage message, CancellationToken cancellationToken)
        {
            return _procMessage(message, cancellationToken);
        }
        private Task ProcMessage(ReadOnlyMemory<byte> body)
        {
            return ProcMessage(_serializer.Deserialize(body), _cancellationToken);
        }

        private void ConsumerStoped()
        {
            _consumerStoped?.Invoke(this);
        }

        public bool Run(Action<IAutorepairConsumer> consumerStoped, int? queueParallelizedTo, CancellationToken cancellationToken)
        {
            _logger.Info("Trying to run ServiceBusProcessor...");
            if (!Prepare(consumerStoped, queueParallelizedTo, cancellationToken))
                return false;
            var tasks = _basicConsumers.Select(c => c.Run(cancellationToken)).ToArray();
            Task.WaitAll(tasks);
            _logger.Info("ServiceBusProcessor is running");
            return true;
        }

        public void Stop()
        {
            _logger.Info("Trying to stop ServiceBusProcessor...");
            var tasks = _basicConsumers.Select(c => c.Stop(false)).ToArray();
            Task.WaitAll(tasks);
            _logger.Info("ServiceBusProcessor stoped");
        }
    }
}
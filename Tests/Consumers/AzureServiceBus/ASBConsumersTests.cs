using Azure.Messaging.ServiceBus;
using Castle.Windsor;
using FailoverRmq;
using FailoverRmq.Connection.ConnectionManagers.AzureServiceBus;
using FailoverRmq.Consumers;
using FailoverRmq.Consumers.BaseConsumers.AzureServiceBus;
using FailoverRmq.Serialization;
using Moq;
using NLog;
using NUnit.Framework;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Tests.Consumers.AzureServiceBus
{
    [TestFixture]
    public class ASBConsumersTests
    {
        private const string _queueName = "queue";
        private const int _queueVersion = 1;

        [DataContract(_queueName, _queueVersion)]
        public class TestDataContract { }

        internal class TestRabbitMqConsumer : AzureServiceBusBaseConsumer<TestDataContract>
        {
            public TestRabbitMqConsumer(IAzureServiceBusConnectionManager connectionManager, ILogger logger, ISerializer<TestDataContract> serializer,
                Func<TestDataContract, CancellationToken, Task> procMessage)
                : base(connectionManager, logger, serializer, procMessage)
            { }
        }

        [Test]
        public void ConsumerExceptionTest()
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var locker = new object();

            var queueName = ModelBuilder.GetQueueName<TestDataContract>(null);

            var processor = new Mock<ServiceBusProcessor>();
            processor.Setup(x => x.StartProcessingAsync(It.IsAny<CancellationToken>())).Returns(Task.Factory.StartNew(() => { }));
            processor.Setup(x => x.StopProcessingAsync(It.IsAny<CancellationToken>())).Returns(Task.Factory.StartNew(() => { }));

            var client = new Mock<ServiceBusClient>();
            client.Setup(x => x.CreateProcessor(queueName, It.IsAny<ServiceBusProcessorOptions>())).Returns(processor.Object);

            var connectionManager = new Mock<IAzureServiceBusConnectionManager>();
            connectionManager.Setup(x => x.GetClient()).Returns(client.Object);

            var logger = new Mock<ILogger>();
            logger.Setup(x => x.Error(It.IsAny<Exception>(), It.IsAny<string>()));
            logger.Setup(x => x.Info(It.IsAny<string>()));

            var serializer = new Mock<ISerializer<TestDataContract>>();
            serializer.Setup(x => x.Serialize(It.IsAny<TestDataContract>())).Returns(new ReadOnlyMemory<byte>());
            serializer.Setup(x => x.Deserialize(It.IsAny<ReadOnlyMemory<byte>>())).Returns(new TestDataContract());

            Task procMessage(TestDataContract dc, CancellationToken c) => Task.Factory.StartNew(() => { });

            var consumer = new TestRabbitMqConsumer(connectionManager.Object, logger.Object, serializer.Object, procMessage);

            var container = new Mock<IWindsorContainer>();
            container.Setup(x => x.Resolve<IAutorepairConsumer>()).Returns(consumer);
            container.Setup(x => x.Release(It.IsAny<IAutorepairConsumer>()));

            var consumers = new AutorepairConsumers<IAutorepairConsumer>(container.Object.Resolve<IAutorepairConsumer>, container.Object.Release, cancellationTokenSource.Token);
            consumers.Start(new AutorepairConsumersConfig { ConsumersCountLimit = 1 });

            Task.Delay(1500).Wait();

            container.Verify(x => x.Resolve<IAutorepairConsumer>(), Times.Exactly(1));

            var basicConsumers = (IEnumerable)consumer.GetType().BaseType
                .GetField("_basicConsumers", BindingFlags.Instance | BindingFlags.NonPublic)
                .GetValue(consumer);
            var enumerator = basicConsumers.GetEnumerator();
            while (enumerator.MoveNext())
                ((AzureServiceBusBaseConsumer<TestDataContract>.BasicConsumer)enumerator.Current).Stop(true).Wait();

            Task.Delay(1000).Wait();

            container.Verify(x => x.Release(consumer), Times.Exactly(1));
            container.Verify(x => x.Resolve<IAutorepairConsumer>(), Times.Exactly(2));
        }
    }
}

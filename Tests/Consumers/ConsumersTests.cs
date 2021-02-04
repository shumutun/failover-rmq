using Castle.Windsor;
using FailoverRmq;
using FailoverRmq.Consumers;
using FailoverRmq.Consumers.BaseConsumers;
using Moq;
using NLog;
using NUnit.Framework;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Tests
{
    [TestFixture]
    public class ConsumersTests
    {

        [Test]
        public void ConsumersStartStopTest()
        {
            const int consumersCount = 10;
            var cancellationTokenSource = new CancellationTokenSource();

            var consumer = new Mock<IAutorepairConsumer>();
            consumer.Setup(x => x.Run(It.IsAny<Action<IAutorepairConsumer>>(), It.IsAny<int?>(), cancellationTokenSource.Token)).Returns(true);

            var container = new Mock<IWindsorContainer>();
            container.Setup(x => x.Resolve<IAutorepairConsumer>()).Returns(consumer.Object);
            container.Setup(x => x.Release(It.IsAny<IAutorepairConsumer>()));

            var consumers = new AutorepairConsumers<IAutorepairConsumer>(container.Object.Resolve<IAutorepairConsumer>, container.Object.Release, cancellationTokenSource.Token);

            for (int i = 1; i <= 2; i++)
            {
                consumers.Start(new AutorepairConsumersConfig { ConsumersCountLimit = consumersCount });
                Task.Delay(1500).Wait();
                container.Verify(x => x.Resolve<IAutorepairConsumer>(), Times.Exactly(consumersCount * i));
                consumer.Verify(x => x.Run(consumers.ConsumerStoped, null, cancellationTokenSource.Token), Times.Exactly(consumersCount * i));

                consumers.Stop();
                container.Verify(x => x.Release(consumer.Object), Times.Exactly(consumersCount * i));
            }
        }

        [Test]
        public void ConsumerDeadTest()
        {
            var cancellationTokenSource = new CancellationTokenSource();

            var consumer = new Mock<IAutorepairConsumer>();
            consumer.Setup(x => x.Run(It.IsAny<Action<IAutorepairConsumer>>(), It.IsAny<int?>(), cancellationTokenSource.Token)).Returns(true);

            var container = new Mock<IWindsorContainer>();
            container.Setup(x => x.Resolve<IAutorepairConsumer>()).Returns(consumer.Object);
            container.Setup(x => x.Release(It.IsAny<IAutorepairConsumer>()));

            var consumers = new AutorepairConsumers<IAutorepairConsumer>(container.Object.Resolve<IAutorepairConsumer>, container.Object.Release, cancellationTokenSource.Token);
            consumers.Start(new AutorepairConsumersConfig { ConsumersCountLimit = 1 });
            Task.Delay(1500).Wait();
            container.Verify(x => x.Resolve<IAutorepairConsumer>(), Times.Exactly(1));
            consumer.Verify(x => x.Run(consumers.ConsumerStoped, null, cancellationTokenSource.Token), Times.Exactly(1));

            consumers.ConsumerStoped(consumer.Object);
            Task.Delay(1000).Wait();
            container.Verify(x => x.Release(consumer.Object), Times.Exactly(1));

            Task.Delay(1000).Wait();
            container.Verify(x => x.Resolve<IAutorepairConsumer>(), Times.Exactly(2));
            consumer.Verify(x => x.Run(consumers.ConsumerStoped, null, cancellationTokenSource.Token), Times.Exactly(2));

            consumers.Stop();
            container.Verify(x => x.Release(consumer.Object), Times.Exactly(2));
        }



        //[Test]
        //public void DistributedConsumersStartStopTest()
        //{
        //    var cancellationTokenSource = new CancellationTokenSource();
        //    var locker = new object();

        //    var model = new Mock<IModel>();
        //    model.Setup(x => x.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
        //           .Returns<string, bool, bool, bool, IDictionary<string, object>>((queue, durable, exclusive, autoDelete, arguments) => new QueueDeclareOk(queue, 0, 0));
        //    model.Setup(x => x.BasicQos(It.IsAny<uint>(), It.IsAny<ushort>(), It.IsAny<bool>()));
        //    model.Setup(x => x.BasicReject(It.IsAny<ulong>(), It.IsAny<bool>()));

        //    var connection = new Mock<IConnection>();
        //    connection.Setup(x => x.CreateModel()).Returns(model.Object);

        //    var connectionManager = new Mock<IRabbitMqConnectionManager>();
        //    connectionManager.Setup(x => x.GetConnection()).Returns(connection.Object);

        //    var logger = new Mock<ICorlog>();
        //    logger.Setup(x => x.Error(It.IsAny<string>(), It.IsAny<Exception>()));
        //    logger.Setup(x => x.Info(It.IsAny<string>()));

        //    var consumer = new TestRabbitMqConsumer(connectionManager.Object, logger.Object);

        //    var container = new Mock<IWindsorContainer>();
        //    container.Setup(x => x.Resolve<IAutorepairConsumer>()).Returns(consumer);
        //    container.Setup(x => x.Release(It.IsAny<IAutorepairConsumer>()));

        //    var consumers = new AutorepairConsumers<IAutorepairConsumer>(container.Object.Resolve<IAutorepairConsumer>, container.Object.Release, cancellationTokenSource.Token);
        //    consumers.Start(new AutorepairConsumersConfig { ConsumersCountLimit = 1, QueueParallelizedTo = 2 });

        //    Task.Delay(1500).Wait();

        //    container.Verify(x => x.Resolve<IAutorepairConsumer>(), Times.Exactly(1));

        //    var basicConsumersEnumerator = ((IEnumerable)consumer.GetType().BaseType
        //        .GetField("_basicConsumers", BindingFlags.Instance | BindingFlags.NonPublic)
        //        .GetValue(consumer)).GetEnumerator();

        //    var basicConsumers = new List<AsyncDefaultBasicConsumer>();
        //    while (basicConsumersEnumerator.MoveNext())
        //        basicConsumers.Add((AsyncDefaultBasicConsumer)basicConsumersEnumerator.Current);

        //    Assert.AreEqual(2, basicConsumers.Count);

        //    var basicConsumer1QueueName = (string)basicConsumers[0].GetType()
        //        .GetField("_queueName", BindingFlags.Instance | BindingFlags.NonPublic)
        //        .GetValue(basicConsumers[0]);

        //    var basicConsumer2QueueName = (string)basicConsumers[1].GetType()
        //        .GetField("_queueName", BindingFlags.Instance | BindingFlags.NonPublic)
        //        .GetValue(basicConsumers[1]);

        //    Assert.AreEqual($"v1-{_queueName}-domain1", basicConsumer1QueueName);
        //    Assert.AreEqual($"v1-{_queueName}-domain2", basicConsumer2QueueName);

        //    basicConsumers[0].OnCancel().Wait();

        //    Task.Delay(1000).Wait();

        //    container.Verify(x => x.Release(consumer), Times.Exactly(1));
        //    container.Verify(x => x.Resolve<IAutorepairConsumer>(), Times.Exactly(2));
        //}
    }
}
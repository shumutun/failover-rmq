using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace FailoverRmq.Consumers
{
    public class AutorepairConsumers<T>
        where T : IAutorepairConsumer
    {
        private const int _consumersActualizaDeleay = 1000;
        private Timer _regenerateTimer;
        private AutorepairConsumersConfig _config;

        private readonly List<IAutorepairConsumer> _consumers = new List<IAutorepairConsumer>();
        private readonly ConcurrentBag<IAutorepairConsumer> _stopedConsumers = new ConcurrentBag<IAutorepairConsumer>();

        private readonly Func<T> _resolveConsumer;
        private readonly Action<T> _releaseConsumer;
        private readonly CancellationToken _cancellationToken;

        public AutorepairConsumers(Func<T> resolveConsumer, Action<T> releaseConsumer, CancellationToken cancellationToken)
        {
            _resolveConsumer = resolveConsumer ?? throw new ArgumentNullException(nameof(resolveConsumer));
            _releaseConsumer = releaseConsumer ?? throw new ArgumentNullException(nameof(releaseConsumer));
            _cancellationToken = cancellationToken;
            _cancellationToken.Register(Stop);
        }

        public void Start(AutorepairConsumersConfig config)
        {
            _config = config;
            Stop();
            _regenerateTimer = new Timer(Regenerate, false, 0, Timeout.Infinite);
        }

        private void Regenerate(object isStop)
        {
            while (_stopedConsumers.TryTake(out IAutorepairConsumer stopedConsumer))
            {
                if (_consumers.Contains(stopedConsumer))
                    _consumers.Remove(stopedConsumer);
                stopedConsumer.Stop();
                _releaseConsumer((T)stopedConsumer);
            }
            if (!(bool)isStop)
            {
                for (int i = _consumers.Count; i < (_config?.ConsumersCountLimit ?? Environment.ProcessorCount); i++)
                {
                    var consumer = _resolveConsumer();
                    if (consumer.Run(ConsumerStoped, _config?.QueueParallelizedTo, _cancellationToken))
                        _consumers.Add(consumer);
                    else
                        _stopedConsumers.Add(consumer);
                }
                _regenerateTimer.Change(_consumersActualizaDeleay, Timeout.Infinite);
            }
        }

        public void ConsumerStoped(IAutorepairConsumer consumer)
        {
            if (consumer != null)
                _stopedConsumers.Add(consumer);
        }

        public void Stop()
        {
            if (_regenerateTimer != null)
                _regenerateTimer.Dispose();
            _regenerateTimer = null;
            foreach (var consumer in _consumers)
                ConsumerStoped(consumer);
            Regenerate(true);
        }
    }
}

using FailoverRmq.Connection;
using FailoverRmq.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FailoverRmq.Producers.RabbitMq
{
    public abstract class RabbitMqDistributedProducerBase<TMessage> : RabbitMqProducerBase<TMessage>, IDistributedProducer<TMessage>
        where TMessage : IDistributableMessage
    {
        private static readonly Dictionary<string, int> _domainsIndexes = new Dictionary<string, int>();
        private static readonly ReaderWriterLockSlim _domainsIndexesLock = new ReaderWriterLockSlim();

        private readonly DistributedProducerConfig _config;
        private readonly ILogger _logger;

        protected RabbitMqDistributedProducerBase(IRabbitMqConnectionManager connectionManager, ILogger logger, ISerializer<TMessage> serializer, DistributedProducerConfig config)
            : base(connectionManager, logger, serializer)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        private static int GetDomainIndex(TMessage message, int parallelizeQueueTo, ILogger logger)
        {
            var messageDomain = new Uri(message.Url, UriKind.Absolute).Host;
            _domainsIndexesLock.EnterUpgradeableReadLock();
            try
            {
                if (!_domainsIndexes.ContainsKey(messageDomain))
                {
                    _domainsIndexesLock.EnterWriteLock();
                    try
                    {
                        var mod = GetStringHash(messageDomain) % (uint)parallelizeQueueTo; //Mod of a digit can't be grather then the digit
                        var index = (int)mod + 1; //Mod can be max parallelizeQueueTo - 1. Add 1 to move it back to the range!
                        _domainsIndexes.Add(messageDomain, index);
                        logger.LogInformation($"For domain {messageDomain} assigned index {index}");
                    }
                    finally
                    {
                        _domainsIndexesLock.ExitWriteLock();
                    }
                }
                return _domainsIndexes[messageDomain];
            }
            finally
            {
                _domainsIndexesLock.ExitUpgradeableReadLock();
            }
        }

        private static uint GetStringHash(string str)
        {
            uint hash = 23;
            unchecked
            {
                foreach (char c in str)
                    hash = hash * 31 + c;
            }
            return hash;
        }

        protected override Task Send(TMessage message, string queueName, CancellationToken cancellationToken)
        {
            var index = GetDomainIndex(message, _config.ParallelizeQueueTo, _logger);
            return base.Send(message, $"{queueName}-domain{index}", cancellationToken);
        }
    }
}

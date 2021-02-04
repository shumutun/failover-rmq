using FailoverRmq.Producers;
using NLog;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace FailoverRmq
{
    public static class ModelBuilder
    {
        public static bool QueueDeclare(IModel model, string queueName, Dictionary<string, object> args = null)
        {
            var queueDeclareResult = model.QueueDeclare(queueName, true, false, false, args);
            return queueDeclareResult.QueueName == queueName;
        }

        public static string GetQueueName<TMessage>(int? index)
        {
            var dc = (DataContractAttribute)typeof(TMessage).GetCustomAttributes(typeof(DataContractAttribute), false).FirstOrDefault() ?? throw new Exception("Data contract must have a DataContract attribute");
            return index.HasValue ? $"{dc.QueueName}-domain{index}" : dc.QueueName;
        }

        public static (string QueueName, int? Delay) GetQueueName<TMessage>()
        {
            var dc = (DataContractAttribute)typeof(TMessage).GetCustomAttributes(typeof(DataContractAttribute), false).FirstOrDefault() ?? throw new Exception("Data contract must have a DataContract attribute");
            return (dc.QueueName, dc.Delay);
        }

        public static string GetQueueName(string queueName, int index)
        {
            return $"{queueName}-domain{index}";
        }


        private static readonly Dictionary<string, int> _domainsIndexes = new Dictionary<string, int>();
        private static readonly ReaderWriterLockSlim _domainsIndexesLock = new ReaderWriterLockSlim();
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
        public static int GetDomainIndex<TMessage>(TMessage message, int parallelizeQueueTo, ILogger logger)
            where TMessage : IDistributableMessage
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
                        logger.Info($"For domain {messageDomain} assigned index {index}");
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
    }
}

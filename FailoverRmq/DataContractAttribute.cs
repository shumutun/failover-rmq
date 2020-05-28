using System;

namespace FailoverRmq
{
    [AttributeUsage(AttributeTargets.Class)]
    public class DataContractAttribute : Attribute
    {
        public string Queue { get; }
        public int Version { get; }
        public int? Delay { get; }
        public string QueueName => $"v{Version}-{Queue}";

        /// <summary>
        /// Queue defination
        /// </summary>
        /// <param name="queue">Queue name</param>
        /// <param name="version">Data contract version</param>
        public DataContractAttribute(string queue, int version)
        {
            Queue = queue;
            Version = version;
        }

        /// <summary>
        /// Queue defination
        /// </summary>
        /// <param name="queue">Queue name</param>
        /// <param name="version">Data contract version</param>
        /// <param name="delayMs">Message delay</param>
        public DataContractAttribute(string queue, int version, int delayMs)
            :this(queue, version)
        {
            Delay = delayMs;
        }
    }
}

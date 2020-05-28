using RabbitMQ.Client;
using System.Collections.Generic;

namespace FailoverRmq
{
    public static class ModelBuilder
    {
        public static bool QueueDeclare(IModel model, string queueName, Dictionary<string, object> args = null)
        {
            return model.QueueDeclare(queueName, true, false, false, args).QueueName == queueName;
        }
    }
}

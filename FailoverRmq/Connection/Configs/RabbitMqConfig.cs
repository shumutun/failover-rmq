namespace FailoverRmq.Connection.Configs
{
    public class RabbitMqConfig
    {
        public string[] Nodes { get; set; }

        public string Vhost { get; set; }

        public string User { get; set; }

        public string Password { get; set; }
    }
}

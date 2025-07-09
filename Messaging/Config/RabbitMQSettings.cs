namespace TM.Messaging.Config
{
    public class RabbitMQSettings
    {
        public required string Host { get; set; }
        public int Port { get; set; }
        public required string Username { get; set; }
        public required string Password { get; set; }

        public List<MessageSettings> Messages { get; set; } = new();
    }

}

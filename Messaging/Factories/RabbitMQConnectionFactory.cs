using TM.Messaging.Config;
using RabbitMQ.Client;
using Microsoft.Extensions.Options;
using RabbitMQ.Client.Exceptions;

namespace TM.Messaging.Factories
{
    public class RabbitMQConnectionFactory(IOptions<RabbitMQSettings> options)
    {
        private readonly RabbitMQSettings _options = options.Value;

        public IConnection CreateConnection()
        {
            var factory = new ConnectionFactory
            {
                HostName = _options.Host,
                Port = _options.Port,
                UserName = _options.Username ?? "guest",
                Password = _options.Password ?? "guest"
            };

            for (int i = 0; i < 5; i++)
            {
                try
                {
                    return factory.CreateConnection();
                }
                catch (BrokerUnreachableException ex)
                {
                    Console.WriteLine($"Tentativa {i + 1} falhou: {ex.Message}");
                    Thread.Sleep(3000);
                }
            }

            throw new Exception("Não foi possível conectar ao RabbitMQ após múltiplas tentativas.");
        }
    }
}

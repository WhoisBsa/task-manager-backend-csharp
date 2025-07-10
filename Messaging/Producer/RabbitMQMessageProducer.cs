using TM.Messaging.Factories;
using TM.Messaging.Interfaces;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Text;

namespace TM.Messaging.Producer
{
    public class RabbitMQMessageProducer(RabbitMQConnectionFactory connectionFactory) : IMessageProducer
    {
        private readonly RabbitMQConnectionFactory _connectionFactory = connectionFactory;

        public void Publish<T>(string exchange, string routingKey, string queueName, T message)
        {
            using var connetion = _connectionFactory.GetConnection() ?? _connectionFactory.CreateConnection();
            using var channel = connetion.CreateModel();

            channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: true);
            channel.QueueBind(queue: queueName, exchange: exchange, routingKey: routingKey);

            var properties = channel.CreateBasicProperties();
            properties.Persistent = true;

            var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));
            channel.BasicPublish(exchange, routingKey, basicProperties: properties, body);
        }
    }
}

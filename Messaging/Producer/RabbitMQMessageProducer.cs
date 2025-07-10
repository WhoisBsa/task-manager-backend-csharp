using TM.Messaging.Factories;
using TM.Messaging.Interfaces;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Text;

namespace TM.Messaging.Producer
{
    public class RabbitMQMessageProducer : IMessageProducer
    {
        private readonly RabbitMQConnectionFactory _connectionFactory;
        private readonly IConnection _connection;
        private readonly IModel _channel;

        public RabbitMQMessageProducer(RabbitMQConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
            _connection = _connectionFactory.GetConnection() ?? _connectionFactory.CreateConnection();
            _channel = _connection.CreateModel();
        }

        public void Publish<T>(string exchange, string routingKey, string queueName, T message)
        {
            _channel.ExchangeDeclare(exchange, ExchangeType.Direct, durable: true);
            _channel.QueueBind(queue: queueName, exchange: exchange, routingKey: routingKey);

            var properties = _channel.CreateBasicProperties();
            properties.Persistent = true;

            var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));
            _channel.BasicPublish(exchange, routingKey, basicProperties: properties, body);
        }
    }
}

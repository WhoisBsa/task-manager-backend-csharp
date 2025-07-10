using TM.Messaging.Factories;
using TM.Messaging.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using Microsoft.Extensions.Logging;

namespace TM.Messaging.Consumer
{
    public class RabbitMqDlqConsumer : IMessageConsumer, IDisposable
    {
        private readonly RabbitMQConnectionFactory _connectionFactory;
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly ILogger<RabbitMqDlqConsumer> _logger;
        private readonly List<string> _consumerTags = new();

        public RabbitMqDlqConsumer(RabbitMQConnectionFactory connectionFactory, ILogger<RabbitMqDlqConsumer> logger)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;
            _connection = _connectionFactory.GetConnection() ?? _connectionFactory.CreateConnection();
            _channel = _connection.CreateModel();
        }

        public void ConsumeAsync(Func<string, Task> onMessageReceived, string originalQueueName)
        {
            var dlqName = $"dlq-{originalQueueName}";

            // Garante que a fila DLQ existe
            _channel.QueueDeclare(queue: dlqName, durable: true, exclusive: false, autoDelete: false);

            var consumer = new EventingBasicConsumer(_channel);

            consumer.Received += async (model, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());

                try
                {
                    _logger.LogWarning("Mensagem recebida da DLQ {DlqName}: {Message}", dlqName, message);
                    await onMessageReceived(message);

                    // Confirma que a mensagem foi processada da DLQ
                    _channel.BasicAck(ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro ao processar mensagem da DLQ {DlqName}", dlqName);
                    // Se quiser, pode reencaminhar ou descartar aqui:
                    _channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
                }
            };

            var consumerTag = _channel.BasicConsume(dlqName, autoAck: false, consumer);
            _consumerTags.Add(consumerTag);
        }

        public void Dispose()
        {
            foreach (var tag in _consumerTags)
            {
                _channel.BasicCancel(tag);
            }

            _channel?.Close();
            _connection?.Close();
        }
    }
}
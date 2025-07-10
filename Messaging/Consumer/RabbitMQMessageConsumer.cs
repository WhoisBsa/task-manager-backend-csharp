using TM.Messaging.Factories;
using TM.Messaging.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using TM.Messaging.Config;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;

namespace TM.Messaging.Consumer
{
    public class RabbitMqMessageConsumer : IMessageConsumer, IDisposable
    {
        private readonly RabbitMQConnectionFactory _connectionFactory;
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly List<string> _consumerTags = [];
        private readonly ILogger _logger;
        private readonly List<MessageSettings> _messages = [];

        public RabbitMqMessageConsumer(
            RabbitMQConnectionFactory connectionFactory,
            IOptions<RabbitMQSettings> rabbitMqSettings,
            ILogger<RabbitMqMessageConsumer> logger)
        {
            _logger = logger;
            _connectionFactory = connectionFactory;
            _connection = _connectionFactory.GetConnection() ?? _connectionFactory.CreateConnection();
            _channel = _connection.CreateModel();
            _messages = rabbitMqSettings.Value.Messages;

            CreateQueues();
        }

        private void CreateQueues()
        {
            // Cria a DLX compartilhada
            _channel.ExchangeDeclare("dead-letter-exchange", ExchangeType.Direct, durable: true);

            foreach (var message in _messages)
            {
                _channel.ExchangeDeclare(exchange: message.Exchange, ExchangeType.Direct, durable: true);

                foreach (var queue in message.Queues)
                {
                    var currentQueue = queue.QueueName;
                    // Declara fila principal com suporte a DLX
                    _channel.QueueDeclare(queue: currentQueue,
                                            durable: true,
                                            exclusive: false,
                                            autoDelete: false,
                                            arguments: new Dictionary<string, object>
                                            {
                                                { "x-dead-letter-exchange", "dead-letter-exchange" },
                                                { "x-dead-letter-routing-key", $"dlq-{currentQueue}" }
                                            });

                    // Declara a fila DLQ correspondente
                    _channel.QueueDeclare(queue: $"dlq-{currentQueue}",
                                            durable: true,
                                            exclusive: false,
                                            autoDelete: false,
                                            arguments: null);

                    // Bind da DLQ à DLX
                    _channel.QueueBind(queue: $"dlq-{currentQueue}",
                                        exchange: "dead-letter-exchange",
                                        routingKey: $"dlq-{currentQueue}");

                    // Bind da fila principal ao exchange principal
                    foreach (var routingKey in queue.RoutingKey)
                    {
                        _channel.QueueBind(queue: currentQueue, exchange: message.Exchange, routingKey: routingKey);
                    }
                }
            }
        }

        public void ConsumeAsync(Func<string, Task> onMessageReceived, string queueName)
        {
            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += async (model, ea) =>
            {
                _logger.LogInformation("[RabbitMQ] Mensagem recebida na fila {QueueName}.", queueName);

                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                _logger.LogInformation("[RabbitMQ] Conteúdo: {Message}", message);

                try
                {
                    await onMessageReceived(message);

                    // Acknowledge da mensagem (processada com sucesso)
                    _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    _logger.LogError("Erro ao processar mensagem da fila {QueueName}. Enviando para DLQ: {Message}", queueName, ex.Message);

                    // Envia para DLQ rejeitando sem requeue
                    _channel.BasicReject(deliveryTag: ea.DeliveryTag, requeue: false);
                }
            };

            var consumerTag = _channel.BasicConsume(queueName, autoAck: false, consumer);
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
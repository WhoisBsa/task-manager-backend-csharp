using TM.Messaging.Factories;
using TM.Messaging.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using TM.Messaging.Config;
using Newtonsoft.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Options;

namespace TM.Messaging.Consumer
{
    public class RabbitMqMessageConsumer : IMessageConsumer, IDisposable
    {
        private readonly RabbitMQConnectionFactory _connectionFactory;
        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly List<string> _consumerTags = [];

        public RabbitMqMessageConsumer(RabbitMQConnectionFactory connectionFactory, IOptions<List<MessageSettings>> messages)
        {
            _connectionFactory = connectionFactory;
            _connection = _connectionFactory.CreateConnection();
            _channel = _connection.CreateModel();

             // Cria a DLX compartilhada
            _channel.ExchangeDeclare("dead-letter-exchange", ExchangeType.Direct, durable: true);

            foreach (var message in messages.Value)
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
            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.Received += async (model, ea) =>
            {
                string message = Encoding.UTF8.GetString(ea.Body.ToArray());

                try
                {
                    await onMessageReceived(message);

                    // Acknowledge da mensagem (processada com sucesso)
                    _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro ao processar mensagem da fila {queueName}. Enviando para DLQ: {ex.Message}");

                    // Envia para DLQ rejeitando sem requeue
                    _channel.BasicReject(deliveryTag: ea.DeliveryTag, requeue: false);
                }
            };

            var consumerTag = _channel.BasicConsume(queueName, autoAck: true, consumer);
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
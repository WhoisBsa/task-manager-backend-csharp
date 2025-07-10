
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TM.Messaging.Config;
using TM.Messaging.Consumer;
using TM.Messaging.Handlers;

namespace TM.Services
{
    public class RabbitMQConsumerService(
        RabbitMqMessageConsumer messageConsumer,
        RabbitMqDlqConsumer dlqConsumer,
        MessageDispatcher dispatcher,
        IOptions<RabbitMQSettings> options,
        ILogger<RabbitMQConsumerService> logger) : BackgroundService
    {
        private readonly RabbitMqMessageConsumer _messageConsumer = messageConsumer;
        private readonly RabbitMqDlqConsumer _dlqConsumer = dlqConsumer;
        private readonly List<MessageSettings> _messages = options.Value.Messages;
        private readonly ILogger _logger = logger;
        private readonly MessageDispatcher _dispatcher = dispatcher;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Serviço RabbitMQ iniciado.");

            foreach (var message in _messages)
            {
                _logger.LogInformation("Configurando consumer - Exchange: {Exchange}, MaxCall: {MaxCall}, BaseDelayMs: {BaseDelayMs}", message.Exchange, message.MaxCall, message.BaseDelayMs);

                foreach (var queue in message.Queues)
                {
                    var currentQueue = queue;
                    
                    // Cria dead letter
                    _dlqConsumer.ConsumeAsync(async (msg) =>
                    {
                        _logger.LogWarning("Mensagem da DLQ recebida da fila {QueueName}: {Message}", currentQueue.QueueName, msg);
                        // persistência, alertas, reprocessamento, etc.
                        await Task.CompletedTask;
                    }, currentQueue.QueueName);

                    // Registra o consumidor para cada fila
                    _messageConsumer.ConsumeAsync(async (msg) =>
                    {
                        try
                        {
                            await ProcessMessageWithRetry(msg, currentQueue.QueueName, message, stoppingToken);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Falha ao processar mensagem da fila {QueueName} após todos os retries. Enviando para Dead Letter.", currentQueue.QueueName);

                            // Aqui você pode enviar para uma fila DLX (Dead Letter Exchange) ou salvar no banco, etc.
                            await HandleDeadLetter(msg, currentQueue.QueueName);
                        }
                    }, currentQueue.QueueName);
                }
            }

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        private async Task ProcessMessageWithRetry(string message, string queueName, MessageSettings messageSettings, CancellationToken token)
        {
            int maxRetries = messageSettings.MaxCall;
            int baseDelayMs = messageSettings.BaseDelayMs;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    _logger.LogInformation("Tentativa {Attempt} de processar mensagem da fila {QueueName}", attempt, queueName);

                    await _dispatcher.DispatchAsync(message);

                    _logger.LogInformation("Mensagem processada com sucesso da fila {QueueName}", queueName);
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro ao processar mensagem da fila {QueueName} na tentativa {Attempt}", queueName, attempt);

                    if (attempt < maxRetries)
                    {
                        var delay = baseDelayMs * (int)Math.Pow(2, attempt - 1);
                        await Task.Delay(delay, token);
                    }
                }
            }

            throw new Exception($"Excedido número máximo de tentativas para a fila {queueName}");
        }
        
        private Task HandleDeadLetter(string message, string queueName)
        {
            _logger.LogWarning("Mensagem da fila {QueueName} foi movida para Dead Letter: {Message}", queueName, message);

            // Aqui só registra/loga; o consumidor DLQ está ativo em paralelo para processar essas mensagens.
            // Se quiser, pode salvar a mensagem em DB ou disparar alerta.

            return Task.CompletedTask;
        }
    }
}
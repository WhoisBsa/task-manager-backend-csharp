using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TM.Messaging.Config;
using TM.Messaging.Consumer;
using TM.Messaging.Producer;
using TM.Messaging.Factories;
using TM.Messaging.Handlers;
using TM.Messaging.Interfaces;
using TM.Services;

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.Configure<RabbitMQSettings>(context.Configuration.GetSection("RabbitMQ"));

        services.AddSingleton<RabbitMQConnectionFactory>();
        services.AddSingleton<RabbitMQMessageProducer>();
        services.AddSingleton<RabbitMqMessageConsumer>();
        services.AddSingleton<RabbitMqDlqConsumer>();

        // Adcionar todos os Handlers das filas
        services.AddSingleton<IMessageHandler, EmailHandler>();
        services.AddSingleton<MessageDispatcher>();

        services.AddHostedService<RabbitMQConsumerService>();
    });

var host = builder.Build();
await host.RunAsync();
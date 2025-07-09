namespace TM.Messaging.Interfaces
{
    public interface IMessageProducer
    {
        void Publish<T>(string exchange, string routingKey, string queueName, T message);
    }
}

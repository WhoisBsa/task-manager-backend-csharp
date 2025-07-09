namespace TM.Messaging.Interfaces
{
    public interface IMessageConsumer
    {
        void ConsumeAsync(Func<string, Task> onMessageReceived, string queueName);
        void Dispose();
    }
}

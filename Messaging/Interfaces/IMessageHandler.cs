namespace TM.Messaging.Interfaces
{
    public interface IMessageHandler
    {
        string Type { get; }
        Task HandleAsync(string content);
    }
}
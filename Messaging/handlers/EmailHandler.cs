using System;
using System.Threading.Tasks;
using TM.Messaging.Interfaces;
using TM.Messaging.Utilities;

namespace TM.Messaging.Handlers
{
    public class EmailHandler : IMessageHandler
    {
        public string Type => Utils.QueueNames.SendTodoEmail;

        public Task HandleAsync(string content)
        {
            Console.WriteLine($"[EMAIL] Mensagem recebida: {content}");
            return Task.CompletedTask;
        }
    }
}
using System;
using System.Threading.Tasks;
using TM.Messaging.Interfaces;
using TM.Messaging.Utilities;

namespace TM.Messaging.Handlers
{
    public class EmailHandler : IMessageHandler
    {
        public string Type => Utils.QueueNames.SendTodoEmail;

        public async Task HandleAsync(string content)
        {
            await Task.Delay(4000);
            Console.WriteLine($"[EMAIL] Mensagem recebida: {content}");
        }
    }
}
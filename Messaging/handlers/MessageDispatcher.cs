using TM.Models;
using Newtonsoft.Json;
using TM.Messaging.Interfaces;

namespace TM.Messaging.Handlers
{
    public class MessageDispatcher
  {
      private readonly Dictionary<string, IMessageHandler> _handlers;

      public MessageDispatcher(IEnumerable<IMessageHandler> handlers)
      {
          _handlers = handlers.ToDictionary(h => h.Type, StringComparer.OrdinalIgnoreCase);
      }

      public async Task DispatchAsync(string rawMessage)
      {
          var dto = JsonConvert.DeserializeObject<MessageDto>(rawMessage);

          if (dto == null || string.IsNullOrWhiteSpace(dto.Type))
          {
              Console.WriteLine("Mensagem inválida.");
              return;
          }

          if (_handlers.TryGetValue(dto.Type, out var handler))
          {
              await handler.HandleAsync(dto.Content);
          }
          else
          {
              Console.WriteLine($"Nenhum handler encontrado para o tipo: {dto.Type}");
          }
      }
  }
}

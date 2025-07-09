using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TM.Messaging.Config
{
    public class MessageSettings
    {
        public int MaxCall {  get; set; }
        public int BaseDelayMs {  get; set; }
        public List<QueueConfig> Queues { get; set; } = new();
        public string Exchange { get; set; } = string.Empty;
        public string ExchangeType { get; set; } = string.Empty;

        public string ToString() {
            return $"{MaxCall}, {BaseDelayMs}";
        }
    }
}
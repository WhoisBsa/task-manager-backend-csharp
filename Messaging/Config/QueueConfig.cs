using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TM.Messaging.Config
{
    public class QueueConfig
    {
        public string QueueName{ get; set; } = string.Empty;
        public List<string> RoutingKey { get; set; } = new();
    }
}

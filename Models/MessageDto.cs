using Newtonsoft.Json;

namespace TM.Models
{
    public class MessageDto
    {
        [JsonProperty("type")]
        public required string Type { get; set; }
        [JsonProperty("content")]
        public required string Content { get; set; }
    }
}
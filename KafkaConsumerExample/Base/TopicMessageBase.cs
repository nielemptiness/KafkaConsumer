using System.Collections.Generic;
using KafkaConsumerExample.Interfaces;

namespace KafkaConsumerExample.Base
{
    public class TopicMessageBase : ITopicMessage
    {
        
        public TopicMessageBase(string body, IDictionary<string, string> headers = null)
        {
            Version = 1;
            Body = body;
            Headers = headers == null ? new Dictionary<string, string>() : (IReadOnlyDictionary<string, string>) new Dictionary<string, string>(headers);
        }
        public int Version { get; }
        public IReadOnlyDictionary<string, string> Headers { get; }
        public string Body { get; }
        
        public override string ToString() => Body;
    }
}
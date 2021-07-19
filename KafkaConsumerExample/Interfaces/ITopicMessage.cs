using System.Collections.Generic;

namespace KafkaConsumerExample.Interfaces
{
    public interface ITopicMessage
    {
        public int Version { get; }

        public IReadOnlyDictionary<string, string> Headers { get; }

        public string Body { get; }
    }
}
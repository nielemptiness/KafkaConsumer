using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace KafkaConsumerExample.Base
{
    public static class TopicResponseBase<T>
    {
        public static List<T> GetTopicMessages(IEnumerable<TopicMessageBase> messages)
        {
            return messages.Select(message => JsonConvert.DeserializeObject<T>(message.Body)).ToList();
        }
    }
}
using KafkaConsumerExample.Consumer;

namespace KafkaConsumerExample.Interfaces
{
    public interface IMessageConsumerFactory
    {
        MessageConsumer CreateMessageConsumer(string topicPrefix, params string[] topicsToListen);
    }
}
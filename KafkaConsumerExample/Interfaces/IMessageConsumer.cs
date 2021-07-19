using System;
using Confluent.Kafka;

namespace KafkaConsumerExample.Interfaces
{
    public interface IMessageConsumer
    {
        public ConsumerBuilder<Ignore, string> CreateConsumerBuilder();

        public IConsumer<Ignore, string> CreateConsumer();
        
        public IConsumer<Ignore, string> CreateConsumerAndStartFrom(DateTime timeStamp);
    }
}
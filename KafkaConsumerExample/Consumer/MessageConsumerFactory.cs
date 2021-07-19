using System;
using KafkaConsumerExample.Base;
using KafkaConsumerExample.Interfaces;

namespace KafkaConsumerExample.Consumer
{
    public class MessageConsumerFactory : IMessageConsumerFactory
    {
        private readonly KafkaSettings _settings;
        
        public MessageConsumerFactory(KafkaSettings settings)
        {
            _settings = settings ?? throw new ArgumentNullException(nameof (settings));
        }
        
        public MessageConsumer CreateMessageConsumer(string topicPrefix, params string[] topicsToListen)
        {
            if (topicsToListen == null)
                throw new ArgumentNullException(nameof (topicsToListen));
            if (topicsToListen.Length == 0)
                throw new ArgumentException("Topics must contains one or more names.");
            
            return new MessageConsumer(_settings, topicPrefix, topicsToListen);
        }

    }
}
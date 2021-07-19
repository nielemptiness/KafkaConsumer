using System;
using System.Collections.Generic;
using System.Text;
using KafkaConsumerExample.Base;
using Serilog;

namespace KafkaConsumerExample.Consumer
{
    public class MessageReader
    {
        /// <summary>
        /// Implemented as a one-time usage. Don't use this for consumer service!
        /// </summary>
        /// <param name="settings">connection data</param>
        /// <param name="topicPrefix">Add this if you want to create custom topic</param>
        /// <param name="topics">topics to connect </param>
        /// <param name="countOfMessages">how many messages to consume </param>
        /// <param name="addHours">should be negative value do decrease from dateTime.Now </param>
        /// <returns></returns>
        public List<TopicMessageBase> GetKafkaMessages(KafkaSettings settings, string topicPrefix, string[] topics, int countOfMessages = 100, double addHours = -1)
        {
            var messages = new List<TopicMessageBase>();
            var pollingTimeout = TimeSpan.FromMilliseconds(10000);
            
            using var consumer = new MessageConsumerFactory(settings)
                .CreateMessageConsumer(topicPrefix, topics)
                .CreateConsumerAndStartFrom(DateTime.Now.AddHours(addHours));
            
            
            while (messages.Count <= countOfMessages)
            {
                
                var consumeResult = consumer.Consume(pollingTimeout);
                
                if (consumeResult == null || consumeResult.IsPartitionEOF)
                {
                    Log.Warning("Reached end of topic. Nothing to consume...");
                    break;
                }
                
                if (consumeResult.Message == null)
                    continue;
                
                var headers = new Dictionary<string, string>();
                foreach (var header in consumeResult.Message.Headers)
                    headers[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
                
                var messageValue = new TopicMessageBase(consumeResult.Message.Value, headers);
                try
                {
                    messages.Add(messageValue);
                    Log.Information($"Consumed message {messageValue.Body} at {consumeResult.Offset}");
                }
                catch(Exception ex)
                {
                    Log.Error(ex, "Failed to consume!");
                }
            }

            consumer.Close();

            return messages;
        }
    }
}
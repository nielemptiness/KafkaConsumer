using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using KafkaConsumerExample.Base;
using KafkaConsumerExample.Interfaces;
using Serilog;

namespace KafkaConsumerExample.Consumer
{
    public class MessageConsumer : IMessageConsumer
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly string[] _topics;

        public MessageConsumer(KafkaSettings settings, string topicPrefix, params string[] topics)
        {
            if (string.IsNullOrWhiteSpace(settings.Servers))
                throw new ArgumentNullException("Servers");
            
            if (string.IsNullOrWhiteSpace(settings.GroupId))
                throw new ArgumentNullException("GroupId");
            
            var consumerConfig = new ConsumerConfig();
            consumerConfig.BootstrapServers = settings.Servers;
            consumerConfig.GroupId = settings.GroupId + topicPrefix;
            consumerConfig.AllowAutoCreateTopics = settings.AllowAutoCreateTopics;
            consumerConfig.AutoOffsetReset = settings.AutoOffsetReset ?? AutoOffsetReset.Latest;
            consumerConfig.ClientId =  string.IsNullOrEmpty(settings.ClientId) ? Guid.NewGuid().ToString() : settings.ClientId+topicPrefix;
            consumerConfig.EnableAutoCommit = true;
            _consumerConfig = consumerConfig;
            _topics = topics;
        }

        public ConsumerBuilder<Ignore, string> CreateConsumerBuilder()
        {
            var consumerBuilder = new ConsumerBuilder<Ignore, string>(_consumerConfig);
            consumerBuilder.SetErrorHandler((sender, error)
                => Log.Error("Error occured while consuming messages {@error}.", (object) error));
            
            consumerBuilder.SetLogHandler((sender, message) 
                => Log.Information(message.ToString()));
            
            consumerBuilder.SetStatisticsHandler((sender, stat) 
                => Log.Debug("Kafka statistics: {stat}.", (object) stat));
            
            return consumerBuilder;
        }

        public IConsumer<Ignore, string> CreateConsumer()
        {
            IConsumer<Ignore, string> consumer = CreateConsumerBuilder().Build();
            consumer.Subscribe((IEnumerable<string>) _topics);
            return consumer;
        }
        
        public IConsumer<Ignore, string> CreateConsumerAndStartFrom(DateTime timeStamp)
        {
            var consumer = CreateConsumerBuilder()
                .SetPartitionsAssignedHandler(((c, list) =>
            {
                var topicPartitionOffsetList = c.OffsetsForTimes(list.Select(partition 
                    => new TopicPartitionTimestamp(partition, new Timestamp(timeStamp))),
                    TimeSpan.FromMilliseconds(3000));
                
                topicPartitionOffsetList.ForEach((offset 
                    => Log.Debug($"Consumer - found {offset} at {timeStamp}")));
                
                return (IEnumerable<TopicPartitionOffset>) topicPartitionOffsetList;
            })).Build();
            
            consumer.Subscribe(_topics);
            return consumer;
        }
    }
}
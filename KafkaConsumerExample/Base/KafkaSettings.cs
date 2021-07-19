namespace KafkaConsumerExample.Base
{
    public class KafkaSettings
    {
        public string Servers { get; set; }

        public string ClientId { get; set; }
        public string GroupId { get; set; }
        public Confluent.Kafka.AutoOffsetReset? AutoOffsetReset { get; set; }
        public bool AllowAutoCreateTopics { get; set; } = false;
    }
}
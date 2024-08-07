namespace Apacha.Kafka.Console.Base.Services;

public class KafkaConsumerService
{
    public static void ConsumeSimpleMessageWithNullKey(string topicName,string groupName)
    {
        //var producer = new ProducerBuilder<Null, string>(config).Build();
        var config = new ConsumerConfig()
        {
            BootstrapServers = KafkaConstants.BootstrapServers,
            GroupId = groupName,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var consumer = new ConsumerBuilder<Null, string>(config).Build();
        consumer.Subscribe(topicName);
        while (true) {
            var consumeResult = consumer.Consume(5000);
            if (consumeResult != null)
                System.Console.WriteLine($"Key:{consumeResult?.Key} Value : {consumeResult.Value}");
        }
    }
}

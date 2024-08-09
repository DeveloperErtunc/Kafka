namespace Apacha.Kafka.Console.Base.Services;

public class KafkaConsumerService:BaseKafkaService
{
    public void ConsumeSimpleMessageWithNullKey(string topicName, string groupName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = KafkaConstants.BootstrapServers,
            GroupId = groupName,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var consumer = new ConsumerBuilder<Null, string>(config).Build();
        consumer.Subscribe(topicName);
        var count = 0;
        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            if (consumeResult != null)
            {
                System.Console.WriteLine($"Key:{consumeResult?.Message?.Key}, Value : {consumeResult.Message.Value}, ConsumerGroupName:{groupName}");
                ++count;
            }
            else
            {
                if (count > 0)
                {
                    count = 0;
                    System.Console.WriteLine("Completed");
                    System.Console.WriteLine("");
                    System.Console.WriteLine("");
                }
            }
        }
    }
    public void ConsumeSimpleMessageWithKey(string topicName, string groupName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = KafkaConstants.BootstrapServers,
            GroupId = groupName,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var consumer = new ConsumerBuilder<int, string>(config).Build();
        consumer.Subscribe(topicName);
        var count = 0;
        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            if (consumeResult != null)
            {
                System.Console.WriteLine($"Key:{consumeResult?.Message?.Key}, Value : {consumeResult.Value}, ConsumerGroupName:{groupName}");
                ++count;
            }
            else
            {
                if (count > 0)
                {
                    count = 0;
                    System.Console.WriteLine("Completed");
                    System.Console.WriteLine("");
                    System.Console.WriteLine("");
                }
            }
        }
    }

    public void ConsumeComplexMessageWithKey<T>(string topicName, string groupName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = KafkaConstants.BootstrapServers,
            GroupId = groupName,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var consumer = new ConsumerBuilder<int, T>(config).SetValueDeserializer(new CustomeValueDeSerilizer<T>()).Build();
        consumer.Subscribe(topicName);
        var count = 0;
        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            if (consumeResult != null)
            {
                System.Console.WriteLine($"Key:{consumeResult?.Message?.Key}, Value : {JsonSerializer.Serialize(consumeResult.Value)}, ConsumerGroupName:{groupName}");
                ++count;
            }
            else
            {
                if (count > 0)
                {
                    count = 0;
                    System.Console.WriteLine("Completed");
                    System.Console.WriteLine("");
                    System.Console.WriteLine("");
                }
            }
        }
    }
}

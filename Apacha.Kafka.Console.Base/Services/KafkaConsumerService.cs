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
                var headers = consumeResult?.Headers?.Select(x => new { Key = x.Key, Value = Encoding.UTF8.GetString(x.GetValueBytes()) });
                System.Console.WriteLine( $"Key:{consumeResult?.Message?.Key}, Value : {JsonSerializer.Serialize(consumeResult.Value)}ConsumerGroupName:{groupName}");
                foreach (var header in headers)
                {
                    System.Console.WriteLine($"Header Key:{header?.Key},Header Value:{header?.Value} ");
                }
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
    public void ConsumeSubcribeAPartionWithAck<T>(string topicName, string groupName,int partion)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = KafkaConstants.BootstrapServers,
            GroupId = groupName,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit =false

        };
        var consumer = new ConsumerBuilder<int, T>(config).SetValueDeserializer(new CustomeValueDeSerilizer<T>()).Build();
        consumer.Assign(new TopicPartition(topicName,new Partition(partion)));
        var count = 0;
        while (true)
        {
            try
            {
                var consumeResult = consumer.Consume(5000);
                if (consumeResult != null)
                {
                    var headers = consumeResult?.Headers?.Select(x => new { Key = x.Key, Value = Encoding.UTF8.GetString(x.GetValueBytes()) });
                    System.Console.WriteLine($"Key:{consumeResult?.Message?.Key}, Value : {JsonSerializer.Serialize(consumeResult.Value)}ConsumerGroupName:{groupName}");
                    foreach (var header in headers)
                    {
                        System.Console.WriteLine($"Header Key:{header?.Key},Header Value:{header?.Value} ");
                    }
                    consumer.Commit(consumeResult);
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
            catch (Exception ex)
            {
                System.Console.WriteLine(ex.Message);
            }
        }
    }
    public void ConsumeComplexMessageWithComplexKey<T>(string topicName, string groupName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = KafkaConstants.BootstrapServers,
            GroupId = groupName,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var consumer = new ConsumerBuilder<MessageKey, T>(config)
            .SetValueDeserializer(new CustomeValueDeSerilizer<T>())
            .SetKeyDeserializer(new CustomeValueDeSerilizer<MessageKey>()).Build();
        consumer.Subscribe(topicName);
        var count = 0;
        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            if (consumeResult != null)
            {
                var headers = consumeResult?.Headers?.Select(x => new { Key = x.Key, Value = Encoding.UTF8.GetString(x.GetValueBytes()) });
                System.Console.WriteLine($"Key:{consumeResult?.Message?.Key}, Value : {JsonSerializer.Serialize(consumeResult.Value)}ConsumerGroupName:{groupName}");
                foreach (var header in headers)
                {
                    System.Console.WriteLine($"Header Key:{header?.Key},Header Value:{header?.Value} ");
                }
            System.Console.WriteLine($"key1 {consumeResult.Key.key},key 2 :{consumeResult.Key.key2}");


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

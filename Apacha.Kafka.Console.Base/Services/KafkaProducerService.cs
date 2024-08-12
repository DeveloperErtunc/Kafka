namespace Apacha.Kafka.Console.Base.Service;

public class KafkaProducerService : BaseKafkaService
{
    static ProducerConfig Config = new ProducerConfig()
    {
        BootstrapServers = KafkaConstants.BootstrapServers
    };
    static Random Rand = new Random();
    public async Task SendSimpleMessageWithKey(string message, string topic, int count)
    {
        var producer = new ProducerBuilder<int, string>(Config).Build();
        for (int i = 0; i < count; i++)
        {
            var key = Rand.Next(0, 3);

            var body = new Message<int, string>() { Value = $"{message} {i}", Key = key };
            var result = await producer.ProduceAsync(topic, body);
            System.Console.WriteLine($"Created {i} topic:{topic}");
        }
    }

    public async Task SendComplexMessageWithKey(string topic, int count)
    {
        var producer = new ProducerBuilder<int, OrderCreatedEvent>(Config)
            .SetValueSerializer(new CustomeValueSerilizer<OrderCreatedEvent>())
            .Build();
        for (int i = 0; i < count; i++)
        {
            var orderEvent = new OrderCreatedEvent(i.ToString(), i * 100, Rand.Next(0, int.MaxValue));
            var body = new Message<int, OrderCreatedEvent>()
            {
                Value = orderEvent,
                Key = Rand.Next(0, 3)
            };
            var result = await producer.ProduceAsync(topic, body);
            System.Console.WriteLine($"{JsonSerializer.Serialize(orderEvent)} Count {i} topic:{topic}");
        }

    }
    public async Task SendComplexMessageSpesificPartionWithKey(string topic, int count)
    {
        var producer = new ProducerBuilder<int, OrderCreatedEvent>(Config)
            .SetValueSerializer(new CustomeValueSerilizer<OrderCreatedEvent>())
            .Build();
        for (int i = 0; i < count; i++)
        {
            var orderEvent = new OrderCreatedEvent(i.ToString(), i * 100, Rand.Next(0, int.MaxValue));
            var body = new Message<int, OrderCreatedEvent>()
            {
                Value = orderEvent,
                Key = Rand.Next(0, 3)
            };
            var result = await producer.ProduceAsync(new TopicPartition(topic, new Partition(2)), body);
            System.Console.WriteLine($"{JsonSerializer.Serialize(orderEvent)} Count {i} topic:{topic}");
        }

    }
    public async Task SendComplexMessageAndHeaderWithKey(string topic, int count)
    {
        var producer = new ProducerBuilder<int, OrderCreatedEvent>(Config)
            .SetValueSerializer(new CustomeValueSerilizer<OrderCreatedEvent>())
            .Build();
        for (int i = 0; i < count; i++)
        {
            var orderEvent = new OrderCreatedEvent(i.ToString(), i * 100, Rand.Next(0, int.MaxValue));
            var body = new Message<int, OrderCreatedEvent>()
            {
                Value = orderEvent,
                Key = Rand.Next(0, 3),
                Headers = new Headers() 
                { 
                    { KafkaConstants.Header_Correlation_Id,Encoding.UTF8.GetBytes("1231")},
                    { KafkaConstants.Header_Version,Encoding.UTF8.GetBytes("v1")},
                }
            };
            var result = await producer.ProduceAsync(topic, body);
            System.Console.WriteLine($"{JsonSerializer.Serialize(orderEvent)} Count {i} topic:{topic},Header sended");
        }

    }
    public async Task SendComplexMessageAndHeaderWithComplexKey(string topic, int count)
    {
        var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(Config)
            .SetValueSerializer(new CustomeValueSerilizer<OrderCreatedEvent>())
            .SetKeySerializer(new CustomeValueSerilizer<MessageKey>())

            .Build();
        for (int i = 0; i < count; i++) 
        {
            var orderEvent = new OrderCreatedEvent(i.ToString(), i * 100, Rand.Next(0, int.MaxValue));
            var body = new Message<MessageKey, OrderCreatedEvent>()
            {
                Value = orderEvent,
                Headers = new Headers()
                {
                    { KafkaConstants.Header_Correlation_Id,Encoding.UTF8.GetBytes("1231")},
                    { KafkaConstants.Header_Version,Encoding.UTF8.GetBytes("v1")},
                },
                Key =new MessageKey(Rand.Next(0,3).ToString(), Rand.Next(0, 3).ToString())
            };
            var result = await producer.ProduceAsync(topic, body);
            System.Console.WriteLine($"{JsonSerializer.Serialize(orderEvent)} Count {i} topic:{topic},Header sended");
        }

    }

    public async Task SendSimpleMessageWithNullKey(string message, string topic, int count)
    {
        var producer = new ProducerBuilder<Null, string>(Config).Build();
        for (int i = 0; i < count; i++)
        {
            var body = new Message<Null, string>() { Value = $"{message} {i}" };
            var result = await producer.ProduceAsync(topic, body);
            System.Console.WriteLine($"Created {i} topic:{topic}");
        }
    }
}

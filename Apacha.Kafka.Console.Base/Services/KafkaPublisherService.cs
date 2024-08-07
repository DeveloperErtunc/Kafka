namespace Apacha.Kafka.Console.Base.Service;

public class KafkaPublisherService
{
    public ProducerConfig? Config { get; set; }
    public IProducer<Null, string> Producer { get; set; }
    public IAdminClient AdminClient { get; set; }
    public KafkaPublisherService()
    {
        AdminClient=  new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = KafkaConstants.BootstrapServers
        }).Build();

        Config = new ProducerConfig()
        {
            BootstrapServers = KafkaConstants.BootstrapServers,
        };
        Producer = new ProducerBuilder<Null, string>(Config).Build();
    }
    public  async Task CreateTopic(List<string> topicsToCreate)
    {

        try
        {
            var metadata = AdminClient.GetMetadata(TimeSpan.FromSeconds(3));
            var topics = metadata.Topics.Select(x => x.Topic).ToList();
            topicsToCreate = topicsToCreate.Where(x => !topics.Contains(x)).ToList();
            var isValidToCrete = topicsToCreate.Select(x => new TopicSpecification()
            {
                Name = x,
                NumPartitions = 3,
                ReplicationFactor = 1

            }).ToList();
            await AdminClient.CreateTopicsAsync(isValidToCrete);
            System.Console.WriteLine($"Created {string.Join(",", isValidToCrete.Select(x => x.Name).ToList())}");

        }
        catch (Exception ex)
        {
            System.Console.WriteLine(ex.Message);
        }
    }
    public  async Task SendSimpleMessageWithNullKey(string message, string topic)
    {
        var body = new Message<Null, string>() { Value = message };
        var result = await Producer.ProduceAsync(topic, body);
        foreach (var item in result.GetType().GetProperties())
        {
            System.Console.WriteLine($"Name:{item.Name} Value : {item. GetValue(result)}");
        }
    }
}

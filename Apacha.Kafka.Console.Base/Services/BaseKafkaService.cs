namespace Apacha.Kafka.Console.Base.Services;

public class BaseKafkaService
{
    public List<string> GetTopics()
    {
        var adminConfig = new AdminClientConfig()
        {
            BootstrapServers = KafkaConstants.BootstrapServers
        };

        var adminClient = new AdminClientBuilder(adminConfig).Build();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        var topicsMetadata = metadata.Topics;
        var topicNames = metadata.Topics.Select(a => a.Topic).ToList();
        return topicNames;
    }
    public async Task CreateTopic(List<string> topicsToCreate)
    {
        var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = KafkaConstants.BootstrapServers
        }).Build();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));
        var topics = metadata.Topics.Select(x => x.Topic).ToList();
        topicsToCreate = topicsToCreate.Where(x => !topics.Contains(x)).ToList();
        var isValidToCrete = topicsToCreate.Select(x => new TopicSpecification()
        {
            Name = x,
            NumPartitions = 3,
            ReplicationFactor = 1

        }).ToList();
        if (isValidToCrete?.Any() == true)
        {
            await adminClient.CreateTopicsAsync(isValidToCrete);
            System.Console.WriteLine($"Created {string.Join(",", isValidToCrete.Select(x => x.Name).ToList())}");
        }
    }
}

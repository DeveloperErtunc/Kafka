KafkaPublisherService kafkaService = new KafkaPublisherService();
 
await kafkaService.CreateTopic(new List<string> { KafkaConstants.UseCaseOne });

for (; ; )
{
    for (int i = 0; i <15; i++)
    {
        await kafkaService.SendSimpleMessageWithNullKey($"Hello Word {i}", KafkaConstants.UseCaseOne);
    }
    Console.WriteLine("Press enter send message again to kafka");
    Console.ReadLine();
}



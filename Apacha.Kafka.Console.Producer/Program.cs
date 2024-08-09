var service = new KafkaPublisherService();
await service.CreateTopic(new List<string> { KafkaConstants.UseCaseOne,KafkaConstants.UseCaseTwo, KafkaConstants.UseCaseThree });
for (; ; )
{
    Console.WriteLine($"press 1 for case that  key is null and string message  kafka partion balance its self ");
    Console.WriteLine($"press 2 for case that  key is int and string message kafka partion balance its self ");
    Console.WriteLine($"press 2 for case that  key is int and OrderCreatedEvent message kafka partion balance its self ");

    var cases = Console.ReadLine();
    if (int.TryParse(cases, out var count))
    {
        var tasks = new List<Task>();
        if (count == 1)
            await service.SendSimpleMessageWithNullKey(KafkaConstants.UseCaseOne, KafkaConstants.UseCaseOne, 20);
        
        else if (count == 2)
            await service.SendSimpleMessageWithKey(KafkaConstants.UseCaseTwo, KafkaConstants.UseCaseTwo, 20);
        else if(count == 3)
            await service.SendSComplexMessageWithKey(KafkaConstants.UseCaseThree, 20);

    }
}

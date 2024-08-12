var service = new KafkaProducerService();
await service.CreateTopic(new List<string> { KafkaConstants.UseCaseOne,KafkaConstants.UseCaseTwo, KafkaConstants.UseCaseThree, KafkaConstants.UseCaseFour, KafkaConstants.UseCaseFive, KafkaConstants.UseCaseSix });
for (; ; )
{
    Console.WriteLine($"press 1 for case that  key is null and string message  kafka partion balance its self ");
    Console.WriteLine($"press 2 for case that  key is int and string message kafka partion balance its self ");
    Console.WriteLine($"press 3 for case that  key is int and OrderCreatedEvent message kafka partion balance its self ");
    Console.WriteLine($"press 4 for case that  key is int and OrderCreatedEvent and headers message kafka partion balance its self ");
    Console.WriteLine($"press 5 for case that  key is complex and OrderCreatedEvent and headers message kafka partion balance its self ");
    Console.WriteLine($"press 6 for case that  key is complex and OrderCreatedEvent and spesific partion  2 ");

    var cases = Console.ReadLine();
    if (int.TryParse(cases, out var count))
    {
        var tasks = new List<Task>();
        if (count == 1)
            await service.SendSimpleMessageWithNullKey(KafkaConstants.UseCaseOne, KafkaConstants.UseCaseOne, 1);
        else if (count == 2)
            await service.SendSimpleMessageWithKey(KafkaConstants.UseCaseTwo, KafkaConstants.UseCaseTwo, 1);
        else if (count == 3)
            await service.SendComplexMessageWithKey(KafkaConstants.UseCaseThree, 1);
        else if (count == 4)
            await service.SendComplexMessageAndHeaderWithKey(KafkaConstants.UseCaseFour, 1);
        else if (count == 5)
            await service.SendComplexMessageAndHeaderWithComplexKey(KafkaConstants.UseCaseFive, 1);
        else if(count == 6)
            await service.SendComplexMessageSpesificPartionWithKey(KafkaConstants.UseCaseSix, 1);

    }
}

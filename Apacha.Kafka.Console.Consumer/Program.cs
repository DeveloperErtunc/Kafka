Console.WriteLine("Start Consumer!");
KafkaConsumerService.ConsumeSimpleMessageWithNullKey(KafkaConstants.UseCaseOne, KafkaConstants.GroupId);
Console.ReadLine();
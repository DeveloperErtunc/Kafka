Console.WriteLine("Start Consumer!");
KafkaConsumerService.ConsumeSimpleMessageWithNullKey(KafkaConstants.UseCaseOne, KafkaConstants.GroupId2);
Console.ReadLine();
﻿var taks = new List<Task>();
var service = new KafkaConsumerService();
var topics =service.GetTopics();
if(topics.Any(x => x == KafkaConstants.UseCaseOne))
{
    taks.Add(Task.Run(() =>
    {
        service.ConsumeSimpleMessageWithNullKey(KafkaConstants.UseCaseOne, KafkaConstants.GroupId);
    }));
}
if (topics.Any(x => x == KafkaConstants.UseCaseTwo))
{
    taks.Add(Task.Run(() =>
    {
        service.ConsumeSimpleMessageWithKey(KafkaConstants.UseCaseTwo, KafkaConstants.GroupId2);
    }));
}
if(topics.Any(x => x == KafkaConstants.UseCaseThree))
{
    taks.Add(Task.Run(() =>
    {
        service.ConsumeComplexMessageWithKey<OrderCreatedEvent>(KafkaConstants.UseCaseThree, KafkaConstants.GroupId3);
    }));
}
var unRead = topics.Where(x => !(x != KafkaConstants.UseCaseOne || x != KafkaConstants.UseCaseTwo || x != KafkaConstants.UseCaseThree || x != KafkaConstants.Unread__consumer_offsets)).ToList();
unRead.ForEach(x => Console.WriteLine("UnCreated Topics In Kafka : "+x +"First Run Producer App"));

Console.WriteLine();
await Task.WhenAll(taks);
Console.ReadLine();


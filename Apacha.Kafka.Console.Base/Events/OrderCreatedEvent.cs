namespace Apacha.Kafka.Console.Base.Events;
public record OrderCreatedEvent
{
    public OrderCreatedEvent(string orderCode, decimal totalPrice, int userId)
    {
        OrderCode = orderCode;
        TotalPrice = totalPrice;
        UserId = userId;
    }
    public OrderCreatedEvent()
    {
        
    }
    public string OrderCode { get; init; } = default!;
    public decimal TotalPrice { get; set; }
    public int UserId { get; set; }

}
public record MessageKey(string key,string key2);
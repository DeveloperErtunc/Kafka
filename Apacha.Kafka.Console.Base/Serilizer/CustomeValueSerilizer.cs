namespace Apacha.Kafka.Console.Base.Helpers;

internal class CustomeValueSerilizer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data, typeof(T))) ;
    }
}
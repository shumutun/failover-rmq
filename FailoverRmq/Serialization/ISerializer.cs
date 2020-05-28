using System;

namespace FailoverRmq.Serialization
{
    public interface ISerializer<T>
    {
        ReadOnlyMemory<byte> Serialize(T obj);
        T Deserialize(ReadOnlyMemory<byte> src);
    }
}

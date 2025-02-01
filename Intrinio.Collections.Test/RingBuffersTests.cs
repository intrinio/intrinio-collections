using System.Buffers.Binary;
using System.Diagnostics;

namespace Intrinio.Collections.Test;

using Intrinio.Collections.RingBuffers;

[TestClass]
public class RingBuffersTests
{
    [TestMethod]
    public void DelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        DelayDynamicBlockSingleProducerRingBuffer ringBuffer = new DelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, null);
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsFalse(ringBuffer.TryDequeue(buffer), "Dequeue should be unsuccessful.");
        Thread.Sleep(delayInMilliseconds + 1); //Wait delay time
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    public void DelayDynamicBlockSingleProducerRingBuffer_HalfFull()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        DelayDynamicBlockSingleProducerRingBuffer ringBuffer = new DelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, null);
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        for (int i = 0; i < capacity / 2; i++)
        {
            Thread.Sleep(1);
            Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Enqueue should be successful.");
        }
        
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsFalse(ringBuffer.TryDequeue(buffer), "Dequeue should be unsuccessful.");
        
        Thread.Sleep(delayInMilliseconds + 1); //Wait delay time
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(capacity / 2 - 1, ringBuffer.Count, "Queue depth should be equal to half minus one.");
        
        
        for (int i = 0; i < capacity / 2 - 1; i++)
        {
            Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        }
        
        Assert.AreEqual(0UL, ringBuffer.Count, "Queue depth should be zero.");
    }
    
    [TestMethod]
    public void DelayDynamicBlockSingleProducerRingBuffer_Full()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        DelayDynamicBlockSingleProducerRingBuffer ringBuffer = new DelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, null);
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        for (int i = 0; i < capacity; i++)
        {
            Thread.Sleep(1);
            Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Enqueue should be successful.");
        }
        
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsFalse(ringBuffer.TryDequeue(buffer), "Dequeue should be unsuccessful.");
        
        Thread.Sleep(delayInMilliseconds + 1); //Wait delay time
        Assert.AreEqual(capacity, ringBuffer.Count, "Queue depth should be equal full.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(capacity - 1, ringBuffer.Count, "Queue depth should be equal to full minus one.");
        
        
        for (int i = 0; i < capacity - 1; i++)
        {
            Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Remaining Dequeues should be successful.");
        }
        
        Assert.AreEqual(0UL, ringBuffer.Count, "Queue depth should be zero.");
    }
    
    [TestMethod]
    public void DelayDynamicBlockSingleProducerRingBuffer_Overflow()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        DelayDynamicBlockSingleProducerRingBuffer ringBuffer = new DelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, null);
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        for (int i = 0; i < capacity; i++)
        {
            Thread.Sleep(1);
            Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Enqueue should be successful.");
        }
        
        for (int i = 0; i < capacity; i++)
        {
            Thread.Sleep(1);
            Assert.IsFalse(ringBuffer.TryEnqueue(buffer), "Enqueue should be unsuccessful.");
        }
        
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsFalse(ringBuffer.TryDequeue(buffer), "Dequeue should be unsuccessful due to not delayed enough yet.");
        
        Thread.Sleep(delayInMilliseconds + 1); //Wait delay time
        Assert.AreEqual(capacity, ringBuffer.Count, "Queue depth should be equal full.");
        Assert.AreEqual(capacity, ringBuffer.DropCount, $"Drops should be {capacity}.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(capacity - 1, ringBuffer.Count, "Queue depth should be equal to full minus one.");
        
        
        for (int i = 0; i < capacity - 1; i++)
        {
            Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Remaining Dequeues should be successful.");
        }
        
        Assert.AreEqual(0UL, ringBuffer.Count, "Queue depth should be zero.");
    }
    
    [TestMethod]
    public void DelayDynamicBlockSingleProducerRingBuffer_Timing()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        long slop = 10L;
        DelayDynamicBlockSingleProducerRingBuffer ringBuffer = new DelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, null);
        
        Span<byte> writeBuffer = stackalloc byte[8];
        Span<byte> readBuffer = stackalloc byte[8];
        Thread.Sleep(10);
        uint dequeued = 0;
        
        Stopwatch sw = Stopwatch.StartNew();
        
        for (int i = 0; i < capacity; i++)
        {
            BinaryPrimitives.WriteUInt64BigEndian(writeBuffer, value);
            Assert.IsTrue(ringBuffer.TryEnqueue(writeBuffer), "Enqueue should be successful.");
        }
        
        while (dequeued < capacity)
        {
            if (ringBuffer.TryDequeue(readBuffer))
                dequeued++;
        }
        sw.Stop();
        
        Assert.IsTrue(sw.ElapsedMilliseconds + slop >= delayInMilliseconds, "Dequeue should be delayed by the given time.");
    }
}
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;

namespace Intrinio.Collections.Test;

using Intrinio.Collections.RingBuffers;

[TestClass]
public class RingBuffersTests
{
    #region PartitionedRoundRobinDelayDynamicBlockRingBuffer
    [TestMethod]
    public void PartitionedRoundRobinDelayDynamicBlockRingBuffer_EnqueueDequeue()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        PartitionedRoundRobinDelayDynamicBlockRingBuffer ringBuffer = new PartitionedRoundRobinDelayDynamicBlockRingBuffer(2U, blockSize, capacity, Convert.ToUInt32(delayInMilliseconds), null);
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(0,buffer), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryEnqueue(1,buffer), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsFalse(ringBuffer.TryDequeue(buffer), "Dequeue should be unsuccessful.");
        Thread.Sleep(delayInMilliseconds + 1); //Wait delay time
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }
    #endregion
    
    #region DelayDynamicBlockSingleProducerRingBuffer
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
            Assert.IsFalse(ringBuffer.TryEnqueue(buffer), "Overflow Enqueue should be unsuccessful.");
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
    
    #endregion //DelayDynamicBlockSingleProducerRingBuffer

    [TestMethod]
    public void MMTest()
    {
        ulong blockCapacity = 1000;
        uint blockSize = 512u;
        ulong targetPageSize = 4096UL;
        ulong dataPageSize = (targetPageSize / Convert.ToUInt64(blockSize)) * Convert.ToUInt64(blockSize);
        string dataFilePath = Path.Combine(System.IO.Path.GetTempPath(), "TestData.bin");
        try
        {
            if (File.Exists(dataFilePath))
                File.Delete(dataFilePath);
            using (var fs = new FileStream(dataFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                fs.SetLength(Convert.ToInt64(blockCapacity) * Convert.ToInt64(blockSize));
            }
            var data = MemoryMappedFile.CreateFromFile(dataFilePath);
            var dataWriteAccessor = data.CreateViewAccessor(0, Convert.ToInt64(dataPageSize), MemoryMappedFileAccess.ReadWrite);
            var dataReadAccessor = data.CreateViewAccessor(0, Convert.ToInt64(dataPageSize), MemoryMappedFileAccess.Read);

            ulong expected = 42UL;
            dataWriteAccessor.Write(0L, expected);
            dataReadAccessor.Read(0L, out ulong value);
            Assert.AreEqual(expected, value); //expect that the read can see the write without flushing.
        }
        catch (Exception e)
        {
            Assert.Fail(e.Message);
        }
        finally
        {
            File.Delete(dataFilePath);
        }
    }
    
    #region MemMapDelayDynamicBlockSingleProducerRingBuffer
    
    [TestMethod]
    public void MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsFalse(ringBuffer.TryDequeue(buffer), "Dequeue should be unsuccessful.");
        Thread.Sleep(delayInMilliseconds + 1); //Wait delay time
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockSingleProducerRingBuffer_HalfFull()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_HalfFull)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_HalfFull)}.bin");
        
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockSingleProducerRingBuffer_Full()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_Full)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_Full)}.bin");
        
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockSingleProducerRingBuffer_Overflow()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_Overflow)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_Overflow)}.bin");
        
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
            Assert.IsFalse(ringBuffer.TryEnqueue(buffer), "Overflow Enqueue should be unsuccessful.");
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockSingleProducerRingBuffer_Timing()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        long slop = 10L;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_Timing)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_Timing)}.bin");
        
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockSingleProducerRingBuffer_PageSizeAlignedWithRequiredFileSize()
    {
        int   threadCount         = 1;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 64u;
        ulong pageSize            = blockSize << 4;
        ulong capacity            = pageSize * 10u;
        
        string                                                file       = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_PageSizeAlignedWithRequiredFileSize)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 5000));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }

                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
            threads[i].Start(ringBuffer);
        }

        //Cleanup
        for (i = 0; i < threads.Length; i++)
        {
            try
            {
                threads[i].Join();
            }
            catch (Exception e)
            {
                
            }
        }

        try
        {
            if (File.Exists(file))
                File.Delete(file);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        
        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockSingleProducerRingBuffer_PageSizeNotAlignedWithRequiredFileSize()
    {
        int   threadCount         = 1;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 64u;
        ulong pageSize            = blockSize << 4;
        ulong capacity            = pageSize * 10u + 1;
        
        string                                                file       = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_PageSizeNotAlignedWithRequiredFileSize)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 5000));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }

                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
            threads[i].Start(ringBuffer);
        }

        //Cleanup
        for (i = 0; i < threads.Length; i++)
        {
            try
            {
                threads[i].Join();
            }
            catch (Exception e)
            {
                
            }
        }

        try
        {
            if (File.Exists(file))
                File.Delete(file);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        
        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockSingleProducerRingBuffer_MultipleThreads()
    {
        int   threadCount         = 32;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 64u;
        ulong pageSize            = blockSize * 16;
        ulong capacity            = pageSize * 100u + 1;
        
        string                                                file       = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_MultipleThreads)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                Random     ran                   = new Random();
                Thread.Sleep(ran.Next(0, 100));
                var        threadLocalRingBuffer = (MemMapDelayDynamicBlockRingBuffer)o;
                Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong i = 0; i < capacity; i++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                while (threadLocalRingBuffer.TryDequeue(buffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong i = 0; i < capacity; i++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
                    
                while (threadLocalRingBuffer.TryDequeue(buffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });
        threads[i].Start(ringBuffer);

        ++i;
        
        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 100));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
            threads[i].Start(ringBuffer);
        }

        //Cleanup
        for (i = 0; i < threads.Length; i++)
        {
            try
            {
                threads[i].Join();
            }
            catch (Exception e)
            {
                
            }
        }

        try
        {
            if (File.Exists(file))
                File.Delete(file);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        
        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    #endregion //MemMapDelayDynamicBlockSingleProducerRingBuffer
    
    #region MemMapDelayDynamicBlockRingBuffer
    
    [TestMethod]
    public void MemMapDelayDynamicBlockRingBuffer_EnqueueDequeue()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_EnqueueDequeue)}.bin");
        using MemMapDelayDynamicBlockRingBuffer ringBuffer = new MemMapDelayDynamicBlockRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_EnqueueDequeue)}.bin");
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsFalse(ringBuffer.TryDequeue(buffer), "Dequeue should be unsuccessful.");
        Thread.Sleep(delayInMilliseconds + 1); //Wait delay time
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockRingBuffer_HalfFull()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_HalfFull)}.bin");
        using MemMapDelayDynamicBlockRingBuffer ringBuffer = new MemMapDelayDynamicBlockRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_HalfFull)}.bin");
        
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockRingBuffer_Full()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_Full)}.bin");
        using MemMapDelayDynamicBlockRingBuffer ringBuffer = new MemMapDelayDynamicBlockRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_Full)}.bin");
        
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockRingBuffer_Overflow()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_Overflow)}.bin");
        using MemMapDelayDynamicBlockRingBuffer ringBuffer = new MemMapDelayDynamicBlockRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_Overflow)}.bin");
        
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
            Assert.IsFalse(ringBuffer.TryEnqueue(buffer), "Overflow Enqueue should be unsuccessful.");
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockRingBuffer_Timing()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        long slop = 10L;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_Timing)}.bin");
        using MemMapDelayDynamicBlockRingBuffer ringBuffer = new MemMapDelayDynamicBlockRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_Timing)}.bin");
        
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockRingBuffer_PageSizeAlignedWithRequiredFileSize()
    {
        int   threadCount         = 1;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 64u;
        ulong pageSize            = blockSize << 4;
        ulong capacity            = pageSize * 10u;
        
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_PageSizeAlignedWithRequiredFileSize)}.bin");
        using MemMapDelayDynamicBlockRingBuffer ringBuffer = new MemMapDelayDynamicBlockRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 5000));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }

                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
            threads[i].Start(ringBuffer);
        }

        //Cleanup
        for (i = 0; i < threads.Length; i++)
        {
            try
            {
                threads[i].Join();
            }
            catch (Exception e)
            {
                
            }
        }

        try
        {
            if (File.Exists(file))
                File.Delete(file);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        
        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockRingBuffer_PageSizeNotAlignedWithRequiredFileSize()
    {
        int   threadCount         = 1;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 64u;
        ulong pageSize            = blockSize << 4;
        ulong capacity            = pageSize * 10u + 1;
        
        string                                  file       = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_PageSizeNotAlignedWithRequiredFileSize)}.bin");
        using MemMapDelayDynamicBlockRingBuffer ringBuffer = new MemMapDelayDynamicBlockRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 5000));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }

                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
            threads[i].Start(ringBuffer);
        }

        //Cleanup
        for (i = 0; i < threads.Length; i++)
        {
            try
            {
                threads[i].Join();
            }
            catch (Exception e)
            {
                
            }
        }

        try
        {
            if (File.Exists(file))
                File.Delete(file);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        
        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockRingBuffer_MultipleThreads()
    {
        int   threadCount         = 32;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 64u;
        ulong pageSize            = blockSize * 16;
        ulong capacity            = pageSize * 100u + 1;
        
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockRingBuffer_MultipleThreads)}.bin");
        using MemMapDelayDynamicBlockRingBuffer ringBuffer = new MemMapDelayDynamicBlockRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 100));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }

                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
            threads[i].Start(ringBuffer);
        }

        //Cleanup
        for (i = 0; i < threads.Length; i++)
        {
            try
            {
                threads[i].Join();
            }
            catch (Exception e)
            {
                
            }
        }

        try
        {
            if (File.Exists(file))
                File.Delete(file);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        
        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    #endregion //MemMapDelayDynamicBlockRingBuffer
    
    #region MemMapDelayDynamicBlockDropOldestRingBuffer
    
    [TestMethod]
    public void MemMapDelayDynamicBlockDropOldestRingBuffer_EnqueueDequeue()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_EnqueueDequeue)}.bin");
        using MemMapDelayDynamicBlockDropOldestRingBuffer ringBuffer = new MemMapDelayDynamicBlockDropOldestRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_EnqueueDequeue)}.bin");
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsFalse(ringBuffer.TryDequeue(buffer), "Dequeue should be unsuccessful.");
        Thread.Sleep(delayInMilliseconds + 1); //Wait delay time
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockDropOldestRingBuffer_HalfFull()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_HalfFull)}.bin");
        using MemMapDelayDynamicBlockDropOldestRingBuffer ringBuffer = new MemMapDelayDynamicBlockDropOldestRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_HalfFull)}.bin");
        
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockDropOldestRingBuffer_Full()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_Full)}.bin");
        using MemMapDelayDynamicBlockDropOldestRingBuffer ringBuffer = new MemMapDelayDynamicBlockDropOldestRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_Full)}.bin");
        
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockDropOldestRingBuffer_Overflow()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_Overflow)}.bin");
        using MemMapDelayDynamicBlockDropOldestRingBuffer ringBuffer = new MemMapDelayDynamicBlockDropOldestRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_Overflow)}.bin");
        
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
            Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Overflow Enqueue should be successful.");
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockDropOldestRingBuffer_Timing()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        long slop = 10L;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_Timing)}.bin");
        using MemMapDelayDynamicBlockDropOldestRingBuffer ringBuffer = new MemMapDelayDynamicBlockDropOldestRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_Timing)}.bin");
        
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
        
        if (File.Exists(file))
            File.Delete(file);
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockDropOldestRingBuffer_PageSizeAlignedWithRequiredFileSize()
    {
        int   threadCount         = 1;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 64u;
        ulong pageSize            = blockSize << 4;
        ulong capacity            = pageSize * 10u;
        
        string                                            file       = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_PageSizeAlignedWithRequiredFileSize)}.bin");
        using MemMapDelayDynamicBlockDropOldestRingBuffer ringBuffer = new MemMapDelayDynamicBlockDropOldestRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 5000));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }

                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
            threads[i].Start(ringBuffer);
        }

        //Cleanup
        for (i = 0; i < threads.Length; i++)
        {
            try
            {
                threads[i].Join();
            }
            catch (Exception e)
            {
                
            }
        }

        try
        {
            if (File.Exists(file))
                File.Delete(file);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        
        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockDropOldestRingBuffer_PageSizeNotAlignedWithRequiredFileSize()
    {
        int   threadCount         = 1;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 64u;
        ulong pageSize            = blockSize << 4;
        ulong capacity            = pageSize * 10u + 1;
        
        string                                            file       = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_PageSizeNotAlignedWithRequiredFileSize)}.bin");
        using MemMapDelayDynamicBlockDropOldestRingBuffer ringBuffer = new MemMapDelayDynamicBlockDropOldestRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 5000));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }

                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
            threads[i].Start(ringBuffer);
        }

        //Cleanup
        for (i = 0; i < threads.Length; i++)
        {
            try
            {
                threads[i].Join();
            }
            catch (Exception e)
            {
                
            }
        }

        try
        {
            if (File.Exists(file))
                File.Delete(file);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        
        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    [TestMethod]
    public void MemMapDelayDynamicBlockDropOldestRingBuffer_MultipleThreads()
    {
        int   threadCount         = 32;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 64u;
        ulong pageSize            = blockSize * 16;
        ulong capacity            = pageSize * 100u + 1;
        
        string                                            file       = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockDropOldestRingBuffer_MultipleThreads)}.bin");
        using MemMapDelayDynamicBlockDropOldestRingBuffer ringBuffer = new MemMapDelayDynamicBlockDropOldestRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin");
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 100));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }

                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    for (ulong i = 0; i < capacity; i++)
                    {
                        threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
            threads[i].Start(ringBuffer);
        }

        //Cleanup
        for (i = 0; i < threads.Length; i++)
        {
            try
            {
                threads[i].Join();
            }
            catch (Exception e)
            {
                
            }
        }

        try
        {
            if (File.Exists(file))
                File.Delete(file);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        
        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    #endregion //MemMapDelayDynamicBlockDropOldestRingBuffer

    #region DynamicBlockSingleProducerRingBuffer

    [TestMethod]
    public void DynamicBlockSingleProducerRingBuffer_EnqueueDequeue()
    {
        ulong                                value      = 5UL;
        uint                                 blockSize  = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint                                 capacity   = 10u;
        DynamicBlockSingleProducerRingBuffer ringBuffer = new DynamicBlockSingleProducerRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(value,         BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(sizeof(ulong), trimmedBuffer.Length,"Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }
    
    [TestMethod]
    public void DynamicBlockSingleProducerRingBuffer_ZeroUsedLength()
    {
        ulong                                value      = 5UL;
        uint                                 blockSize  = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint                                 capacity   = 10u;
        DynamicBlockSingleProducerRingBuffer ringBuffer = new DynamicBlockSingleProducerRingBuffer(blockSize, capacity);
        
        Span<byte> buffer        = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, 0)), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(0, trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    #endregion //DynamicBlockSingleProducerRingBuffer
}
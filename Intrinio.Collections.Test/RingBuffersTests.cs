using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;

namespace Intrinio.Collections.Test;

using Intrinio.Collections.RingBuffers;

[DoNotParallelize]
[TestClass]
public class RingBuffersTests
{
    public void StartStopThreads(Thread[] threads, object threadData)
    {
        foreach (Thread thread in threads)
            thread.Start(threadData);
        
        CancellationTokenSource cts = new CancellationTokenSource();
        cts.CancelAfter(120000);

        Thread.Sleep(60_000);

        //Cleanup
        Parallel.ForEach(threads, new ParallelOptions(){CancellationToken = cts.Token}, thread =>
        {
            try
            {
                if (thread.IsAlive)
                    thread.Join(5000);
                if (thread.IsAlive)
                    thread.Abort();
            }
            catch (Exception e) { }
        });
    }
    
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

    #region DropOldestRingBuffer

    [TestMethod]
    public void DropOldestRingBuffer_EnqueueDequeue()
    {
        ulong      value      = 5UL;
        uint       blockSize  = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint       capacity   = 10u;
        DropOldestRingBuffer ringBuffer = new DropOldestRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    public void DropOldestRingBuffer_T_EnqueueDequeue()
    {
        ulong             value      = 5UL;
        uint              blockSize  = sizeof(ulong);
        uint              capacity   = 10u;
        DropOldestRingBuffer<ulong> ringBuffer = new DropOldestRingBuffer<ulong>(capacity);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(value),            "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(out ulong result), "Dequeue should be successful.");
        Assert.AreEqual(value, result, "Dequeued value should be equal to the original value.");
    }

    #endregion //DropOldestRingBuffer
    
    #region DynamicBlockDropOldestRingBuffer

    [TestMethod]
    public void DynamicBlockDropOldestRingBuffer_EnqueueDequeue()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        DynamicBlockDropOldestRingBuffer ringBuffer = new DynamicBlockDropOldestRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(sizeof(ulong), trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    [TestMethod]
    public void DynamicBlockDropOldestRingBuffer_ZeroUsedLength()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        DynamicBlockDropOldestRingBuffer ringBuffer = new DynamicBlockDropOldestRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, 0)), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(0, trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    [TestMethod]
    [Timeout(150000)]
    public void DynamicBlockDropOldestRingBuffer_MultipleThreads()
    {
        int   threadCount = 4;
        ulong value       = 5UL;
        uint  blockSize   = 117u;
        ulong capacity    = blockSize * 8192u * 16 + 1;
        
        DynamicBlockDropOldestRingBuffer ringBuffer = new DynamicBlockDropOldestRingBuffer(blockSize, capacity);
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                Random ran = new Random();
                var threadLocalRingBuffer = (DynamicBlockDropOldestRingBuffer)o;
                Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });

        ++i;
        
        threads[i] = new Thread(o =>
        {
            try
            {
                Random     ran                   = new Random();
                var        threadLocalRingBuffer = (DynamicBlockDropOldestRingBuffer)o;
                Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                Span<byte> trimmedBuffer = buffer;
                while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
                    
                while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });
        
        for (++i;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    var        threadLocalRingBuffer = (DynamicBlockDropOldestRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    
                    Span<byte> trimmedBuffer = buffer;
                    while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
        }

        StartStopThreads(threads, ringBuffer);

        if (failed)
            Assert.Fail("Thread failed.");
    }

    #endregion //DynamicBlockDropOldestRingBuffer

    #region DynamicBlockNoLockDropOldestRingBuffer

    [TestMethod]
    public void DynamicBlockNoLockDropOldestRingBuffer_EnqueueDequeue()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        DynamicBlockNoLockDropOldestRingBuffer ringBuffer = new DynamicBlockNoLockDropOldestRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(sizeof(ulong), trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    [TestMethod]
    public void DynamicBlockNoLockDropOldestRingBuffer_ZeroUsedLength()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        DynamicBlockNoLockDropOldestRingBuffer ringBuffer = new DynamicBlockNoLockDropOldestRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, 0)), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(0, trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    [TestMethod]
    [Timeout(150000)]
    public void DynamicBlockNoLockDropOldestRingBuffer_MultipleThreads()
    {
        int   threadCount = 16;
        ulong value       = 5UL;
        uint  blockSize   = 117u;
        ulong capacity    = blockSize * 8192u * 16 + 1;
        
        DynamicBlockNoLockDropOldestRingBuffer ringBuffer = new DynamicBlockNoLockDropOldestRingBuffer(blockSize, capacity);
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                Random ran = new Random();
                var threadLocalRingBuffer = (DynamicBlockNoLockDropOldestRingBuffer)o;
                Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });
        
        ++i;
        
        threads[i] = new Thread(o =>
        {
            try
            {
                Random     ran                   = new Random();
                var        threadLocalRingBuffer = (DynamicBlockNoLockDropOldestRingBuffer)o;
                Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                Span<byte> trimmedBuffer = buffer;
                while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
                    
                while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });
        
        for (++i;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    var        threadLocalRingBuffer = (DynamicBlockNoLockDropOldestRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    
                    Span<byte> trimmedBuffer = buffer;
                    while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
        }

        StartStopThreads(threads,ringBuffer);

        if (failed)
            Assert.Fail("Thread failed.");
    }

    #endregion //DynamicBlockNoLockDropOldestRingBuffer

    #region DynamicBlockNoLockRingBuffer

    [TestMethod]
    public void DynamicBlockNoLockRingBuffer_EnqueueDequeue()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        DynamicBlockNoLockRingBuffer ringBuffer = new DynamicBlockNoLockRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(sizeof(ulong), trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    [TestMethod]
    public void DynamicBlockNoLockRingBuffer_ZeroUsedLength()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        DynamicBlockNoLockRingBuffer ringBuffer = new DynamicBlockNoLockRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, 0)), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(0, trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    [TestMethod]
    [Timeout(150000)]
    public void DynamicBlockNoLockRingBuffer_MultipleThreads()
    {
        int   threadCount = 16;
        ulong value       = 5UL;
        uint  blockSize   = 117u;
        ulong capacity    = blockSize * 8192u * 16 + 1;
        
        DynamicBlockNoLockRingBuffer ringBuffer = new DynamicBlockNoLockRingBuffer(blockSize, capacity);
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                Random ran = new Random();
                var threadLocalRingBuffer = (DynamicBlockNoLockRingBuffer)o;
                Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });

        ++i;
        
        threads[i] = new Thread(o =>
        {
            try
            {
                Random     ran                   = new Random();
                var        threadLocalRingBuffer = (DynamicBlockNoLockRingBuffer)o;
                Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                Span<byte> trimmedBuffer = buffer;
                while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
                    
                while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });
        
        for (++i;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    var        threadLocalRingBuffer = (DynamicBlockNoLockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    
                    Span<byte> trimmedBuffer = buffer;
                    while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
        }

        //Cleanup
        StartStopThreads(threads, ringBuffer);

        if (failed)
            Assert.Fail("Thread failed.");
    }

    #endregion //DynamicBlockNoLockRingBuffer

    #region DynamicBlockRingBuffer

    [TestMethod]
    public void DynamicBlockRingBuffer_EnqueueDequeue()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        DynamicBlockRingBuffer ringBuffer = new DynamicBlockRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(sizeof(ulong), trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    [TestMethod]
    public void DynamicBlockRingBuffer_ZeroUsedLength()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        DynamicBlockRingBuffer ringBuffer = new DynamicBlockRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, 0)), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(0, trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    [TestMethod]
    [Timeout(150000)]
    public void DynamicBlockRingBuffer_MultipleThreads()
    {
        int   threadCount = 16;
        ulong value       = 5UL;
        uint  blockSize   = 117u;
        ulong capacity    = blockSize * 8192u * 16 + 1;
        
        DynamicBlockRingBuffer ringBuffer = new DynamicBlockRingBuffer(blockSize, capacity);
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                Random ran = new Random();
                var threadLocalRingBuffer = (DynamicBlockRingBuffer)o;
                Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });

        ++i;
        
        threads[i] = new Thread(o =>
        {
            try
            {
                Random     ran                   = new Random();
                var        threadLocalRingBuffer = (DynamicBlockRingBuffer)o;
                Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                Span<byte> trimmedBuffer = buffer;
                while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
                    
                while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });
        
        for (++i;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    var        threadLocalRingBuffer = (DynamicBlockRingBuffer)o;
                    Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                    BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                    
                    Span<byte> trimmedBuffer = buffer;
                    while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(buffer, out trimmedBuffer))
                    {
                        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
        }

        //Cleanup
        StartStopThreads(threads, ringBuffer);

        if (failed)
            Assert.Fail("Thread failed.");
    }

    #endregion //DynamicBlockRingBuffer

    #region DynamicBlockUnsafeRingBuffer

    [TestMethod]
    public void DynamicBlockUnsafeRingBuffer_EnqueueDequeue()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        DynamicBlockUnsafeRingBuffer ringBuffer = new DynamicBlockUnsafeRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(trimmedBuffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(sizeof(ulong), trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    [TestMethod]
    public void DynamicBlockUnsafeRingBuffer_ZeroUsedLength()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        DynamicBlockUnsafeRingBuffer ringBuffer = new DynamicBlockUnsafeRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        Span<byte> trimmedBuffer = buffer;
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, 0)), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer, out trimmedBuffer), "Dequeue should be successful.");
        Assert.AreEqual(0, trimmedBuffer.Length, "Trimmed length should be equal to the original length.");
        Assert.AreNotEqual(blockSize, Convert.ToUInt32(trimmedBuffer.Length), "Trimmed length should not be equal to the block size if the input was sliced smaller.");
    }

    #endregion //DynamicBlockUnsafeRingBuffer

    #region LargeRingBuffer

    [TestMethod]
    public void LargeRingBuffer_EnqueueDequeue()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint capacity = 10u;
        uint stripeQuantity = 10u;
        LargeRingBuffer ringBuffer = new LargeRingBuffer(blockSize, capacity, stripeQuantity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }

    [TestMethod]
    public void LargeRingBuffer_T_EnqueueDequeue()
    {
        ulong value = 5UL;
        uint capacity = 10u;
        uint stripeQuantity = 10u;
        LargeRingBuffer<ulong> ringBuffer = new LargeRingBuffer<ulong>(capacity, stripeQuantity);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(value), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(out ulong result), "Dequeue should be successful.");
        Assert.AreEqual(value, result, "Dequeued value should be equal to the original value.");
    }

    [TestMethod]
    public void LargeRingBuffer_HalfFull()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        uint stripeQuantity = 10u;
        LargeRingBuffer ringBuffer = new LargeRingBuffer(blockSize, capacity, stripeQuantity);
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        for (int i = 0; i < capacity * stripeQuantity / 2; i++)
        {
            Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Enqueue should be successful.");
        }
        
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(capacity * stripeQuantity / 2 - 1, ringBuffer.Count, "Queue depth should be equal to half minus one.");
        
        
        for (int i = 0; i < capacity * stripeQuantity / 2 - 1; i++)
        {
            Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        }
        
        Assert.AreEqual(0UL, ringBuffer.Count, "Queue depth should be zero.");
    }
    
    [TestMethod]
    public void LargeRingBuffer_Full()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        uint stripeQuantity = 10u;
        LargeRingBuffer ringBuffer = new LargeRingBuffer(blockSize, capacity, stripeQuantity);
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        for (int i = 0; i < capacity * stripeQuantity; i++)
        {
            Thread.Sleep(1);
            Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Enqueue should be successful.");
        }
        
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        
        Assert.AreEqual(capacity * stripeQuantity - 1, ringBuffer.Count, "Queue depth should be equal full.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(capacity * stripeQuantity - 2, ringBuffer.Count, "Queue depth should be equal to full minus one.");
        
        
        for (int i = 0; i < capacity * stripeQuantity - 2; i++)
        {
            Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Remaining Dequeues should be successful.");
        }
        
        Assert.AreEqual(0UL, ringBuffer.Count, "Queue depth should be zero.");
    }
    
    [TestMethod]
    public void LargeRingBuffer_Overflow()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        uint stripeQuantity = 10u;
        LargeRingBuffer ringBuffer = new LargeRingBuffer(blockSize, capacity, stripeQuantity);
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        for (int i = 0; i < capacity * stripeQuantity; i++)
        {
            Thread.Sleep(1);
            Assert.IsTrue(ringBuffer.TryEnqueue(buffer), "Enqueue should be successful.");
        }
        
        for (int i = 0; i < capacity * stripeQuantity; i++)
        {
            Thread.Sleep(1);
            Assert.IsFalse(ringBuffer.TryEnqueue(buffer), "Overflow Enqueue should be unsuccessful.");
        }
        
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.AreEqual(capacity * stripeQuantity, ringBuffer.Count, "Queue depth should be equal full.");
        Assert.AreEqual(capacity * stripeQuantity, ringBuffer.DropCount, $"Drops should be {capacity}.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        Assert.AreEqual(capacity * stripeQuantity - 1, ringBuffer.Count, "Queue depth should be equal to full minus one.");
        
        
        for (int i = 0; i < capacity * stripeQuantity - 1; i++)
        {
            Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Remaining Dequeues should be successful.");
        }
        
        Assert.AreEqual(0UL, ringBuffer.Count, "Queue depth should be zero.");
    }

    #endregion //LargeRingBuffer

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
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 117u;
        ulong pageSize            = blockSize << 4;
        ulong capacity            = pageSize * 10u;
        
        string                                                file       = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_PageSizeAlignedWithRequiredFileSize)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin", null, pageSize);
        
        bool failed  = false;

        try
        {
            Random     ran                   = new Random();
            Thread.Sleep(ran.Next(0, 5000));
            Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
            BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
            for (ulong j = 0; j < capacity; j++)
            {
                ringBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
            }

            while (ringBuffer.TryDequeue(buffer))
            {
                Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
            }
                    
            for (ulong j = 0; j < capacity; j++)
            {
                ringBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
            }
                    
            while (ringBuffer.TryDequeue(buffer))
            {
                Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
            }
        }
        catch(Exception e)
        {
            failed = true;
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
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 117u;
        ulong pageSize            = blockSize << 4;
        ulong capacity            = pageSize * 10u + 1;
        
        string                                                file       = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_PageSizeNotAlignedWithRequiredFileSize)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin", null, pageSize);
        
        bool failed  = false;

        try
        {
            Random     ran                   = new Random();
            Thread.Sleep(ran.Next(0, 5000));
            Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
            BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
            for (ulong j = 0; j < capacity; j++)
            {
                ringBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
            }

            while (ringBuffer.TryDequeue(buffer))
            {
                Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
            }
                    
            for (ulong j = 0; j < capacity; j++)
            {
                ringBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
            }
                    
            while (ringBuffer.TryDequeue(buffer))
            {
                Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
            }
        }
        catch(Exception e)
        {
            failed = true;
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
    [Timeout(150000)]
    public void MemMapDelayDynamicBlockSingleProducerRingBuffer_MultipleThreads()
    {
        int   threadCount         = 32;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 117u;
        ulong pageSize            = blockSize * 16;
        ulong capacity            = pageSize * 100u + 1;
        
        string                                                file       = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_MultipleThreads)}.bin");
        using MemMapDelayDynamicBlockSingleProducerRingBuffer ringBuffer = new MemMapDelayDynamicBlockSingleProducerRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockSingleProducerRingBuffer_EnqueueDequeue)}.bin", null, pageSize);
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                Random     ran                   = new Random();
                Thread.Sleep(ran.Next(0, 100));
                var        threadLocalRingBuffer = (MemMapDelayDynamicBlockSingleProducerRingBuffer)o;
                Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                while (threadLocalRingBuffer.TryDequeue(buffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong j = 0; j < capacity; j++)
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

        ++i;
        
        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 100));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockSingleProducerRingBuffer)o;
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
        }

        //Cleanup
        StartStopThreads(threads, ringBuffer);

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

    #region MemMapDelayDynamicBlockUnsafeRingBuffer

    [TestMethod]
    public void MemMapDelayDynamicBlockUnsafeRingBuffer_EnqueueDequeue()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_EnqueueDequeue)}.bin");
        using MemMapDelayDynamicBlockUnsafeRingBuffer ringBuffer = new MemMapDelayDynamicBlockUnsafeRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_EnqueueDequeue)}.bin");
        
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
    public void MemMapDelayDynamicBlockUnsafeRingBuffer_HalfFull()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_HalfFull)}.bin");
        using MemMapDelayDynamicBlockUnsafeRingBuffer ringBuffer = new MemMapDelayDynamicBlockUnsafeRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_HalfFull)}.bin");
        
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
    public void MemMapDelayDynamicBlockUnsafeRingBuffer_Full()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_Full)}.bin");
        using MemMapDelayDynamicBlockUnsafeRingBuffer ringBuffer = new MemMapDelayDynamicBlockUnsafeRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_Full)}.bin");
        
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
    public void MemMapDelayDynamicBlockUnsafeRingBuffer_Overflow()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_Overflow)}.bin");
        using MemMapDelayDynamicBlockUnsafeRingBuffer ringBuffer = new MemMapDelayDynamicBlockUnsafeRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_Overflow)}.bin");
        
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
    public void MemMapDelayDynamicBlockUnsafeRingBuffer_Timing()
    {
        int delayInMilliseconds = 1000;
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        long slop = 10L;
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_Timing)}.bin");
        using MemMapDelayDynamicBlockUnsafeRingBuffer ringBuffer = new MemMapDelayDynamicBlockUnsafeRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_Timing)}.bin");
        
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
    public void MemMapDelayDynamicBlockUnsafeRingBuffer_PageSizeAlignedWithRequiredFileSize()
    {
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 117u;
        ulong pageSize            = blockSize << 4;
        ulong capacity            = pageSize * 10u;
        
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_PageSizeAlignedWithRequiredFileSize)}.bin");
        using MemMapDelayDynamicBlockUnsafeRingBuffer ringBuffer = new MemMapDelayDynamicBlockUnsafeRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_PageSizeAlignedWithRequiredFileSize)}.bin", null, pageSize);
        
        bool failed = false;

        try
        {
            Random ran = new Random();
            Thread.Sleep(ran.Next(0, 5000));
            Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
            BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
            for (ulong j = 0; j < capacity; j++)
            {
                ringBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
            }

            while (ringBuffer.TryDequeue(buffer))
            {
                Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
            }
                    
            for (ulong j = 0; j < capacity; j++)
            {
                ringBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
            }
                    
            while (ringBuffer.TryDequeue(buffer))
            {
                Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
            }
        }
        catch(Exception e)
        {
            failed = true;
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
    public void MemMapDelayDynamicBlockUnsafeRingBuffer_PageSizeNotAlignedWithRequiredFileSize()
    {
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 117u;
        ulong pageSize            = blockSize << 4;
        ulong capacity            = pageSize * 10u + 1;
        
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_PageSizeNotAlignedWithRequiredFileSize)}.bin");
        using MemMapDelayDynamicBlockUnsafeRingBuffer ringBuffer = new MemMapDelayDynamicBlockUnsafeRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_PageSizeNotAlignedWithRequiredFileSize)}.bin", null, pageSize);
        
        bool failed = false;

        try
        {
            Random ran = new Random();
            Thread.Sleep(ran.Next(0, 5000));
            Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
            BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
            for (ulong j = 0; j < capacity; j++)
            {
                ringBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
            }

            while (ringBuffer.TryDequeue(buffer))
            {
                Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
            }
                    
            for (ulong j = 0; j < capacity; j++)
            {
                ringBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
            }
                    
            while (ringBuffer.TryDequeue(buffer))
            {
                Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
            }
        }
        catch(Exception e)
        {
            failed = true;
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
    [Timeout(150000)]
    public void MemMapDelayDynamicBlockUnsafeRingBuffer_MultipleThreads()
    {
        int   threadCount         = 32;
        int   delayInMilliseconds = 1000;
        ulong value               = 5UL;
        uint  blockSize           = 117u;
        ulong pageSize            = blockSize * 16;
        ulong capacity            = pageSize * 100u + 1;
        
        string file = Path.Combine(System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_MultipleThreads)}.bin");
        using MemMapDelayDynamicBlockUnsafeRingBuffer ringBuffer = new MemMapDelayDynamicBlockUnsafeRingBuffer(Convert.ToUInt32(delayInMilliseconds), blockSize, capacity, System.IO.Path.GetTempPath(), $"{nameof(MemMapDelayDynamicBlockUnsafeRingBuffer_MultipleThreads)}.bin", null, pageSize);
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                Random     ran                   = new Random();
                Thread.Sleep(ran.Next(0, 100));
                var        threadLocalRingBuffer = (MemMapDelayDynamicBlockUnsafeRingBuffer)o;
                Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                while (threadLocalRingBuffer.TryDequeue(buffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong j = 0; j < capacity; j++)
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

        ++i;
        
        for (;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random     ran                   = new Random();
                    Thread.Sleep(ran.Next(0, 100));
                    var        threadLocalRingBuffer = (MemMapDelayDynamicBlockUnsafeRingBuffer)o;
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
        }

        //Cleanup
        StartStopThreads(threads, ringBuffer);

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

    #endregion //MemMapDelayDynamicBlockUnsafeRingBuffer

    #region NoLockDropOldestRingBuffer

    [TestMethod]
    public void NoLockDropOldestRingBuffer_EnqueueDequeue()
    {
        ulong                      value      = 5UL;
        uint                       blockSize  = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint                       capacity   = 10u;
        NoLockDropOldestRingBuffer ringBuffer = new NoLockDropOldestRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    public void NoLockDropOldestRingBuffer_T_EnqueueDequeue()
    {
        ulong                             value      = 5UL;
        uint                              blockSize  = sizeof(ulong);
        uint                              capacity   = 10u;
        NoLockDropOldestRingBuffer<ulong> ringBuffer = new NoLockDropOldestRingBuffer<ulong>(capacity);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(value),            "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(out ulong result), "Dequeue should be successful.");
        Assert.AreEqual(value, result, "Dequeued value should be equal to the original value.");
    }

    #endregion //NoLockDropOldestRingBuffer
    
    #region NoLockRingBuffer

    [TestMethod]
    public void NoLockRingBuffer_EnqueueDequeue()
    {
        ulong             value      = 5UL;
        uint              blockSize  = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint              capacity   = 10u;
        NoLockRingBuffer ringBuffer = new NoLockRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    public void NoLockRingBuffer_T_EnqueueDequeue()
    {
        ulong                    value      = 5UL;
        uint                     blockSize  = sizeof(ulong);
        uint                     capacity   = 10u;
        NoLockRingBuffer<ulong> ringBuffer = new NoLockRingBuffer<ulong>(capacity);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(value), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(out ulong result), "Dequeue should be successful.");
        Assert.AreEqual(value, result, "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    [Timeout(150000)]
    public void NoLockRingBuffer_MultipleThreads()
    {
        int   threadCount = 16;
        ulong value       = 5UL;
        uint  blockSize   = 117u;
        ulong capacity    = blockSize * 8192u * 16 + 1;
        
        NoLockRingBuffer ringBuffer = new NoLockRingBuffer(blockSize, capacity);
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                Random     ran                   = new Random();
                var        threadLocalRingBuffer = (NoLockRingBuffer)o;
                Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });

        ++i;
        
        threads[i] = new Thread(o =>
        {
            try
            {
                Random     ran                   = new Random();
                var        threadLocalRingBuffer = (NoLockRingBuffer)o;
                Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                while (threadLocalRingBuffer.TryDequeue(buffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong j = 0; j < capacity; j++)
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
        
        for (++i;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Thread.Sleep(1000);
                    Random     ran                   = new Random();
                    var        threadLocalRingBuffer = (NoLockRingBuffer)o;
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
        }

        //Cleanup
        StartStopThreads(threads, ringBuffer);

        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    [TestMethod]
    [Timeout(150000)]
    public void NoLockRingBuffer_T_MultipleThreads()
    {
        int   threadCount = 16;
        ulong value       = 5UL;
        ulong capacity    = 8192u * 16 + 1;
        
        NoLockRingBuffer<ulong> ringBuffer = new NoLockRingBuffer<ulong>(capacity);
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                var threadLocalRingBuffer = (NoLockRingBuffer<ulong>)o;
                
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer?.TryEnqueue(value); //we're going to over-enqueue a lot on purpose.
                }

                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer?.TryEnqueue(value); //we're going to over-enqueue a lot on purpose.
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });

        ++i;
        
        threads[i] = new Thread(o =>
        {
            try
            {
                var threadLocalRingBuffer = (NoLockRingBuffer<ulong>)o;
             
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(value); //we're going to over-enqueue a lot on purpose.
                }

                ulong val;
                while (threadLocalRingBuffer.TryDequeue(out val))
                {
                    Assert.AreEqual(value, val, "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(value); //we're going to over-enqueue a lot on purpose.
                }
                    
                while (threadLocalRingBuffer.TryDequeue(out val))
                {
                    Assert.AreEqual(value, val, "Dequeued value should be equal to the original value.");
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });
        
        for (++i;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Thread.Sleep(1000);
                    var threadLocalRingBuffer = (NoLockRingBuffer<ulong>)o;

                    ulong val;
                    while (threadLocalRingBuffer.TryDequeue(out val))
                    {
                        Assert.AreEqual(value, val, "Dequeued value should be equal to the original value.");
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(out val))
                    {
                        Assert.AreEqual(value, val, "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
        }

        //Cleanup
        StartStopThreads(threads, ringBuffer);

        if (failed)
            Assert.Fail("Thread failed.");
    }

    #endregion //NoLockRingBuffer
    
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
    
    #region PartitionedRoundRobinDynamicBlockRingBuffer

    [TestMethod]
    public void PartitionedRoundRobinDynamicBlockRingBuffer_EnqueueDequeue()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        PartitionedRoundRobinDynamicBlockRingBuffer ringBuffer = new PartitionedRoundRobinDynamicBlockRingBuffer(2U, blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(0, buffer), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryEnqueue(1, buffer), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }

    #endregion //PartitionedRoundRobinDynamicBlockRingBuffer

    #region PartitionedRoundRobinRingBuffer

    [TestMethod]
    public void PartitionedRoundRobinRingBuffer_EnqueueDequeue()
    {
        ulong value = 5UL;
        uint blockSize = sizeof(ulong);
        uint capacity = 10u;
        PartitionedRoundRobinRingBuffer ringBuffer = new PartitionedRoundRobinRingBuffer(2U, blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(0, buffer), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryEnqueue(1, buffer), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //reset
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }

    #endregion //PartitionedRoundRobinRingBuffer

    #region RingBuffer

    [TestMethod]
    public void RingBuffer_EnqueueDequeue()
    {
        ulong      value      = 5UL;
        uint       blockSize  = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint       capacity   = 10u;
        RingBuffer ringBuffer = new RingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    public void RingBuffer_T_EnqueueDequeue()
    {
        ulong             value      = 5UL;
        uint              blockSize  = sizeof(ulong);
        uint              capacity   = 10u;
        RingBuffer<ulong> ringBuffer = new RingBuffer<ulong>(capacity);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(value), "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(out ulong result), "Dequeue should be successful.");
        Assert.AreEqual(value, result, "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    public void RingBuffer_MultipleThreads()
    {
        int   threadCount = 16;
        ulong value       = 5UL;
        uint  blockSize   = 117u;
        ulong capacity    = blockSize * 8192u * 16 + 1;
        
        RingBuffer ringBuffer = new RingBuffer(blockSize, capacity);
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                Random ran = new Random();
                var threadLocalRingBuffer = (RingBuffer)o;
                Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });

        ++i;
        
        threads[i] = new Thread(o =>
        {
            try
            {
                Random     ran                   = new Random();
                var        threadLocalRingBuffer = (RingBuffer)o;
                Span<byte> buffer                = stackalloc byte[Convert.ToInt32(blockSize)];
                BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(buffer); //we're going to over-enqueue a lot on purpose.
                }

                while (threadLocalRingBuffer.TryDequeue(buffer))
                {
                    Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong j = 0; j < capacity; j++)
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
        
        for (++i;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    Random ran = new Random();
                    var threadLocalRingBuffer = (RingBuffer)o;
                    Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
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
        }

        //Cleanup
        StartStopThreads(threads, ringBuffer);

        if (failed)
            Assert.Fail("Thread failed.");
    }
    
    [TestMethod]
    [Timeout(150000)]
    public void RingBuffer_T_MultipleThreads()
    {
        int   threadCount = 16;
        ulong value       = 5UL;
        ulong capacity    = 8192u * 16 + 1;
        
        RingBuffer<ulong> ringBuffer = new RingBuffer<ulong>(capacity);
        
        Thread[] threads = new Thread[threadCount];
        bool     failed  = false;
        int i = 0;

        threads[i] = new Thread(o =>
        {
            try
            {
                var threadLocalRingBuffer = (RingBuffer<ulong>)o;
                
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer?.TryEnqueue(value); //we're going to over-enqueue a lot on purpose.
                }

                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer?.TryEnqueue(value); //we're going to over-enqueue a lot on purpose.
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });

        ++i;
        
        threads[i] = new Thread(o =>
        {
            try
            {
                var threadLocalRingBuffer = (RingBuffer<ulong>)o;
             
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(value); //we're going to over-enqueue a lot on purpose.
                }

                ulong val;
                while (threadLocalRingBuffer.TryDequeue(out val))
                {
                    Assert.AreEqual(value, val, "Dequeued value should be equal to the original value.");
                }
                    
                for (ulong j = 0; j < capacity; j++)
                {
                    threadLocalRingBuffer.TryEnqueue(value); //we're going to over-enqueue a lot on purpose.
                }
                    
                while (threadLocalRingBuffer.TryDequeue(out val))
                {
                    Assert.AreEqual(value, val, "Dequeued value should be equal to the original value.");
                }
            }
            catch(Exception e)
            {
                failed = true;
            }
        });
        
        for (++i;i < threads.Length; i++)
        {
            threads[i] = new Thread(o =>
            {
                try
                {
                    var threadLocalRingBuffer = (RingBuffer<ulong>)o;

                    ulong val;
                    while (threadLocalRingBuffer.TryDequeue(out val))
                    {
                        Assert.AreEqual(value, val, "Dequeued value should be equal to the original value.");
                    }
                    
                    while (threadLocalRingBuffer.TryDequeue(out val))
                    {
                        Assert.AreEqual(value, val, "Dequeued value should be equal to the original value.");
                    }
                }
                catch(Exception e)
                {
                    failed = true;
                }
            });
        }

        //Cleanup
        StartStopThreads(threads, ringBuffer);

        if (failed)
            Assert.Fail("Thread failed.");
    }

    #endregion //RingBuffer
    
    #region SingleProducerDropOldestRingBuffer

    [TestMethod]
    public void SingleProducerDropOldestRingBuffer_EnqueueDequeue()
    {
        ulong                    value      = 5UL;
        uint                     blockSize  = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint                     capacity   = 10u;
        SingleProducerDropOldestRingBuffer ringBuffer = new SingleProducerDropOldestRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    public void SingleProducerDropOldestRingBuffer_T_EnqueueDequeue()
    {
        ulong                           value      = 5UL;
        uint                            blockSize  = sizeof(ulong);
        uint                            capacity   = 10u;
        SingleProducerDropOldestRingBuffer<ulong> ringBuffer = new SingleProducerDropOldestRingBuffer<ulong>(capacity);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(value),            "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(out ulong result), "Dequeue should be successful.");
        Assert.AreEqual(value, result, "Dequeued value should be equal to the original value.");
    }

    #endregion //SingleProducerDropOldestRingBuffer
    
    #region SingleProducerRingBuffer

    [TestMethod]
    public void SingleProducerRingBuffer_EnqueueDequeue()
    {
        ulong                    value      = 5UL;
        uint                     blockSize  = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint                     capacity   = 10u;
        SingleProducerRingBuffer ringBuffer = new SingleProducerRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    public void SingleProducerRingBuffer_T_EnqueueDequeue()
    {
        ulong             value      = 5UL;
        uint              blockSize  = sizeof(ulong);
        uint              capacity   = 10u;
        SingleProducerRingBuffer<ulong> ringBuffer = new SingleProducerRingBuffer<ulong>(capacity);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(value),            "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(out ulong result), "Dequeue should be successful.");
        Assert.AreEqual(value, result, "Dequeued value should be equal to the original value.");
    }

    #endregion //SingleProducerRingBuffer
    
    #region UnsafeDropOldestRingBuffer

    [TestMethod]
    public void UnsafeDropOldestRingBuffer_EnqueueDequeue()
    {
        ulong                      value      = 5UL;
        uint                       blockSize  = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint                       capacity   = 10u;
        UnsafeDropOldestRingBuffer ringBuffer = new UnsafeDropOldestRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    public void UnsafeDropOldestRingBuffer_T_EnqueueDequeue()
    {
        ulong                             value      = 5UL;
        uint                              capacity   = 10u;
        UnsafeDropOldestRingBuffer<ulong> ringBuffer = new UnsafeDropOldestRingBuffer<ulong>(capacity);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(value),            "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(out ulong result), "Dequeue should be successful.");
        Assert.AreEqual(value, result, "Dequeued value should be equal to the original value.");
    }

    #endregion //UnsafeDropOldestRingBuffer
    
    #region UnsafeRingBuffer

    [TestMethod]
    public void UnsafeRingBuffer_EnqueueDequeue()
    {
        ulong            value      = 5UL;
        uint             blockSize  = sizeof(ulong) + 5; //intentionally make block bigger than we need so we can see it trim.
        uint             capacity   = 10u;
        UnsafeRingBuffer ringBuffer = new UnsafeRingBuffer(blockSize, capacity);
        
        Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(buffer.Slice(0, sizeof(ulong))), "Enqueue should be successful.");
        BinaryPrimitives.WriteUInt64BigEndian(buffer, 0UL); //clear buffer state
        Assert.IsTrue(ringBuffer.TryDequeue(buffer), "Dequeue should be successful.");
        Assert.AreEqual(value, BinaryPrimitives.ReadUInt64BigEndian(buffer), "Dequeued value should be equal to the original value.");
    }
    
    [TestMethod]
    public void UnsafeRingBuffer_T_EnqueueDequeue()
    {
        ulong                   value      = 5UL;
        uint                    blockSize  = sizeof(ulong);
        uint                    capacity   = 10u;
        UnsafeRingBuffer<ulong> ringBuffer = new UnsafeRingBuffer<ulong>(capacity);
        
        Assert.IsTrue(ringBuffer.TryEnqueue(value),            "Enqueue should be successful.");
        Assert.IsTrue(ringBuffer.TryDequeue(out ulong result), "Dequeue should be successful.");
        Assert.AreEqual(value, result, "Dequeued value should be equal to the original value.");
    }

    #endregion //UnsafeRingBuffer
    
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

    [TestMethod]
    public void NoLockPerfTest()
    {
        ulong casWait  = 0UL;
        ulong spinWait = 0UL;
        
        CancellationTokenSource cts = new CancellationTokenSource();
        cts.CancelAfter(120000);
        
        ulong            value            = 5UL;
        uint             blockSize        = sizeof(ulong);
        uint             capacity         = 1000u;
        NoLockRingBuffer noLockRingBuffer = new NoLockRingBuffer(blockSize, capacity);
        RingBuffer       spinRingBuffer   = new RingBuffer(blockSize, capacity);
        const int        LOOP_COUNT       = 5000; //Actual count is this squared * 2
        
        Parallel.For(0, LOOP_COUNT, new ParallelOptions(){CancellationToken = cts.Token}, i => 
        {
            Span<byte> buffer = stackalloc byte[Convert.ToInt32(blockSize)];
            Span<byte> outBuffer = stackalloc byte[Convert.ToInt32(blockSize)];
            BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
            Stopwatch sw = Stopwatch.StartNew();

            for (int j = 0; j < LOOP_COUNT; j++)
            {
                sw.Start();
                noLockRingBuffer.TryEnqueue(buffer);
                sw.Stop();
                Interlocked.Add(ref casWait,  Convert.ToUInt64(sw.ElapsedTicks));
                sw.Reset();

                sw.Start();
                noLockRingBuffer.TryDequeue(outBuffer);
                sw.Stop();
                Interlocked.Add(ref casWait,  Convert.ToUInt64(sw.ElapsedTicks));
                sw.Reset();
            }
        });
        
        Parallel.For(0, LOOP_COUNT, new ParallelOptions(){CancellationToken = cts.Token}, i => 
        {
            Span<byte> buffer    = stackalloc byte[Convert.ToInt32(blockSize)];
            Span<byte> outBuffer = stackalloc byte[Convert.ToInt32(blockSize)];
            BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
            Stopwatch sw = Stopwatch.StartNew();

            for (int j = 0; j < LOOP_COUNT; j++)
            {
                sw.Start();
                spinRingBuffer.TryEnqueue(buffer);
                sw.Stop();
                Interlocked.Add(ref spinWait, Convert.ToUInt64(sw.ElapsedTicks));
                sw.Reset();
            
                sw.Start();
                spinRingBuffer.TryDequeue(outBuffer);
                sw.Stop();
                Interlocked.Add(ref spinWait, Convert.ToUInt64(sw.ElapsedTicks));
            }
        });

        Assert.IsTrue(Interlocked.Read(ref spinWait) > Interlocked.Read(ref casWait));
    }
}
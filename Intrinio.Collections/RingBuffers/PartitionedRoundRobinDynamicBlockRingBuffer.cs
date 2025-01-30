namespace Intrinio.Collections.RingBuffers;

using System;
using System.Linq;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Buffers.Binary;

/// <summary>
/// A fixed size group of <see cref="Intrinio.Collections.RingBuffers.DynamicBlockSingleProducerRingBuffer"/> partitioned by an index, so that multiple writers may have their own write channel without being locked, while consumption is channel agnostic, thread-safe, and performed in a round-robin style.
/// </summary>
public class PartitionedRoundRobinDynamicBlockRingBuffer : IPartitionedDynamicBlockRingBuffer
{
    private readonly ulong _concurrency;
    private ulong _readIndex;
    private readonly DynamicBlockSingleProducerRingBuffer[] _queues;
    private readonly uint _blockSize;
    private readonly uint _eachBlockCapacity;
    private readonly ulong _totalBlockCapacity;
    
    /// <summary>
    /// The fixed size of each byte block.
    /// </summary>
    public uint BlockSize { get { return _blockSize; } }
    
    /// <summary>
    /// The fixed capacity of blocks in each ring buffer.
    /// </summary>
    public uint EachQueueBlockCapacity { get { return _eachBlockCapacity; } }
    
    /// <summary>
    /// The fixed total capacity of blocks across all ring buffers.
    /// </summary>
    public ulong TotalBlockCapacity { get { return _totalBlockCapacity; } }

    /// <summary>
    /// The count of blocks currently in the ring buffer.
    /// </summary>
    public ulong Count
    {
        get
        {
            ulong sum = 0UL;
            foreach (DynamicBlockSingleProducerRingBuffer queue in _queues)
                sum += queue.Count;
            return sum;
        }
    }
    
    public ulong ProcessedCount
    {
        get
        {
            ulong sum = 0UL;
            foreach (DynamicBlockSingleProducerRingBuffer queue in _queues)
                sum += queue.ProcessedCount;
            return sum;
        }
    }

    /// <summary>
    /// The quantity of dropped blocks due to being full.
    /// </summary>
    public ulong DropCount
    {
        get
        {
            ulong sum = 0UL;
            foreach (DynamicBlockSingleProducerRingBuffer queue in _queues)
                sum += queue.DropCount;
            return sum;
        }
    }
    
    /// <summary>
    /// Whether the ring buffer is currently empty.  Non-transactional.
    /// </summary>
    public bool IsEmpty
    {
        get
        {
            return _queues.All(q => q.IsEmpty);
        }
    }

    /// <summary>
    /// Whether the ring buffer is currently full.  Non-transactional.
    /// </summary>
    public bool IsFull
    {
        get
        {
            return _queues.All(q => q.IsFull);
        }
    }

    /// <summary>
    /// A fixed size group of <see cref="Intrinio.Collections.RingBuffers.DynamicBlockSingleProducerRingBuffer"/> partitioned by an index, so that multiple writers may have their own write channel without being locked, while consumption is channel agnostic, thread-safe, and performed in a round-robin style.  Provides support for dealing with blocks of varying size less than or equal to block size. .
    /// </summary>
    /// <param name="concurrency">The quantity of concurrent access required for writing. This many channels will be created.</param>
    /// <param name="blockSize">The fixed size of each byte block.</param>
    /// <param name="eachQueueBlockCapacity">The fixed capacity of block count in each channel.</param>
    /// <exception cref="ArgumentException">Throws an ArgumentException if concurrency is zero.</exception>
    public PartitionedRoundRobinDynamicBlockRingBuffer(uint concurrency, uint blockSize, uint eachQueueBlockCapacity)
    {
        if (concurrency == 0U)
            throw new ArgumentException("Argument concurrency must be greater than zero.", nameof(concurrency));
        
        _readIndex = 0UL;
        this._blockSize = blockSize;
        _eachBlockCapacity = eachQueueBlockCapacity;
        _totalBlockCapacity = Convert.ToUInt64(eachQueueBlockCapacity) * this._concurrency;
        this._concurrency = concurrency;
        _queues = new DynamicBlockSingleProducerRingBuffer[concurrency];
        for (int i = 0; i < concurrency; i++)
            _queues[i] = new DynamicBlockSingleProducerRingBuffer(blockSize, eachQueueBlockCapacity);
    }

    /// <summary>
    /// Not thread-safe (for a single threadIndex) try enqueue.  This is not safe for calling concurrently on the same threadIndex, and intended for use with a single producer per index.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="threadIndex">The zero based index for the channel to try enqueuing to. Max value is concurrency - 1.</param>
    /// <param name="blockToWrite">The byte block to copy from.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(uint threadIndex, ReadOnlySpan<byte> blockToWrite)
    {
        return _queues[threadIndex].TryEnqueue(blockToWrite);
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize or greater! Uses a round-robin method to try dequeuing from channels.
    /// </summary>
    /// <param name="blockBuffer">The buffer to copy the byte block to.</param>
    /// <returns>Whether a block was successfully dequeued or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(Span<byte> blockBuffer)
    {
        ulong tryCount = 0UL;
        while (tryCount < _concurrency && !_queues[Interlocked.Increment(ref _readIndex) % _concurrency].TryDequeue(blockBuffer))
            tryCount++;
        return tryCount != _concurrency;
    }

    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="trimmedBuffer">The fullBlockBuffer, trimmed down to the original size it enqueued as.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer)
    {
        ulong tryCount = 0UL;
        trimmedBuffer = fullBlockBuffer;
        while (tryCount < _concurrency && !_queues[Interlocked.Increment(ref _readIndex) % _concurrency].TryDequeue(fullBlockBuffer, out trimmedBuffer))
            tryCount++;
        return tryCount != _concurrency;
    }

    /// <summary>
    /// The count of blocks currently in the ring buffer at the specified index.
    /// </summary>
    /// <param name="threadIndex">The zero based index for the channel to try enqueuing to. Max value is concurrency - 1.</param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong GetCount(int threadIndex)
    {
        return _queues[threadIndex].Count;
    }
    
    /// <summary>
    /// The quantity of dropped blocks due to being full at the specified index.
    /// </summary>
    /// <param name="threadIndex">The zero based index for the channel to try enqueuing to. Max value is concurrency - 1.</param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong GetDropCount(int threadIndex)
    {
        return _queues[threadIndex].DropCount;
    }
}
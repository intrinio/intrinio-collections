namespace Intrinio.Collections.RingBuffers;

using System;
using System.Linq;
using System.Threading;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Intrinio.Collections.RingBuffers;
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
    private readonly uint _usableBlockSize;
    
    public uint UsableBlockSize { get { return _usableBlockSize; } }
    
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
    /// A fixed size group of <see cref="Intrinio.Collections.RingBuffers.DynamicBlockSingleProducerRingBuffer"/> partitioned by an index, so that multiple writers may have their own write channel without being locked, while consumption is channel agnostic, thread-safe, and performed in a round-robin style.  Provides support for dealing with blocks of varying size less than or equal to block size minus sizeof(UInt32). The first sizeof(UInt32) bytes of each block are reserved for tracking the used size of that block.
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
        _usableBlockSize = _blockSize - sizeof(UInt32);
        _queues = new DynamicBlockSingleProducerRingBuffer[concurrency];
        for (int i = 0; i < concurrency; i++)
            _queues[i] = new DynamicBlockSingleProducerRingBuffer(blockSize, eachQueueBlockCapacity);
    }

    /// <summary>
    /// Not thread-safe (for a single threadIndex) try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize! This is not safe for calling concurrently on the same threadIndex, and intended for use with a single producer per index.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="threadIndex">The zero based index for the channel to try enqueuing to. Max value is concurrency - 1.</param>
    /// <param name="blockToWrite">The full length byte block to copy from.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(uint threadIndex, Span<byte> blockToWrite)
    {
        return _queues[threadIndex].TryEnqueue(blockToWrite);
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize! Uses a round-robin method to try dequeuing from channels.
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
    /// Not thread-safe (for a single threadIndex) try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize! This is not safe for calling concurrently on the same threadIndex, and intended for use with a single producer per index.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="threadIndex">The zero based index for the channel to try enqueuing to. Max value is concurrency - 1.</param>
    /// <param name="fullBlockToWrite">The full length byte block to copy from. MUST be of length BlockSize!</param>
    /// <param name="usedLength">The length of used space in fullBlockToWrite, not including the section for used size tracking.  Use GetUsableArea to aid in this, and then use the length of that span after your further manipulation here.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(uint threadIndex, Span<byte> fullBlockToWrite, uint usedLength)
    {
        return _queues[threadIndex].TryEnqueue(fullBlockToWrite, usedLength);
    }

    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="usedSize">The used size of the full block after the used size tracking section.  This is the size set during enqueue.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(Span<byte> fullBlockBuffer, out uint usedSize)
    {
        bool result = TryDequeue(fullBlockBuffer);
        usedSize = BinaryPrimitives.ReadUInt32BigEndian(fullBlockBuffer);
        return result;
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
    
    /// <summary>
    /// Slice the full block to the writable area (removing the internally tracked used size section).
    /// </summary>
    /// <param name="fullBlockToTrim">The full sized block.</param>
    /// <returns>The fullBlockToTrim windowed without the internally tracked used size section.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Span<byte> GetUsableArea(Span<byte> fullBlockToTrim)
    {
        return DynamicBlockSingleProducerRingBuffer.GetUsableArea(fullBlockToTrim);
    }
}
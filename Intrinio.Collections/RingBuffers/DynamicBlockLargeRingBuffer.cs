namespace Intrinio.Collections.RingBuffers;

using System;
using System.Linq;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Buffers.Binary;

/// <summary>
/// A fixed size group of striped <see cref="Intrinio.Collections.RingBuffers.DynamicBlockSingleProducerRingBuffer"/>, so that the ring buffer may exceed the <see cref="System.Int32.MaxValue"/> in total size. Production and consumption is thread-safe but limited to one producer concurrently, and one consumer concurrently in order to maintain FIFO.  Provides support for dealing with blocks of varying size less than or equal to block size.
/// </summary>
public class DynamicBlockLargeRingBuffer : IDynamicBlockRingBuffer
{
    private readonly ulong _stripeCount;
    private ulong _readIndex;
    private ulong _writeIndex;
    private readonly DynamicBlockSingleProducerRingBuffer[] _queues;
    private readonly uint _blockSize;
    private readonly ulong _stripeBlockCapacity;
    private readonly ulong _totalBlockCapacity;
#if NET9_0_OR_GREATER
    private readonly Lock _readLock;
    private readonly Lock _writeLock;
#else
    private readonly object _readLock;
    private readonly object _writeLock;
#endif
    
    /// <summary>
    /// The fixed size of each byte block.
    /// </summary>
    public uint BlockSize { get { return _blockSize; } }
    
    /// <summary>
    /// The fixed total capacity.
    /// </summary>
    public ulong BlockCapacity { get { return _totalBlockCapacity; } }

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
    /// A fixed size group of striped <see cref="Intrinio.Collections.RingBuffers.DynamicBlockSingleProducerRingBuffer"/>, so that the ring buffer may exceed the <see cref="System.Int32.MaxValue"/> in total size. Production and consumption is thread-safe but limited to one producer concurrently, and one consumer concurrently in order to maintain FIFO.  Provides support for dealing with blocks of varying size less than or equal to block size.
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block.</param>
    /// <param name="stripeBlockCapacity">The fixed capacity of block count in each stripe.</param>
    /// <param name="stripeCount">The quantity of concurrent access required for writing. This many channels will be created.</param>
    /// <exception cref="ArgumentException">Throws an ArgumentException if concurrency is zero.</exception>
    public DynamicBlockLargeRingBuffer(uint blockSize, ulong stripeBlockCapacity, uint stripeCount)
    {
        if (blockSize == 0U)
            throw new ArgumentException($"Argument {nameof(blockSize)} must be greater than zero.", nameof(blockSize));
        
        if (stripeBlockCapacity == 0U)
            throw new ArgumentException($"Argument {nameof(stripeBlockCapacity)} must be greater than zero.", nameof(stripeBlockCapacity));
        
        if (stripeCount == 0U)
            throw new ArgumentException($"Argument {nameof(stripeCount)} must be greater than zero.", nameof(stripeCount));
        
        _readLock = new();
        _writeLock = new();
        _readIndex = 0UL;
        this._blockSize = blockSize;
        _stripeBlockCapacity = stripeBlockCapacity;
        this._stripeCount = stripeCount;
        _totalBlockCapacity = Convert.ToUInt64(stripeBlockCapacity) * this._stripeCount;
        _queues = new DynamicBlockSingleProducerRingBuffer[stripeCount];
        for (int i = 0; i < stripeCount; i++)
            _queues[i] = new DynamicBlockSingleProducerRingBuffer(blockSize, stripeBlockCapacity);
    }

    /// <summary>
    /// Not thread-safe (for a single threadIndex) try enqueue.  This is not safe for calling concurrently on the same threadIndex, and intended for use with a single producer per index.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="threadIndex">The zero based index for the channel to try enqueuing to. Max value is concurrency - 1.</param>
    /// <param name="blockToWrite">The byte block to copy from.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(ReadOnlySpan<byte> blockToWrite)
    {
        lock (_writeLock)
        {
            if (_queues[_writeIndex % _stripeCount].TryEnqueue(blockToWrite))
            {
                Interlocked.Increment(ref _writeIndex);
                return true;
            }
            
            return false;
        }
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize or greater! Uses a round-robin method to try dequeuing from channels.
    /// </summary>
    /// <param name="blockBuffer">The buffer to copy the byte block to.</param>
    /// <returns>Whether a block was successfully dequeued or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(Span<byte> blockBuffer)
    {
        lock (_readLock)
        {
            if (_queues[_readIndex % _stripeCount].TryDequeue(blockBuffer))
            {
                Interlocked.Increment(ref _readIndex);
                return true;
            }

            return false;
        }
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
        trimmedBuffer = fullBlockBuffer;
        lock (_readLock)
        {
            if (_queues[_readIndex % _stripeCount].TryDequeue(fullBlockBuffer, out trimmedBuffer))
            {
                Interlocked.Increment(ref _readIndex);
                return true;
            }

            return false;
        }
    }
}
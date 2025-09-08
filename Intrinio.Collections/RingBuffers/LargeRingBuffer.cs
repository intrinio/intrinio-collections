namespace Intrinio.Collections.RingBuffers;

using System;
using System.Linq;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Buffers.Binary;

/// <summary>
/// A fixed size group of striped <see cref="Intrinio.Collections.RingBuffers.UnsafeRingBuffer"/>, so that the ring buffer may exceed the <see cref="System.Int32.MaxValue"/> in total size. Production and consumption is thread-safe but limited to one producer concurrently, and one consumer concurrently in order to maintain FIFO.  Provides support for dealing with blocks of varying size less than or equal to block size.
/// </summary>
public class LargeRingBuffer : IRingBuffer
{
    private readonly ulong _stripeCount;
    private ulong _readIndex;
    private ulong _writeIndex;
    private readonly UnsafeRingBuffer[] _queues;
    private readonly uint _blockSize;
    private readonly ulong _stripeBlockCapacity;
    private readonly ulong _totalBlockCapacity;
    private SpinLock _readLock;
    private SpinLock _writeLock;
    
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
            foreach (UnsafeRingBuffer queue in _queues)
                sum += queue.Count;
            return sum;
        }
    }
    
    public ulong ProcessedCount
    {
        get
        {
            ulong sum = 0UL;
            foreach (UnsafeRingBuffer queue in _queues)
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
            foreach (UnsafeRingBuffer queue in _queues)
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
    /// A fixed size group of striped <see cref="Intrinio.Collections.RingBuffers.UnsafeRingBuffer"/>, so that the ring buffer may exceed the <see cref="System.Int32.MaxValue"/> in total size. Production and consumption is thread-safe but limited to one producer concurrently, and one consumer concurrently in order to maintain FIFO.  Provides support for dealing with blocks of varying size less than or equal to block size.
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block.</param>
    /// <param name="stripeBlockCapacity">The fixed capacity of block count in each stripe.</param>
    /// <param name="stripeCount">The quantity of concurrent access required for writing. This many channels will be created.</param>
    /// <exception cref="ArgumentException">Throws an ArgumentException if concurrency is zero.</exception>
    public LargeRingBuffer(uint blockSize, ulong stripeBlockCapacity, uint stripeCount)
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
        _queues = new UnsafeRingBuffer[stripeCount];
        for (int i = 0; i < stripeCount; i++)
            _queues[i] = new UnsafeRingBuffer(blockSize, stripeBlockCapacity);
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
        bool lockTaken = false;
        try
        {
            _writeLock.Enter(ref lockTaken);
            
            if (_queues[_writeIndex % _stripeCount].TryEnqueue(blockToWrite))
            {
                Interlocked.Increment(ref _writeIndex);
                return true;
            }
            
            return false;
        }
        finally
        {
            if (lockTaken) _writeLock.Exit();
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
        bool lockTaken = false;
        try
        {
            _readLock.Enter(ref lockTaken);
            
            if (_queues[_readIndex % _stripeCount].TryDequeue(blockBuffer))
            {
                Interlocked.Increment(ref _readIndex);
                return true;
            }

            return false;
        }
        finally
        {
            if (lockTaken) _readLock.Exit();
        }
    }
}

/// <summary>
/// A fixed size group of striped <see cref="Intrinio.Collections.RingBuffers.UnsafeRingBuffer"/>, so that the ring buffer may exceed the <see cref="System.Int32.MaxValue"/> in total size. Production and consumption is thread-safe but limited to one producer concurrently, and one consumer concurrently in order to maintain FIFO.
/// /// Full behavior: the <see cref="T"/> trying to be enqueued will be dropped. 
/// </summary>
public class LargeRingBuffer<T> : IRingBuffer<T> where T : struct
{
    private readonly ulong _stripeCount;
    private ulong _readIndex;
    private ulong _writeIndex;
    private readonly UnsafeRingBuffer<T>[] _queues;
    private readonly ulong _stripeCapacity;
    private readonly ulong _totalCapacity;
    private SpinLock _readLock;
    private SpinLock _writeLock;
    
    /// <summary>
    /// The fixed total capacity.
    /// </summary>
    public ulong Capacity { get { return _totalCapacity; } }

    /// <summary>
    /// The count of blocks currently in the ring buffer.
    /// </summary>
    public ulong Count
    {
        get
        {
            ulong sum = 0UL;
            foreach (UnsafeRingBuffer<T> queue in _queues)
                sum += queue.Count;
            return sum;
        }
    }
    
    public ulong ProcessedCount
    {
        get
        {
            ulong sum = 0UL;
            foreach (UnsafeRingBuffer<T> queue in _queues)
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
            foreach (UnsafeRingBuffer<T> queue in _queues)
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
    /// A fixed size group of striped <see cref="Intrinio.Collections.RingBuffers.UnsafeRingBuffer"/>, so that the ring buffer may exceed the <see cref="System.Int32.MaxValue"/> in total size. Production and consumption is thread-safe but limited to one producer concurrently, and one consumer concurrently in order to maintain FIFO.
    /// Full behavior: the <see cref="T"/> trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="stripeCapacity">The fixed capacity of block count in each stripe.</param>
    /// <param name="stripeCount">The quantity of concurrent access required for writing. This many channels will be created.</param>
    /// <exception cref="ArgumentException">Throws an ArgumentException if concurrency is zero.</exception>
    public LargeRingBuffer(ulong stripeCapacity, uint stripeCount)
    {
        if (stripeCapacity == 0U)
            throw new ArgumentException($"Argument {nameof(stripeCapacity)} must be greater than zero.", nameof(stripeCapacity));
        
        if (stripeCount == 0U)
            throw new ArgumentException($"Argument {nameof(stripeCount)} must be greater than zero.", nameof(stripeCount));
        
        _readLock = new();
        _writeLock = new();
        _readIndex = 0UL;
        _stripeCapacity = stripeCapacity;
        this._stripeCount = stripeCount;
        _totalCapacity = Convert.ToUInt64(stripeCapacity) * this._stripeCount;
        _queues = new UnsafeRingBuffer<T>[stripeCount];
        for (int i = 0; i < stripeCount; i++)
            _queues[i] = new UnsafeRingBuffer<T>(stripeCapacity);
    }

    /// <summary>
    /// Thread-safe but single entry enqueue.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="obj">The <see cref="T"/> to enqueue.</param>
    /// <returns>Whether the <see cref="T"/> was successfully enqueued or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(T obj)
    {
        bool lockTaken = false;
        try
        {
            _writeLock.Enter(ref lockTaken);
            
            if (_queues[_writeIndex % _stripeCount].TryEnqueue(obj))
            {
                Interlocked.Increment(ref _writeIndex);
                return true;
            }
            
            return false;
        }
        finally
        {
            if (lockTaken) _writeLock.Exit();
        }
    }

    /// <summary>
    /// Thread-safe but single entry dequeue.
    /// </summary>
    /// <param name="obj">The dequeued <see cref="T"/>.</param>
    /// <returns>Whether a <see cref="T"/> was successfully dequeued or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(out T obj)
    {
        bool lockTaken = false;
        try
        {
            _readLock.Enter(ref lockTaken);
            
            if (_queues[_readIndex % _stripeCount].TryDequeue(out obj))
            {
                Interlocked.Increment(ref _readIndex);
                return true;
            }

            return false;
        }
        finally
        {
            if (lockTaken) _readLock.Exit();
        }
    }
}
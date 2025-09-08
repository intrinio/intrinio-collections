namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Runtime.CompilerServices;

/// <summary>
/// A thread-safe implementation of the IRingBuffer (multiple producer and multiple consumer).  Full behavior: the oldest block in the ring buffer will be dropped. 
/// </summary>
public class SingleProducerDropOldestRingBuffer : IRingBuffer
{
    #region Data Members
    private readonly byte[] _data;
    private ulong _blockNextReadIndex;
    private ulong _blockNextWriteIndex;
    private SpinLock _readLock;
    private ulong _count;
    private readonly uint _blockSize;
    private readonly ulong _blockCapacity;
    private ulong _dropCount;
    
    private ulong _processed;
    public ulong ProcessedCount { get { return Interlocked.Read(ref _processed); } }
    
    public ulong Count { get { return Interlocked.Read(ref _count); } }
    public uint BlockSize { get { return _blockSize; } }
    public ulong BlockCapacity { get { return _blockCapacity; } }
    public ulong DropCount { get { return Interlocked.Read(ref _dropCount); } }

    public bool IsEmpty
    {
        get
        {
            return IsEmptyNoLock();
        }
    }

    public bool IsFull
    {
        get
        {
            return IsFullNoLock();
        }
    }
    #endregion //Data Members

    #region Constructors

    /// <summary>
    /// A thread-safe implementation of the IRingBuffer (multiple producer and multiple consumer).  Full behavior: the oldest block in the ring buffer will be dropped. 
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block.</param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    public SingleProducerDropOldestRingBuffer(uint blockSize, ulong blockCapacity)
    {
        this._blockSize = blockSize;
        this._blockCapacity = blockCapacity;
        _processed = 0UL;
        _blockNextReadIndex = 0u;
        _blockNextWriteIndex = 0u;
        _count = 0u;
        _dropCount = 0UL;
        _readLock = new();
        _data = new byte[blockSize * blockCapacity];
    }

    #endregion //Constructors

    /// <summary>
    /// Thread-safe try enqueue.
    /// Full behavior: the oldest block in the ring buffer will be dropped. 
    /// </summary>
    /// <param name="blockToWrite">The byte block to copy from.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    public bool TryEnqueue(ReadOnlySpan<byte> blockToWrite)
    {
        if (IsFullNoLock())
        {
            bool readLockTaken = false;
            try
            {
                _readLock.Enter(ref readLockTaken);
            
                if (IsFullNoLock())
                {
                    _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
                    Interlocked.Decrement(ref _count);
                    Interlocked.Increment(ref _dropCount);
                }
            }
            finally
            {
                if (readLockTaken) 
                    _readLock.Exit();
            }
        }
            
        Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextWriteIndex * BlockSize), Convert.ToInt32(BlockSize));
        blockToWrite.Slice(0, Math.Min(blockToWrite.Length, Convert.ToInt32(_blockSize))).CopyTo(target);
            
        _blockNextWriteIndex = (++_blockNextWriteIndex) % BlockCapacity;
        Interlocked.Increment(ref _count);
        return true;
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize!
    /// </summary>
    /// <param name="fullBlockBuffer">The buffer to copy the byte block to.</param>
    /// <returns>Whether a block was successfully dequeued or not.</returns>
    public bool TryDequeue(Span<byte> fullBlockBuffer)
    {
        bool lockTaken = false;
        try
        {
            _readLock.Enter(ref lockTaken);
            
            if (IsEmptyNoLock())
                return false;
            
            Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextReadIndex * BlockSize), Convert.ToInt32(BlockSize));
            target.CopyTo(fullBlockBuffer);
            
            _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
            Interlocked.Decrement(ref _count);
            Interlocked.Increment(ref _processed);
            return true;
        }
        finally
        {
            if (lockTaken) 
                _readLock.Exit();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsFullNoLock()
    {
        return Interlocked.Read(ref _count) == _blockCapacity;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsEmptyNoLock()
    {
        return Interlocked.Read(ref _count) == 0UL;
    }
}

/// <summary>
/// A thread-safe implementation of the IRingBuffer (multiple producer and multiple consumer).  Full behavior: the oldest <see cref="T"/> in the ring buffer will be dropped. 
/// </summary>
public class SingleProducerDropOldestRingBuffer<T> : IRingBuffer<T> where T : struct
{
    #region Data Members
    private readonly T[] _data;
    private readonly T DEFAULT = default(T);
    private ulong _nextReadIndex;
    private ulong _nextWriteIndex;
    private SpinLock _readLock;
    private ulong _count;
    private readonly ulong _capacity;
    private ulong _dropCount;
    
    private ulong _processed;
    public ulong ProcessedCount { get { return Interlocked.Read(ref _processed); } }
    
    public ulong Count { get { return Interlocked.Read(ref _count); } }
    public ulong Capacity { get { return _capacity; } }
    public ulong DropCount { get { return Interlocked.Read(ref _dropCount); } }

    public bool IsEmpty
    {
        get
        {
            return IsEmptyNoLock();
        }
    }

    public bool IsFull
    {
        get
        {
            return IsFullNoLock();
        }
    }
    #endregion //Data Members

    #region Constructors

    /// <summary>
    /// A thread-safe implementation of the IRingBuffer (multiple producer and multiple consumer).  Full behavior: the oldest block in the ring buffer will be dropped. 
    /// </summary>
    /// <param name="capacity">The fixed capacity of block count.</param>
    public SingleProducerDropOldestRingBuffer(ulong capacity)
    {
        this._capacity = capacity;
        _processed = 0UL;
        _nextReadIndex = 0u;
        _nextWriteIndex = 0u;
        _count = 0u;
        _dropCount = 0UL;
        _readLock = new();
        _data = new T[capacity];
    }

    #endregion //Constructors

    /// <summary>
    /// Thread-safe try enqueue.
    /// Full behavior: the oldest <see cref="T"/> in the ring buffer will be dropped. 
    /// </summary>
    /// <param name="obj">The <see cref="T"/> to enqueue.</param>
    /// <returns>Whether the enqueue was successful or not.</returns>
    public bool TryEnqueue(T obj)
    {
        if (IsFullNoLock())
        {
            bool readLockTaken = false;
            try
            {
                _readLock.Enter(ref readLockTaken);
            
                if (IsFullNoLock())
                {
                    _nextReadIndex = (++_nextReadIndex) % Capacity;
                    Interlocked.Decrement(ref _count);
                    Interlocked.Increment(ref _dropCount);
                }
            }
            finally
            {
                if (readLockTaken) 
                    _readLock.Exit();
            }
        }
            
        _data[_nextWriteIndex] = obj;
            
        _nextWriteIndex = (++_nextWriteIndex) % Capacity;
        Interlocked.Increment(ref _count);
        return true;
    }

    /// <summary>
    /// Thread-safe try dequeue.
    /// </summary>
    /// <returns>Whether the dequeue was successful or not.</returns>
    /// <returns>Whether a block was successfully dequeued or not.</returns>
    public bool TryDequeue(out T obj)
    {
        bool lockTaken = false;
        try
        {
            _readLock.Enter(ref lockTaken);
            
            if (IsEmptyNoLock())
            {
                obj = DEFAULT;
                return false;
            }
            
            obj = _data[_nextReadIndex];
            
            _nextReadIndex = (++_nextReadIndex) % Capacity;
            Interlocked.Decrement(ref _count);
            Interlocked.Increment(ref _processed);
            return true;
        }
        finally
        {
            if (lockTaken) 
                _readLock.Exit();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsFullNoLock()
    {
        return Interlocked.Read(ref _count) == _capacity;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsEmptyNoLock()
    {
        return Interlocked.Read(ref _count) == 0UL;
    }
}
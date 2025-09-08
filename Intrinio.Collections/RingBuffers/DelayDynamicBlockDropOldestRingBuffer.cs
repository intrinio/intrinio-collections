namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// A thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (multiple producer and multiple consumer), with support for tracking the used size of each byte-block.  Full behavior: the oldest block in the queue will be dropped. 
/// </summary>
public class DelayDynamicBlockDropOldestRingBuffer: IDynamicBlockRingBuffer
{
    #region Data Members
    private readonly byte[] _data;
    private readonly int[] _blockLengths;
    private readonly long[] _enqueueTimes;
    private readonly long _delayMilliseconds;
    private readonly System.Diagnostics.Stopwatch _stopwatch;
    private ulong _blockNextReadIndex;
    private ulong _blockNextWriteIndex;
    private SpinLock _readLock;
    private SpinLock _writeLock;
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
    /// A thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (multiple producer and multiple consumer), with support for tracking the used size of each byte-block.  Full behavior: the oldest block in the queue will be dropped.  Provides support for dealing with blocks of varying size less than or equal to block size. 
    /// </summary>
    /// <param name="delayMilliseconds">The number of milliseconds to delay blocks from being dequeued.</param>
    /// <param name="blockSize">The fixed size of each byte block.  /></param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    /// <param name="stopwatch">The stopwatch to use for comparison of elapsed milliseconds at time of dequeue to the elapsed milliseconds at the time a partition block was enqueued.</param>
    public DelayDynamicBlockDropOldestRingBuffer(uint delayMilliseconds, uint blockSize, ulong blockCapacity, System.Diagnostics.Stopwatch? stopwatch = default)
    {
        _blockSize = blockSize;
        _blockCapacity = blockCapacity;
        _delayMilliseconds = Convert.ToInt64(delayMilliseconds);
        _processed = 0UL;
        _stopwatch = stopwatch ?? System.Diagnostics.Stopwatch.StartNew();
        _blockLengths = new int[_blockCapacity];
        _enqueueTimes = new long[_blockCapacity];
        _blockNextReadIndex = 0u;
        _blockNextWriteIndex = 0u;
        _count = 0u;
        _dropCount = 0UL;
        _readLock = new();
        _writeLock = new();
        _data = new byte[blockSize * blockCapacity];
    }

    #endregion //Constructors
    
    /// <summary>
    /// Thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize!
    /// Full behavior: the oldest block in the ring buffer will be dropped. 
    /// </summary>
    /// <returns>Whether the enqueue was successful or not.</returns>
    /// <param name="blockToEnqueue">The byte block to copy from.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(ReadOnlySpan<byte> blockToEnqueue)
    {
        bool writeLockTaken = false;
        try
        {
            _writeLock.Enter(ref writeLockTaken);
            
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
                    if (readLockTaken) _readLock.Exit();
                }
            }
            
            int                length       = Math.Min(blockToEnqueue.Length, Convert.ToInt32(_blockSize));
            ReadOnlySpan<byte> trimmedBlock = blockToEnqueue.Slice(0, length);
            Span<byte>         target       = new Span<byte>(_data, Convert.ToInt32(_blockNextWriteIndex * BlockSize), Convert.ToInt32(BlockSize));
            trimmedBlock.CopyTo(target);
            _blockLengths[_blockNextReadIndex]  = length;
            _enqueueTimes[_blockNextWriteIndex] = _stopwatch.ElapsedMilliseconds;
            
            _blockNextWriteIndex = (++_blockNextWriteIndex) % BlockCapacity;
            Interlocked.Increment(ref _count);
            return true;
        }
        finally
        {
            if (writeLockTaken) 
                _writeLock.Exit();
        }
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <returns>Whether the dequeue was successful or not.</returns>
    /// <param name="fullBlockBuffer">The buffer to copy the byte block to.</param>
    public bool TryDequeue(Span<byte> fullBlockBuffer)
    {
        bool lockTaken = false;
        try
        {
            _readLock.Enter(ref lockTaken);
            
            if (IsEmptyNoLock() || (_delayMilliseconds > (_stopwatch.ElapsedMilliseconds - _enqueueTimes[_blockNextReadIndex])))
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
            if (lockTaken) _readLock.Exit();
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

    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="trimmedBuffer">The fullBlockBuffer, trimmed down to the original size it enqueued as.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer)
    {
        bool lockTaken = false;
        try
        {
            _readLock.Enter(ref lockTaken);
            
            if (IsEmptyNoLock() || (_delayMilliseconds > (_stopwatch.ElapsedMilliseconds - _enqueueTimes[_blockNextReadIndex])))
            {
                trimmedBuffer = fullBlockBuffer;
                return false;
            }
            
            Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextReadIndex * BlockSize), Convert.ToInt32(BlockSize));
            target.CopyTo(fullBlockBuffer);
            trimmedBuffer = fullBlockBuffer.Slice(0, _blockLengths[_blockNextReadIndex]);
            
            _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
            Interlocked.Decrement(ref _count);
            Interlocked.Increment(ref _processed);
            return true;
        }
        finally
        {
            if (lockTaken) _readLock.Exit();
        }
    }
}

/// <summary>
/// A thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (multiple producer and multiple consumer), with support for tracking the used size of each byte-block.  Full behavior: the oldest block in the queue will be dropped. 
/// </summary>
public class DelayDropOldestRingBuffer<T> : IRingBuffer<T> where T : struct
{
    #region Data Members
    private readonly T[] _data;
    private readonly T DEFAULT = default(T);
    private readonly long[] _enqueueTimes;
    private readonly long _delayMilliseconds;
    private readonly System.Diagnostics.Stopwatch _stopwatch;
    private ulong _nextReadIndex;
    private ulong _nextWriteIndex;
    private SpinLock _readLock;
    private SpinLock _writeLock;
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
    /// A thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (multiple producer and multiple consumer), with support for tracking the used size of each byte-block.  Full behavior: the oldest block in the queue will be dropped.  Provides support for dealing with blocks of varying size less than or equal to block size. 
    /// </summary>
    /// <param name="delayMilliseconds">The number of milliseconds to delay blocks from being dequeued.</param>
    /// <param name="capacity">The fixed capacity of block count.</param>
    /// <param name="stopwatch">The stopwatch to use for comparison of elapsed milliseconds at time of dequeue to the elapsed milliseconds at the time a partition block was enqueued.</param>
    public DelayDropOldestRingBuffer(uint delayMilliseconds, ulong capacity, System.Diagnostics.Stopwatch? stopwatch = default)
    {
        _capacity = capacity;
        _delayMilliseconds = Convert.ToInt64(delayMilliseconds);
        _processed = 0UL;
        _stopwatch = stopwatch ?? System.Diagnostics.Stopwatch.StartNew();
        _enqueueTimes = new long[_capacity];
        _nextReadIndex = 0u;
        _nextWriteIndex = 0u;
        _count = 0u;
        _dropCount = 0UL;
        _readLock = new();
        _writeLock = new();
        _data = new T[capacity];
    }

    #endregion //Constructors
    
    /// <summary>
    /// Thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize!
    /// Full behavior: the oldest block in the ring buffer will be dropped. 
    /// </summary>
    /// <returns>Whether the enqueue was successful or not.</returns>
    /// <param name="obj">The <see cref="T"/> to enqueue.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(T obj)
    {
        bool writeLockTaken = false;
        try
        {
            _writeLock.Enter(ref writeLockTaken);
            
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
                    if (readLockTaken) _readLock.Exit();
                }
            }
            
            _data[_nextWriteIndex] = obj;
            _enqueueTimes[_nextWriteIndex] = _stopwatch.ElapsedMilliseconds;
            
            _nextWriteIndex = (++_nextWriteIndex) % Capacity;
            Interlocked.Increment(ref _count);
            return true;
        }
        finally
        {
            if (writeLockTaken) 
                _writeLock.Exit();
        }
    }

    /// <summary>
    /// Thread-safe try dequeue.
    /// </summary>
    /// <returns>Whether the dequeue was successful or not.</returns>
    /// <param name="obj">The dequeued <see cref="T"/>.</param>
    public bool TryDequeue(out T obj)
    {
        bool lockTaken = false;
        try
        {
            _readLock.Enter(ref lockTaken);
            
            if (IsEmptyNoLock() || (_delayMilliseconds > (_stopwatch.ElapsedMilliseconds - _enqueueTimes[_nextReadIndex])))
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
            if (lockTaken) _readLock.Exit();
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
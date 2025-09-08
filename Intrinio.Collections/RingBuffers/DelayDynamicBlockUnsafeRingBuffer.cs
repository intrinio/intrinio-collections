namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// A thread-unsafe implementation of the <see cref="IDynamicBlockRingBuffer"/> (same producer and consumer), with support for tracking the used size of each byte-block.  Full behavior: the block trying to be enqueued will be dropped. 
/// </summary>
public class DelayDynamicBlockUnsafeRingBuffer: IDynamicBlockRingBuffer
{
    #region Data Members
    private readonly byte[] _data;
    private readonly int[] _blockLengths;
    private readonly long[] _enqueueTimes;
    private readonly long _delayMilliseconds;
    private readonly System.Diagnostics.Stopwatch _stopwatch;
    private ulong _blockNextReadIndex;
    private ulong _blockNextWriteIndex;
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
    /// A thread-unsafe implementation of the <see cref="IDynamicBlockRingBuffer"/> (same producer and consumer), with support for tracking the used size of each byte-block.  Full behavior: the block trying to be enqueued will be dropped.  Provides support for dealing with blocks of varying size less than or equal to block size. 
    /// </summary>
    /// <param name="delayMilliseconds">The number of milliseconds to delay blocks from being dequeued.</param>
    /// <param name="blockSize">The fixed size of each byte block.  /></param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    /// <param name="stopwatch">The stopwatch to use for comparison of elapsed milliseconds at time of dequeue to the elapsed milliseconds at the time a partition block was enqueued.</param>
    public DelayDynamicBlockUnsafeRingBuffer(uint delayMilliseconds, uint blockSize, ulong blockCapacity, System.Diagnostics.Stopwatch? stopwatch = default)
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
        _data = new byte[blockSize * blockCapacity];
    }

    #endregion //Constructors
    
    /// <summary>
    /// Not thread-safe try enqueue.  This is not safe for calling concurrently, and intended for use with a single producer. Detects and stores the length of the input block for use in dequeue.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="blockToEnqueue">The byte block to copy from.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(ReadOnlySpan<byte> blockToEnqueue)
    {
        if (IsFullNoLock())
        {
            Interlocked.Increment(ref _dropCount);
            return false;
        }

        int length = Math.Min(blockToEnqueue.Length, Convert.ToInt32(_blockSize));
        ReadOnlySpan<byte> trimmedBlock = blockToEnqueue.Slice(0, length);
        Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextWriteIndex * BlockSize), Convert.ToInt32(BlockSize));
        trimmedBlock.CopyTo(target);
        _blockLengths[_blockNextWriteIndex] = length;
        _enqueueTimes[_blockNextWriteIndex] = _stopwatch.ElapsedMilliseconds;
            
        _blockNextWriteIndex = (++_blockNextWriteIndex) % BlockCapacity;
        Interlocked.Increment(ref _count);

        return true;
    }

    /// <summary>
    /// Thread-unsafe try dequeue.  Parameter "fullBlockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <param name="fullBlockBuffer">The buffer to copy the byte block to.</param>
    public bool TryDequeue(Span<byte> fullBlockBuffer)
    {
        if (IsEmptyNoLock() || (_delayMilliseconds > (_stopwatch.ElapsedMilliseconds - _enqueueTimes[_blockNextReadIndex])))
            return false;
            
        Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextReadIndex * BlockSize), Convert.ToInt32(BlockSize));
        target.CopyTo(fullBlockBuffer);
            
        _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
        Interlocked.Decrement(ref _count);
        Interlocked.Increment(ref _processed);
        return true;
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
    /// Thread-unsafe try dequeue.  Parameter "fullBlockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="trimmedBuffer">The fullBlockBuffer, trimmed down to the original size it enqueued as.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    public bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer)
    {
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
}

/// <summary>
/// A thread-unsafe implementation of the <see cref="IDynamicBlockRingBuffer"/> (same producer and consumer), with support for tracking the used size of each byte-block.  Full behavior: the block trying to be enqueued will be dropped.  
/// </summary>
public class DelayUnsafeRingBuffer<T> : IRingBuffer<T> where T : struct
{
    #region Data Members
    private readonly T[] _data;
    private readonly T DEFAULT = default(T);
    private readonly long[] _enqueueTimes;
    private readonly long _delayMilliseconds;
    private readonly System.Diagnostics.Stopwatch _stopwatch;
    private ulong _nextReadIndex;
    private ulong _nextWriteIndex;
    private ulong _count;
    private readonly ulong _capacity;
    private ulong _dropCount;
    
    private ulong _processed;
    public ulong ProcessedCount { get { return _processed; } }
    
    public ulong Count { get { return _count; } }
    public ulong Capacity { get { return _capacity; } }
    public ulong DropCount { get { return _dropCount; } }

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
    /// A thread-unsafe implementation of the <see cref="IDynamicBlockRingBuffer"/> (same producer and consumer), with support for tracking the used size of each byte-block.  Full behavior: the block trying to be enqueued will be dropped.  Provides support for dealing with blocks of varying size less than or equal to block size. 
    /// </summary>
    /// <param name="delayMilliseconds">The number of milliseconds to delay blocks from being dequeued.</param>
    /// <param name="capacity">The fixed capacity of block count.</param>
    /// <param name="stopwatch">The stopwatch to use for comparison of elapsed milliseconds at time of dequeue to the elapsed milliseconds at the time a partition block was enqueued.</param>
    public DelayUnsafeRingBuffer(uint delayMilliseconds, ulong capacity, System.Diagnostics.Stopwatch? stopwatch = default)
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
        _data = new T[capacity];
    }

    #endregion //Constructors
    
    /// <summary>
    /// Thread-unsafe try enqueue.  
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <returns>Whether the enqueue was successful or not.</returns>
    /// <param name="obj">The <see cref="T"/> to enqueue.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(T obj)
    {
        if (IsFullNoLock())
        {
            ++_dropCount;
            return false;
        }
            
        _data[_nextWriteIndex] = obj;
        _enqueueTimes[_nextWriteIndex] = _stopwatch.ElapsedMilliseconds;
            
        _nextWriteIndex = (++_nextWriteIndex) % Capacity;
        ++_count;

        return true;
    }

    /// <summary>
    /// Thread-unsafe try dequeue.
    /// </summary>
    /// <returns>Whether the dequeue was successful or not.</returns>
    /// <param name="obj">The dequeued <see cref="T"/>.</param>
    public bool TryDequeue(out T obj)
    {
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
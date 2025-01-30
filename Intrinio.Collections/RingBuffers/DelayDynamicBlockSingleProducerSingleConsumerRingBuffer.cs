namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;

/// <summary>
/// A single producer, single consumer dynamic block ring buffer implementation of the <see cref="IDynamicBlockRingBuffer"/> where blocks are delayed from being dequeued for a specified time. Dequeues are blocked until a non-partition block is able to be dequeued. Enqueue partition blocks at regular intervals to gate-keep time partitions, as contiguous non-partition blocks between partition blocks will not be delayed.  Provides support for dealing with blocks of varying size less than or equal to block size.
/// Full behavior: the block trying to be enqueued will be dropped. 
/// </summary>
public sealed class DelayDynamicBlockSingleProducerSingleConsumerRingBuffer : IDelayDynamicBlockRingBuffer
{
    #region Data Members
    private const int PartitionPacketSize = sizeof(long);
    private readonly long _maximumDriftMilliseconds;
    private readonly long _delayMilliseconds;
    private readonly byte[] _blockTypes;
    private readonly AutoResetEvent _continueEvent = new(false);
    private ulong _processed;
    private readonly System.Diagnostics.Stopwatch _stopwatch;
    private readonly byte[] _partitionBuffer;
    private readonly byte[] _data;
    private readonly int[] _blockLengths;
    private uint _blockNextReadIndex;
    private uint _blockNextWriteIndex;
    private readonly object _readLock;
    private ulong _count;
    private readonly uint _blockSize;
    private readonly uint _blockCapacity;
    private ulong _dropCount;
    
    public ulong Count { get { return Interlocked.Read(ref _count); } }
    public uint BlockSize { get { return _blockSize; } }
    public uint BlockCapacity { get { return _blockCapacity; } }
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
    
    public ulong ProcessedCount { get { return _processed; } }
    
    #endregion //Data Members
    
    #region Constructors

    /// <summary>
    /// A single producer, single consumer dynamic block ring buffer implementation of the <see cref="IDynamicBlockRingBuffer"/> where blocks are delayed from being dequeued for a specified time. Dequeues are blocked until a non-partition block is able to be dequeued. Enqueue partition blocks at regular intervals to gate-keep time partitions, as contiguous non-partition blocks between partition blocks will not be delayed. Provides support for dealing with blocks of varying size less than or equal to block size.
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block. Internally, the first sizeof(UInt32) of each block is reserved for tracking the used size of each block. /></param>
    /// <param name="delayMilliseconds">The number of milliseconds to delay blocks from being dequeued.</param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    /// <param name="maximumDriftMilliseconds">The maximum drift in milliseconds before an artificial wait is forced.</param>
    /// <param name="stopwatch">The stopwatch to use for comparison of elapsed milliseconds at time of dequeue to the elapsed milliseconds at the time a partition block was enqueued.</param>
    public DelayDynamicBlockSingleProducerSingleConsumerRingBuffer(uint delayMilliseconds, uint blockCapacity, uint blockSize, uint maximumDriftMilliseconds = 100U, System.Diagnostics.Stopwatch? stopwatch = default)
    {
        if (blockSize < PartitionPacketSize)
            throw new ArgumentException($"Argument {nameof(blockSize)} Must be at least {PartitionPacketSize}.", nameof(blockSize));
        _processed = 0UL;
        _delayMilliseconds = Convert.ToInt64(delayMilliseconds);
        _maximumDriftMilliseconds = Convert.ToInt64(maximumDriftMilliseconds);
        _blockSize = blockSize;
        _stopwatch = stopwatch ?? System.Diagnostics.Stopwatch.StartNew();
        _blockSize = blockSize;
        _blockCapacity = blockCapacity;
        _blockLengths = new int[_blockCapacity];
        _blockNextReadIndex = 0u;
        _blockNextWriteIndex = 0u;
        _count = 0u;
        _dropCount = 0UL;
        _readLock = new object();
        _data = new byte[blockSize * blockCapacity];
        _blockTypes = new byte[_blockCapacity];
        _partitionBuffer = new byte[PartitionPacketSize];
        EnqueuePartitionPacket();
    }
    
    #endregion //Constructors

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
    /// Not thread-safe enqueue of a partition packet. This is not safe for calling concurrently, and intended for use with a single producer.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool EnqueuePartitionPacket()
    {
        Span<byte> buffer = _partitionBuffer;
        BinaryPrimitives.WriteInt64BigEndian(buffer, _stopwatch.ElapsedMilliseconds);
        return TryEnqueueImpl(buffer, 0);
    }
    
    /// <summary>
    /// Not thread-safe try enqueue.  This is not safe for calling concurrently, and intended for use with a single producer. Detects and stores the length of the input block for use in dequeue.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="blockToEnqueue">The byte block to copy from.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(ReadOnlySpan<byte> blockToEnqueue)
    {
        return TryEnqueueImpl(blockToEnqueue, 1);
    }

    /// <summary>
    /// Not thread-safe try enqueue.  This is not safe for calling concurrently, and intended for use with a single producer. Detects and stores the length of the input block for use in dequeue.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="blockToEnqueue">The byte block to copy from.</param>
    /// <param name="blockType">The type of block. 0 for partition block, 1 for normal block.</param>
    private bool TryEnqueueImpl(ReadOnlySpan<byte> blockToEnqueue, byte blockType)
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
        _blockTypes[_blockNextWriteIndex] = blockType;
            
        _blockNextWriteIndex = (++_blockNextWriteIndex) % BlockCapacity;
        Interlocked.Increment(ref _count);
        _continueEvent.Set();

        return true;
    }
    
    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <param name="fullBlockBuffer">The buffer to copy the byte block to.</param>
    public bool TryDequeue(Span<byte> fullBlockBuffer)
    {
        bool success = false;

        while (!success)
        {
            success = TryDequeueImpl(fullBlockBuffer, out byte blockType);
            if (!success)
                _continueEvent.WaitOne();
            else if (success && blockType == 0) //Encountered a partition message. 
            {
                success = false;
                long then = BinaryPrimitives.ReadInt64BigEndian(fullBlockBuffer);
                long elapsed = _stopwatch.ElapsedMilliseconds - then;
                if (_delayMilliseconds > elapsed)
                {
                    long diff = _delayMilliseconds - elapsed;
                    if (diff > _maximumDriftMilliseconds) 
                        Thread.Sleep(Convert.ToInt32(diff));
                }
            }
        }
        
        return success;
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "fullBlockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <param name="fullBlockBuffer">The buffer to copy the byte block to.</param>
    /// <param name="blockType">The type of block. 0 for partition block, 1 for normal block.</param>
    private bool TryDequeueImpl(Span<byte> fullBlockBuffer, out byte blockType)
    {
        blockType = 1;
        lock (_readLock)
        {
            if (IsEmptyNoLock())
                return false;
            
            Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextReadIndex * BlockSize), Convert.ToInt32(BlockSize));
            target.CopyTo(fullBlockBuffer);
            blockType = _blockTypes[_blockNextReadIndex];
            
            _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
            Interlocked.Decrement(ref _count);
            Interlocked.Increment(ref _processed);
            return true;
        }
    }
    
    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="trimmedBuffer">The fullBlockBuffer, trimmed down to the original size it enqueued as.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    public bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer)
    {
        bool success = false;
        trimmedBuffer = fullBlockBuffer;

        while (!success)
        {
            success = TryDequeueImpl(fullBlockBuffer, out trimmedBuffer, out byte blockType);
            if (!success)
                _continueEvent.WaitOne();
            else if (success && blockType == 0) //Encountered a partition message. 
            {
                success = false;
                long then = BinaryPrimitives.ReadInt64BigEndian(trimmedBuffer);
                long elapsed = _stopwatch.ElapsedMilliseconds - then;
                if (_delayMilliseconds > elapsed)
                {
                    long diff = _delayMilliseconds - elapsed;
                    if (diff > _maximumDriftMilliseconds) 
                        Thread.Sleep(Convert.ToInt32(diff));
                }
            }
        }
        
        return success;
    }
    
    /// <summary>
    /// Thread-safe try dequeue.  Parameter "fullBlockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="trimmedBuffer">The fullBlockBuffer, trimmed down to the original size it enqueued as.</param>
    /// /// <param name="blockType">The type of block. 0 for partition block, 1 for normal block.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    private bool TryDequeueImpl(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer, out byte blockType)
    {
        blockType = 1;
        lock (_readLock)
        {
            if (IsEmptyNoLock())
            {
                trimmedBuffer = fullBlockBuffer;
                return false;
            }
            
            Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextReadIndex * BlockSize), Convert.ToInt32(BlockSize));
            target.CopyTo(fullBlockBuffer);
            trimmedBuffer = fullBlockBuffer.Slice(0, _blockLengths[_blockNextReadIndex]);
            blockType = _blockTypes[_blockNextReadIndex];
            
            _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
            Interlocked.Decrement(ref _count);
            Interlocked.Increment(ref _processed);
            return true;
        }
    }
}
namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// A read thread-safe, write not thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (single producer and multiple consumer), with support for tracking the used size of each byte-block.  Full behavior: the block trying to be enqueued will be dropped. 
/// </summary>
public class DynamicBlockSingleProducerRingBuffer: IDynamicBlockRingBuffer
{
    #region Data Members
    private readonly byte[] _data;
    private readonly int[] _blockLengths;
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
    /// A read thread-safe, write not thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (single producer and multiple consumer).  Full behavior: the block trying to be enqueued will be dropped. Provides support for dealing with blocks of varying size less than or equal to block size. 
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block.  /></param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    public DynamicBlockSingleProducerRingBuffer(uint blockSize, ulong blockCapacity)
    {
        _blockSize = blockSize;
        _blockCapacity = blockCapacity;
        _processed = 0UL;
        _blockLengths = new int[_blockCapacity];
        _blockNextReadIndex = 0u;
        _blockNextWriteIndex = 0u;
        _count = 0u;
        _dropCount = 0UL;
        _readLock = new();
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
            
        _blockNextWriteIndex = (++_blockNextWriteIndex) % BlockCapacity;
        Interlocked.Increment(ref _count);

        return true;
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "fullBlockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <param name="fullBlockBuffer">The buffer to copy the byte block to.</param>
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
    /// Thread-safe try dequeue.  Parameter "fullBlockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="trimmedBuffer">The fullBlockBuffer, trimmed down to the original size it enqueued as.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    public bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer)
    {
        bool lockTaken = false;
        try
        {
            _readLock.Enter(ref lockTaken);
            
            if (IsEmptyNoLock())
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
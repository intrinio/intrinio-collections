namespace Intrinio.Collections.RingBuffers;

using System;
using System.Linq;
using System.Threading;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Intrinio.Collections.RingBuffers;

/// <summary>
/// A read thread-safe, write not thread-safe implementation of the IRingBuffer (single producer and multiple consumer).  Full behavior: the block trying to be enqueued will be dropped. 
/// </summary>
public class SingleProducerRingBuffer : IRingBuffer
{
    #region Data Members
    private readonly byte[] _data;
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
    #endregion //Data Members

    #region Constructors

    /// <summary>
    /// A read thread-safe, write not thread-safe implementation of the IRingBuffer (single producer and multiple consumer).  Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block.</param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    public SingleProducerRingBuffer(uint blockSize, uint blockCapacity)
    {
        this._blockSize = blockSize;
        this._blockCapacity = blockCapacity;
        _blockNextReadIndex = 0u;
        _blockNextWriteIndex = 0u;
        _count = 0u;
        _dropCount = 0UL;
        _readLock = new object();
        _data = new byte[blockSize * blockCapacity];
    }

    #endregion //Constructors

    /// <summary>
    /// Not thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize! This is not safe for calling concurrently, and intended for use with a single producer.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="blockToWrite">The byte block to copy from.</param>
    public bool TryEnqueue(ReadOnlySpan<byte> blockToWrite)
    {
        if (IsFullNoLock())
        {
            Interlocked.Increment(ref _dropCount);
            return false;
        }
            
        Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextWriteIndex * BlockSize), Convert.ToInt32(BlockSize));
        blockToWrite.CopyTo(target);
            
        _blockNextWriteIndex = (++_blockNextWriteIndex) % BlockCapacity;
        Interlocked.Increment(ref _count);

        return true;
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize!
    /// </summary>
    /// <param name="blockBuffer">The buffer to copy the byte block to.</param>
    public bool TryDequeue(Span<byte> blockBuffer)
    {
        lock (_readLock)
        {
            if (IsEmptyNoLock())
                return false;
            
            Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextReadIndex * BlockSize), Convert.ToInt32(BlockSize));
            target.CopyTo(blockBuffer);
            
            _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
            Interlocked.Decrement(ref _count);
            return true;
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
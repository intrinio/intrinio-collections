namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Runtime.CompilerServices;

/// <summary>
/// A thread-safe implementation of the IRingBuffer (multiple producer and multiple consumer).  Full behavior: the oldest block in the ring buffer will be dropped. 
/// </summary>
public class DropOldestRingBuffer : IRingBuffer
{
    #region Data Members
    private readonly byte[] _data;
    private uint _blockNextReadIndex;
    private uint _blockNextWriteIndex;
    private readonly object _readLock;
    private readonly object _writeLock;
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
    /// A thread-safe implementation of the IRingBuffer (multiple producer and multiple consumer).  Full behavior: the oldest block in the ring buffer will be dropped. 
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block.</param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    public DropOldestRingBuffer(uint blockSize, uint blockCapacity)
    {
        this._blockSize = blockSize;
        this._blockCapacity = blockCapacity;
        _blockNextReadIndex = 0u;
        _blockNextWriteIndex = 0u;
        _count = 0u;
        _dropCount = 0UL;
        _readLock = new object();
        _writeLock = new object();
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
        lock (_writeLock)
        {
            if (IsFullNoLock())
            {
                lock (_readLock)
                {
                    if (IsFullNoLock())
                    {
                        _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
                        Interlocked.Decrement(ref _count);
                        Interlocked.Increment(ref _dropCount);
                    }
                }
            }
            
            Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextWriteIndex * BlockSize), Convert.ToInt32(BlockSize));
            blockToWrite.Slice(0, Math.Min(blockToWrite.Length, Convert.ToInt32(_blockSize))).CopyTo(target);
            
            _blockNextWriteIndex = (++_blockNextWriteIndex) % BlockCapacity;
            Interlocked.Increment(ref _count);
            return true;
        }
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize!
    /// </summary>
    /// <param name="fullBlockBuffer">The buffer to copy the byte block to.</param>
    /// <returns>Whether a block was successfully dequeued or not.</returns>
    public bool TryDequeue(Span<byte> fullBlockBuffer)
    {
        lock (_readLock)
        {
            if (IsEmptyNoLock())
                return false;
            
            Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextReadIndex * BlockSize), Convert.ToInt32(BlockSize));
            target.CopyTo(fullBlockBuffer);
            
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
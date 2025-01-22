namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// A read thread-safe, write not thread-safe implementation of the IDynamicBlockRingBuffer (single producer and multiple consumer), with support for tracking the used size of each byte-block.  Full behavior: the block trying to be enqueued will be dropped. 
/// </summary>
public class DynamicBlockRingBuffer: IDynamicBlockRingBuffer
{
    #region Data Members
    private readonly byte[] _data;
    private uint _blockNextReadIndex;
    private uint _blockNextWriteIndex;
    private readonly object _writeLock;
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

    private readonly uint _usableBlockSize;

    /// <summary>
    /// The fixed usable size of each byte block.
    /// </summary>
    public uint UsableBlockSize
    {
        get
        {
            return _usableBlockSize;
        }
    }
    #endregion //Data Members
    
    #region Constructors

    /// <summary>
    /// A read thread-safe, write not thread-safe implementation of the IDynamicBlockRingBuffer (single producer and multiple consumer).  Full behavior: the block trying to be enqueued will be dropped. Provides support for dealing with blocks of varying size less than or equal to block size minus sizeof(UInt32). The first sizeof(UInt32) bytes of each block are reserved for tracking the used size of that block. 
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block. Internally, the first sizeof(UInt32) of each block is reserved for tracking the used size of each block. /></param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    public DynamicBlockRingBuffer(uint blockSize, uint blockCapacity)
    {
        _blockSize = blockSize;
        _blockCapacity = blockCapacity;
        _usableBlockSize = _blockSize - sizeof(UInt32);
        _blockNextReadIndex = 0u;
        _blockNextWriteIndex = 0u;
        _count = 0u;
        _dropCount = 0UL;
        _writeLock = new object();
        _readLock = new object();
        _data = new byte[blockSize * blockCapacity];
    }

    #endregion //Constructors
    
    /// <summary>
    /// Thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize!
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="blockToWrite">The byte block to copy from.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(Span<byte> blockToWrite)
    {
        return TryEnqueue(blockToWrite, _usableBlockSize);
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

    /// <summary>
    /// Thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize!
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="fullBlockToWrite">The byte block to copy from. MUST be of length BlockSize!</param>
    /// <param name="usedLength">The length of used space in fullBlockToWrite, not including the section for used size tracking.  Use GetUsableArea to aid in this, and then use the length of that span after your further manipulation here.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(Span<byte> fullBlockToWrite, uint usedLength)
    {
        BinaryPrimitives.WriteUInt32BigEndian(fullBlockToWrite, usedLength);
        
        if (IsFullNoLock())
        {
            Interlocked.Increment(ref _dropCount);
            return false;
        }

        lock (_writeLock)
        {
            if (IsFullNoLock())
            {
                Interlocked.Increment(ref _dropCount);
                return false;
            }
            
            Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextWriteIndex * BlockSize), Convert.ToInt32(BlockSize));
            fullBlockToWrite.CopyTo(target);
            
            _blockNextWriteIndex = (++_blockNextWriteIndex) % BlockCapacity;
            Interlocked.Increment(ref _count);

            return true;
        }
    }

    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to. Must be of BlockSize.</param>
    /// <param name="usedSize">The used size of the full block.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(Span<byte> fullBlockBuffer, out uint usedSize)
    {
        bool result = TryDequeue(fullBlockBuffer);
        usedSize = BinaryPrimitives.ReadUInt32BigEndian(fullBlockBuffer);
        return result;
    }
    
    /// <summary>
    /// Slice the full block to the usable area (removing the internally tracked used size section).
    /// </summary>
    /// <param name="fullBlockToTrim">The full sized block.</param>
    /// <returns>The fullBlockToTrim windowed without the internally tracked used size section.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Span<byte> GetUsableArea(Span<byte> fullBlockToTrim)
    {
        return fullBlockToTrim.Slice(sizeof(UInt32));
    }
}
namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Buffers;
using System.Runtime.CompilerServices;

/// <summary>
/// A thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (multiple producer and multiple consumer), with support for tracking the used size of each byte-block.  Full behavior: the oldest block in the ring buffer will be dropped. 
/// </summary>
public class DynamicBlockNoLockDropOldestRingBuffer: IDynamicBlockRingBuffer
{
    #region Data Members
    private readonly byte[][] _blocks;
    private readonly int[] _blockLengths;
    private ulong _blockNextReadIndex;
    private ulong _blockNextWriteIndex;
    private ulong _dropCount;
    private ulong _processed;
    private readonly uint _blockSize;
    private readonly ulong _blockCapacity;
    private readonly ArrayPool<byte> _pool;

    private readonly ThreadLocal<ulong> _producerCachedRead = new ThreadLocal<ulong>(() => 0UL);
    private readonly ThreadLocal<ulong> _consumerCachedWrite = new ThreadLocal<ulong>(() => 0UL);

    public ulong ProcessedCount { get { return Interlocked.Read(ref _processed); } }
    
    public ulong Count { get { return Volatile.Read(ref _blockNextWriteIndex) - Volatile.Read(ref _blockNextReadIndex); } }
    public uint BlockSize { get { return _blockSize; } }
    public ulong BlockCapacity { get { return _blockCapacity; } }
    public ulong DropCount { get { return Interlocked.Read(ref _dropCount); } }

    public bool IsEmpty
    {
        get
        {
            return Count == 0UL;
        }
    }

    public bool IsFull
    {
        get
        {
            return Count == _blockCapacity;
        }
    }
    #endregion //Data Members
    
    #region Constructors

    /// <summary>
    /// A thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (multiple producer and multiple consumer).  Full behavior: the oldest block in the ring buffer will be dropped. Provides support for dealing with blocks of varying size less than or equal to block size. 
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block.  /></param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    public DynamicBlockNoLockDropOldestRingBuffer(uint blockSize, ulong blockCapacity)
    {
        _blockSize = blockSize;
        _blockCapacity = blockCapacity;
        _processed = 0UL;
        _blockNextReadIndex = 0u;
        _blockNextWriteIndex = 0u;
        _dropCount = 0UL;
        _blocks = new byte[blockCapacity][];
        _blockLengths = new int[blockCapacity];
        _pool = ArrayPool<byte>.Create((int)blockSize * 2, (int)(blockCapacity + (blockCapacity / 2)));
    }

    #endregion //Constructors
    
    /// <summary>
    /// Thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize!
    /// Full behavior: the oldest block in the ring buffer will be dropped. 
    /// </summary>
    /// <param name="blockToEnqueue">The byte block to copy from.</param>
    public bool TryEnqueue(ReadOnlySpan<byte> blockToEnqueue)
    {
        int length = Math.Min(blockToEnqueue.Length, Convert.ToInt32(_blockSize));
        ReadOnlySpan<byte> trimmedBlock = blockToEnqueue.Slice(0, Convert.ToInt32(length));
        byte[] rented = _pool.Rent((int)_blockSize);
        trimmedBlock.CopyTo(rented);
        
        while (true)
        {
            ulong currentWrite = Volatile.Read(ref _blockNextWriteIndex);
            ulong cachedRead = _producerCachedRead.Value;
            
            if (currentWrite - cachedRead >= _blockCapacity)
            {
                ulong actualRead = Volatile.Read(ref _blockNextReadIndex);
                if (currentWrite - actualRead >= _blockCapacity)
                {
                    ulong currRead = actualRead;
                    ulong nxtRead = currRead + 1UL;
                    if (Interlocked.CompareExchange(ref _blockNextReadIndex, nxtRead, currRead) == currRead)
                    {
                        ulong slot = (currRead % _blockCapacity);
                        byte[] block;
                        do
                        {
                            block = Volatile.Read(ref _blocks[slot]);
                            if (block == null) Thread.Yield();
                        }
                        while (block == null);
                        Thread.MemoryBarrier();
                        Volatile.Write(ref _blocks[slot], null);
                        _pool.Return(block, false);
                        Interlocked.Increment(ref _dropCount);
                    }
                    _producerCachedRead.Value = Volatile.Read(ref _blockNextReadIndex);
                    continue;
                }
                _producerCachedRead.Value = actualRead;
            }
            
            ulong nextWrite = currentWrite + 1UL;
            
            if (Interlocked.CompareExchange(ref _blockNextWriteIndex, nextWrite, currentWrite) == currentWrite)
            {
                ulong slot = currentWrite % _blockCapacity;
                _blockLengths[slot] = length;
                Thread.MemoryBarrier();
                Volatile.Write(ref _blocks[slot], rented);
                return true;
            }
        }
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <param name="fullBlockBuffer">The buffer to copy the byte block to.</param>
    public bool TryDequeue(Span<byte> fullBlockBuffer)
    {
        while (true)
        {
            ulong currentRead = Volatile.Read(ref _blockNextReadIndex);
            ulong cachedWrite = _consumerCachedWrite.Value;
            
            if (currentRead >= cachedWrite)
            {
                ulong actualWrite = Volatile.Read(ref _blockNextWriteIndex);
                if (currentRead >= actualWrite)
                    return false;
                _consumerCachedWrite.Value = actualWrite;
            }
            
            ulong nextRead = currentRead + 1UL;
            
            if (Interlocked.CompareExchange(ref _blockNextReadIndex, nextRead, currentRead) == currentRead)
            {
                ulong slot = currentRead % _blockCapacity;
                byte[] block;
                
                do
                {
                    block = Volatile.Read(ref _blocks[slot]);
                    if (block == null) Thread.Yield();
                }
                while (block == null);
                
                Thread.MemoryBarrier();
                new Span<byte>(block, 0, (int)_blockSize).CopyTo(fullBlockBuffer);
                
                Volatile.Write(ref _blocks[slot], null);
                _pool.Return(block, false);
                Interlocked.Increment(ref _processed);
                return true;
            }
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
        while (true)
        {
            ulong currentRead = Volatile.Read(ref _blockNextReadIndex);
            ulong cachedWrite = _consumerCachedWrite.Value;
            
            if (currentRead >= cachedWrite)
            {
                ulong actualWrite = Volatile.Read(ref _blockNextWriteIndex);
                if (currentRead >= actualWrite)
                    return false;
                _consumerCachedWrite.Value = actualWrite;
            }
            
            ulong nextRead = currentRead + 1UL;
            
            if (Interlocked.CompareExchange(ref _blockNextReadIndex, nextRead, currentRead) == currentRead)
            {
                ulong slot = currentRead % _blockCapacity;
                byte[] block;
                
                do
                {
                    block = Volatile.Read(ref _blocks[slot]);
                    if (block == null) Thread.Yield();
                }
                while (block == null);
                
                Thread.MemoryBarrier();
                int length = _blockLengths[slot];
                new Span<byte>(block, 0, (int)_blockSize).CopyTo(fullBlockBuffer);
                trimmedBuffer = fullBlockBuffer.Slice(0, Convert.ToInt32(length));
                
                Volatile.Write(ref _blocks[slot], null);
                _pool.Return(block, false);
                Interlocked.Increment(ref _processed);
                return true;
            }
        }
    }
}
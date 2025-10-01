namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Buffers;
using System.Numerics;

/// <summary>
/// A thread-safe implementation of the IRingBuffer (multiple producer and multiple consumer).  Full behavior: the block trying to be enqueued will be dropped. 
/// </summary>
public class NoLockRingBuffer : IRingBuffer
{
    #region Data Members
    private readonly byte[][] _blocks;
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
    /// A thread-safe implementation of the IRingBuffer (multiple producer and multiple consumer).  Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block.</param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    public NoLockRingBuffer(uint blockSize, ulong blockCapacity)
    {
        this._blockSize = blockSize;
        this._blockCapacity = blockCapacity;
        _processed = 0UL;
        _blockNextReadIndex = 0u;
        _blockNextWriteIndex = 0u;
        _dropCount = 0UL;
        _blocks = new byte[(int)blockCapacity][];

        // Custom single pool with maxArraysPerBucket set to 1.5x capacity (1/2 over)
        _pool = ArrayPool<byte>.Create((int)BitOperations.RoundUpToPowerOf2(blockSize), (int)(blockCapacity + (blockCapacity / 2)));
    }

    #endregion //Constructors

    /// <summary>
    /// Thread-safe try enqueue.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="blockToWrite">The byte block to copy from.</param>
    public bool TryEnqueue(ReadOnlySpan<byte> blockToWrite)
    {
        byte[] rented = _pool.Rent((int)_blockSize);
        blockToWrite.Slice(0, Math.Min(blockToWrite.Length, (int)_blockSize)).CopyTo(rented);
        
        while (true)
        {
            ulong currentWrite = Volatile.Read(ref _blockNextWriteIndex);
            ulong cachedRead = _producerCachedRead.Value;
            
            if (currentWrite - cachedRead >= _blockCapacity)
            {
                ulong actualRead = Volatile.Read(ref _blockNextReadIndex);
                if (currentWrite - actualRead >= _blockCapacity)
                {
                    Interlocked.Increment(ref _dropCount);
                    _pool.Return(rented, false);
                    return false;
                }
                _producerCachedRead.Value = actualRead;
            }
            
            ulong nextWrite = currentWrite + 1UL;
            
            if (Interlocked.CompareExchange(ref _blockNextWriteIndex, nextWrite, currentWrite) == currentWrite)
            {
                ulong slot = currentWrite % _blockCapacity;
                Thread.MemoryBarrier();
                Volatile.Write(ref _blocks[slot], rented);
                return true;
            }
        }
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize!
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
}

/// <summary>
/// A thread-safe implementation of the IRingBuffer (multiple producer and multiple consumer).  Full behavior: the <see cref="T"/> trying to be enqueued will be dropped.
/// </summary>
public class NoLockRingBuffer<T> : IRingBuffer<T> where T : struct
{
    #region Data Members
    private readonly T[] _data;
    private readonly byte[] _flags;
    private ulong _nextReadIndex;
    private ulong _nextWriteIndex;
    private ulong _dropCount;
    private ulong _processed;
    private readonly ulong _capacity;

    private readonly ThreadLocal<ulong> _producerCachedRead = new ThreadLocal<ulong>(() => 0UL);
    private readonly ThreadLocal<ulong> _consumerCachedWrite = new ThreadLocal<ulong>(() => 0UL);

    public ulong ProcessedCount { get { return Interlocked.Read(ref _processed); } }
    
    public ulong Count { get { return Volatile.Read(ref _nextWriteIndex) - Volatile.Read(ref _nextReadIndex); } }
    public ulong Capacity { get { return _capacity; } }
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
            return Count == _capacity;
        }
    }
    #endregion //Data Members

    #region Constructors

    /// <summary>
    /// A thread-safe implementation of a circular buffer (multiple producer and multiple consumer).  Full behavior: the <see cref="T"/> trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="capacity">The fixed capacity.</param>
    public NoLockRingBuffer(ulong capacity)
    {
        this._capacity = capacity;
        _processed = 0UL;
        _nextReadIndex = 0u;
        _nextWriteIndex = 0u;
        _dropCount = 0UL;
        _data = new T[capacity];
        _flags = new byte[capacity];
    }

    #endregion //Constructors

    /// <summary>
    /// Thread-safe try enqueue.
    /// Full behavior: the <see cref="T"/> trying to be enqueued will be dropped. 
    /// </summary>
    /// <returns>Whether the enqueue was successful or not.</returns>
    /// <param name="obj">The <see cref="T"/> to enqueue.</param>
    public bool TryEnqueue(T obj)
    {
        while (true)
        {
            ulong currentWrite = Volatile.Read(ref _nextWriteIndex);
            ulong cachedRead = _producerCachedRead.Value;
            if (currentWrite - cachedRead >= _capacity)
            {
                ulong actualRead = Volatile.Read(ref _nextReadIndex);
                if (currentWrite - actualRead >= _capacity)
                {
                    Interlocked.Increment(ref _dropCount);
                    return false;
                }
                _producerCachedRead.Value = actualRead;
            }
            
            ulong nextWrite = currentWrite + 1UL;
            
            if (Interlocked.CompareExchange(ref _nextWriteIndex, nextWrite, currentWrite) == currentWrite)
            {
                ulong slot = currentWrite % _capacity;
                _data[slot] = obj;
                Thread.MemoryBarrier();
                Volatile.Write(ref _flags[slot], (byte)1);
                return true;
            }
        }
    }

    /// <summary>
    /// Thread-safe try dequeue.
    /// </summary>
    /// <returns>Whether the dequeue was successful or not.</returns>
    /// <param name="obj">The dequeued <see cref="T"/>.</param>
    public bool TryDequeue(out T obj)
    {
        obj = default(T);
        while (true)
        {
            ulong currentRead = Volatile.Read(ref _nextReadIndex);
            ulong cachedWrite = _consumerCachedWrite.Value;
            if (currentRead >= cachedWrite)
            {
                ulong actualWrite = Volatile.Read(ref _nextWriteIndex);
                if (currentRead >= actualWrite)
                {
                    return false;
                }
                _consumerCachedWrite.Value = actualWrite;
            }
            
            ulong nextRead = currentRead + 1UL;
            
            if (Interlocked.CompareExchange(ref _nextReadIndex, nextRead, currentRead) == currentRead)
            {
                ulong slot = currentRead % _capacity;
                
                while (Volatile.Read(ref _flags[slot]) == 0) Thread.Yield();
                
                obj = _data[slot];
                Thread.MemoryBarrier();
                Volatile.Write(ref _flags[slot], (byte)0);
                Interlocked.Increment(ref _processed);
                return true;
            }
        }
    }
}
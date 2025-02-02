using System.IO;
using System.Numerics;

namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.IO.MemoryMappedFiles;

/// <summary>
/// A read thread-safe, write not thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (single producer and multiple consumer), with support for tracking the used size of each byte-block.  Full behavior: the block trying to be enqueued will be dropped. 
/// </summary>
public class MemMapDelayDynamicBlockSingleProducerRingBuffer: IDynamicBlockRingBuffer, IDisposable
{
    #region Data Members
    private const ulong DefaultTargetPageSize = 4096UL;
    private MemoryMappedFile _data;
    private readonly byte[] _writeBuffer;
    private MemoryMappedViewAccessor _dataWriteAccessor;
    private ulong _dataWritePageIndex;
    private MemoryMappedViewAccessor _dataReadAccessor;
    private ulong _dataReadPageIndex;
    private readonly ulong _dataPageSize;
    
    private MemoryMappedFile _blockLengthsData;
    private MemoryMappedViewAccessor _blockLengthsWriteAccessor;
    private ulong _blockLengthsWritePageIndex;
    private MemoryMappedViewAccessor _blockLengthsReadAccessor;
    private ulong _blockLengthsReadPageIndex;
    private readonly ulong _blockLengthsPageSize;
    
    private MemoryMappedFile _enqueueTimesData;
    private MemoryMappedViewAccessor _enqueueTimesWriteAccessor;
    private ulong _enqueueTimesWritePageIndex;
    private MemoryMappedViewAccessor _enqueueTimesReadAccessor;
    private ulong _enqueueTimesReadPageIndex;
    private readonly ulong _enqueueTimesPageSize;
    
    private readonly long _delayMilliseconds;
    private readonly System.Diagnostics.Stopwatch _stopwatch;
    private ulong _blockNextReadIndex;
    private ulong _blockNextWriteIndex;
    private readonly object _readLock;
    private ulong _count;
    private readonly uint _blockSize;
    private readonly ulong _blockCapacity;
    private ulong _dropCount;
    private readonly string dataFilePath;
    private readonly string blockLengthsFilePath;
    private readonly string enqueueTimesFilePath;
    
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
    /// <param name="delayMilliseconds">The number of milliseconds to delay blocks from being dequeued.</param>
    /// <param name="blockSize">The fixed size of each byte block.  /></param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    /// <param name="fileDirectory">The directory in which the memory mapped files will reside.</param>
    /// <param name="fileNamePrefix">The prefix for the names of the files.</param>
    /// <param name="stopwatch">The stopwatch to use for comparison of elapsed milliseconds at time of dequeue to the elapsed milliseconds at the time a partition block was enqueued.</param>
    /// <param name="targetPageSize">The preferred page size for memory swapping. This size, or something closely under it will be used.</param>
    public MemMapDelayDynamicBlockSingleProducerRingBuffer(uint delayMilliseconds, uint blockSize, ulong blockCapacity, string fileDirectory, string fileNamePrefix, System.Diagnostics.Stopwatch? stopwatch = default, ulong targetPageSize = DefaultTargetPageSize)
    {
        //8_388_608UL
        _blockSize = blockSize;
        _blockCapacity = blockCapacity;
        _delayMilliseconds = Convert.ToInt64(delayMilliseconds);
        _processed = 0UL;
        _stopwatch = stopwatch ?? System.Diagnostics.Stopwatch.StartNew();
        _writeBuffer = new byte[_blockCapacity];
        _blockNextReadIndex = 0u;
        _blockNextWriteIndex = 0u;
        _dataWritePageIndex = 0UL;
        _dataReadPageIndex = 0UL;
        _blockLengthsWritePageIndex = 0UL;
        _blockLengthsReadPageIndex = 0UL;
        _enqueueTimesWritePageIndex = 0UL;
        _enqueueTimesReadPageIndex = 0UL;
        if (blockSize > targetPageSize)
            throw new ArgumentException($"Argument blockSize must be less than {nameof(targetPageSize)}", nameof(blockSize));
        _dataPageSize = (targetPageSize / Convert.ToUInt64(blockSize)) * Convert.ToUInt64(blockSize);
        _blockLengthsPageSize = (targetPageSize / Convert.ToUInt64(sizeof(int))) * Convert.ToUInt64(sizeof(int));
        _enqueueTimesPageSize = (targetPageSize / Convert.ToUInt64(sizeof(long))) * Convert.ToUInt64(sizeof(long));
        _count = 0u;
        _dropCount = 0UL;
        _readLock = new object();
        
        dataFilePath = System.IO.Path.Combine(fileDirectory, $"{fileNamePrefix}_Data.bin");
        blockLengthsFilePath = System.IO.Path.Combine(fileDirectory, $"{fileNamePrefix}_Lengths.bin");
        enqueueTimesFilePath = System.IO.Path.Combine(fileDirectory, $"{fileNamePrefix}_EnqueueTimes.bin");
        
        if (File.Exists(dataFilePath))
            File.Delete(dataFilePath);
        using (var fs = new FileStream(dataFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(Convert.ToInt64(blockCapacity) * Convert.ToInt64(blockSize));
        }
        _data = MemoryMappedFile.CreateFromFile(dataFilePath);
        _dataWriteAccessor = _data.CreateViewAccessor(0, Convert.ToInt64(_dataPageSize), MemoryMappedFileAccess.ReadWriteExecute);
        _dataReadAccessor = _data.CreateViewAccessor(0, Convert.ToInt64(_dataPageSize), MemoryMappedFileAccess.Read);
        
        if (File.Exists(blockLengthsFilePath))
            File.Delete(blockLengthsFilePath);
        using (var fs = new FileStream(blockLengthsFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(Convert.ToInt64(blockCapacity) * Convert.ToInt64(sizeof(int)));
        }
        _blockLengthsData = MemoryMappedFile.CreateFromFile(blockLengthsFilePath);
        _blockLengthsWriteAccessor = _blockLengthsData.CreateViewAccessor(0, Convert.ToInt64(_blockLengthsPageSize), MemoryMappedFileAccess.ReadWriteExecute);
        _blockLengthsReadAccessor = _blockLengthsData.CreateViewAccessor(0, Convert.ToInt64(_blockLengthsPageSize), MemoryMappedFileAccess.Read);
        
        if (File.Exists(enqueueTimesFilePath))
            File.Delete(enqueueTimesFilePath);
        using (var fs = new FileStream(enqueueTimesFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(Convert.ToInt64(blockCapacity) * Convert.ToInt64(sizeof(long)));
        }
        _enqueueTimesData = MemoryMappedFile.CreateFromFile(enqueueTimesFilePath);
        _enqueueTimesWriteAccessor = _enqueueTimesData.CreateViewAccessor(0, Convert.ToInt64(_enqueueTimesPageSize), MemoryMappedFileAccess.ReadWriteExecute);
        _enqueueTimesReadAccessor = _enqueueTimesData.CreateViewAccessor(0, Convert.ToInt64(_enqueueTimesPageSize), MemoryMappedFileAccess.Read);
    }
    
    #endregion //Constructors
    
    public void Dispose()
    {
        _dataWriteAccessor.Flush();
        _dataWriteAccessor.Dispose();
        _dataReadAccessor.Dispose();
        _data.Dispose();
        
        _blockLengthsWriteAccessor.Flush();
        _blockLengthsWriteAccessor.Dispose();
        _blockLengthsReadAccessor.Dispose();
        _blockLengthsData.Dispose();
    
        _enqueueTimesWriteAccessor.Flush();
        _enqueueTimesWriteAccessor.Dispose();
        _enqueueTimesReadAccessor.Dispose();
        _enqueueTimesData.Dispose();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong GetPageIndex(ulong blockIndex, ulong blockSize, ulong pageSize)
    {
        return (blockIndex * blockSize) / pageSize;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong GetPageStartingIndex(ulong pageIndex, ulong pageSize)
    {
        return pageIndex * pageSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong GetIntraPageOffset(ulong blockIndex, ulong blockSize, ulong pageSize)
    {
        return (blockIndex * blockSize) % pageSize;
    }

    private MemoryMappedViewAccessor GetDataWriteAccessor()
    {
        ulong nextDesiredPageIndex = GetPageIndex(_blockNextWriteIndex, _blockSize, _dataPageSize);
        if (nextDesiredPageIndex != _dataWritePageIndex)
        {
            //_dataWriteAccessor.Flush(); //we already flush immediately when we write.
            _dataWriteAccessor.Dispose();
            _dataWriteAccessor = _data.CreateViewAccessor(Convert.ToInt64(GetPageStartingIndex(nextDesiredPageIndex, _dataPageSize)), Convert.ToInt64(_dataPageSize), MemoryMappedFileAccess.ReadWriteExecute);
            _dataWritePageIndex = nextDesiredPageIndex;
        }
        
        return _dataWriteAccessor;
    }
    
    private MemoryMappedViewAccessor GetBlockLengthsWriteAccessor()
    {
        ulong nextDesiredPageIndex = GetPageIndex(_blockNextWriteIndex, sizeof(int), _blockLengthsPageSize);
        if (nextDesiredPageIndex != _blockLengthsWritePageIndex)
        {
            //_blockLengthsWriteAccessor.Flush(); //we already flush immediately when we write.
            _blockLengthsWriteAccessor.Dispose();
            _blockLengthsWriteAccessor = _blockLengthsData.CreateViewAccessor(Convert.ToInt64(GetPageStartingIndex(nextDesiredPageIndex, _blockLengthsPageSize)), Convert.ToInt64(_blockLengthsPageSize), MemoryMappedFileAccess.ReadWriteExecute);
            _blockLengthsWritePageIndex = nextDesiredPageIndex;
        }
        
        return _blockLengthsWriteAccessor;
    }
    
    private MemoryMappedViewAccessor GetEnqueueTimesWriteAccessor()
    {
        ulong nextDesiredPageIndex = GetPageIndex(_blockNextWriteIndex, sizeof(long), _enqueueTimesPageSize);
        if (nextDesiredPageIndex != _enqueueTimesWritePageIndex)
        {
            //_enqueueTimesWriteAccessor.Flush(); //we already flush immediately when we write.
            _enqueueTimesWriteAccessor.Dispose();
            _enqueueTimesWriteAccessor = _enqueueTimesData.CreateViewAccessor(Convert.ToInt64(GetPageStartingIndex(nextDesiredPageIndex, _enqueueTimesPageSize)), Convert.ToInt64(_enqueueTimesPageSize), MemoryMappedFileAccess.ReadWriteExecute);
            _enqueueTimesWritePageIndex = nextDesiredPageIndex;
        }
        
        return _enqueueTimesWriteAccessor;
    }
    
    private MemoryMappedViewAccessor GetDataReadAccessor()
    {
        ulong nextDesiredPageIndex = GetPageIndex(_blockNextReadIndex, _blockSize, _dataPageSize);
        if (nextDesiredPageIndex != _dataReadPageIndex)
        {
            _dataReadAccessor.Dispose();
            _dataReadAccessor = _data.CreateViewAccessor(Convert.ToInt64(GetPageStartingIndex(nextDesiredPageIndex, _dataPageSize)), Convert.ToInt64(_dataPageSize), MemoryMappedFileAccess.Read);
            _dataReadPageIndex = nextDesiredPageIndex;
        }
        
        return _dataReadAccessor;
    }
    
    private MemoryMappedViewAccessor GetBlockLengthsReadAccessor()
    {
        ulong nextDesiredPageIndex = GetPageIndex(_blockNextReadIndex, sizeof(int), _blockLengthsPageSize);
        if (nextDesiredPageIndex != _blockLengthsReadPageIndex)
        {
            _blockLengthsReadAccessor.Flush();
            _blockLengthsReadAccessor.Dispose();
            _blockLengthsReadAccessor = _blockLengthsData.CreateViewAccessor(Convert.ToInt64(GetPageStartingIndex(nextDesiredPageIndex, _blockLengthsPageSize)), Convert.ToInt64(_blockLengthsPageSize), MemoryMappedFileAccess.Read);
            _blockLengthsReadPageIndex = nextDesiredPageIndex;
        }
        
        return _blockLengthsReadAccessor;
    }
    
    private MemoryMappedViewAccessor GetEnqueueTimesReadAccessor()
    {
        ulong nextDesiredPageIndex = GetPageIndex(_blockNextReadIndex, sizeof(long), _enqueueTimesPageSize);
        if (nextDesiredPageIndex != _enqueueTimesReadPageIndex)
        {
            _enqueueTimesReadAccessor.Flush();
            _enqueueTimesReadAccessor.Dispose();
            _enqueueTimesReadAccessor = _enqueueTimesData.CreateViewAccessor(Convert.ToInt64(GetPageStartingIndex(nextDesiredPageIndex, _enqueueTimesPageSize)), Convert.ToInt64(_enqueueTimesPageSize), MemoryMappedFileAccess.Read);
            _enqueueTimesReadPageIndex = nextDesiredPageIndex;
        }
        
        return _enqueueTimesReadAccessor;
    }
    
    /// <summary>
    /// Not thread-safe try enqueue.  This is not safe for calling concurrently, and intended for use with a single producer. Detects and stores the length of the input block for use in dequeue.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="blockToEnqueue">The byte block to copy from.</param>
    public bool TryEnqueue(ReadOnlySpan<byte> blockToEnqueue)
    {
        if (IsFullNoLock())
        {
            Interlocked.Increment(ref _dropCount);
            return false;
        }

        int length = Math.Min(blockToEnqueue.Length, Convert.ToInt32(_blockSize));
        ReadOnlySpan<byte> trimmedBlock = blockToEnqueue.Slice(0, length);
        trimmedBlock.CopyTo(_writeBuffer);
        MemoryMappedViewAccessor dataWriteAccessor = GetDataWriteAccessor();
        dataWriteAccessor.WriteArray(Convert.ToInt64(GetIntraPageOffset(_blockNextWriteIndex, BlockSize, _dataPageSize)), _writeBuffer, 0, _writeBuffer.Length);
        dataWriteAccessor.Flush();

        MemoryMappedViewAccessor blockLengthsWriteAccessor = GetBlockLengthsWriteAccessor();
        blockLengthsWriteAccessor.Write(Convert.ToInt64(GetIntraPageOffset(_blockNextWriteIndex, sizeof(int), _blockLengthsPageSize)), length);
        blockLengthsWriteAccessor.Flush();
        
        MemoryMappedViewAccessor enqueueTimesWriteAccessor = GetEnqueueTimesWriteAccessor();
        enqueueTimesWriteAccessor.Write(Convert.ToInt64(GetIntraPageOffset(_blockNextWriteIndex, sizeof(long), _enqueueTimesPageSize)), _stopwatch.ElapsedMilliseconds);
        enqueueTimesWriteAccessor.Flush();
            
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
        // lock (_readLock)
        // {
        //     if (IsEmptyNoLock())
        //         return false;
        //     
        //     long enqueueTime = GetEnqueueTimesReadAccessor().ReadInt64(Convert.ToInt64(GetIntraPageOffset(_blockNextReadIndex, sizeof(long), _enqueueTimesPageSize)));
        //     if (_delayMilliseconds > (_stopwatch.ElapsedMilliseconds - enqueueTime))
        //         return false;
        //     
        //     Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextReadIndex * BlockSize), Convert.ToInt32(BlockSize));
        //     target.CopyTo(fullBlockBuffer);
        //     
        //     _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
        //     Interlocked.Decrement(ref _count);
        //     Interlocked.Increment(ref _processed);
        //     return true;
        // }
        return false;
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
        // lock (_readLock)
        // {
        //     if (IsEmptyNoLock() || (_delayMilliseconds > (_stopwatch.ElapsedMilliseconds - _enqueueTimes[_blockNextReadIndex])))
        //     {
        //         trimmedBuffer = fullBlockBuffer;
        //         return false;
        //     }
        //     
        //     Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextReadIndex * BlockSize), Convert.ToInt32(BlockSize));
        //     target.CopyTo(fullBlockBuffer);
        //     trimmedBuffer = fullBlockBuffer.Slice(0, _blockLengths[_blockNextReadIndex]);
        //     
        //     _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
        //     Interlocked.Decrement(ref _count);
        //     Interlocked.Increment(ref _processed);
        //     return true;
        // }
        trimmedBuffer = fullBlockBuffer;
        return false;
    }
}
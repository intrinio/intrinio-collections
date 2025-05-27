namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Runtime.CompilerServices;
using System.IO.MemoryMappedFiles;
using System.IO;

/// <summary>
/// A read thread-safe, write not thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (single producer and multiple consumer), with support for tracking the used size of each byte-block.  Full behavior: the block trying to be enqueued will be dropped. 
/// </summary>
public class MemMapDelayDynamicBlockDropOldestRingBuffer: IDynamicBlockRingBuffer, IDisposable
{
    #region Data Members
    private const ulong DefaultTargetPageSize = 8_388_608UL;
    private MemoryMappedFile _data;
    private readonly byte[] _writeBuffer;
    private readonly byte[] _readBuffer;
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
    private readonly object _writeLock;
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
    /// <param name="delayMilliseconds">The number of milliseconds to delay blocks from being dequeued.</param>
    /// <param name="blockSize">The fixed size of each byte block.  /></param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    /// <param name="stopwatch">The stopwatch to use for comparison of elapsed milliseconds at time of dequeue to the elapsed milliseconds at the time a partition block was enqueued.</param>
    /// <param name="targetPageSize">The target page size to use for the memory-mapped files.  This will be increased to the nearest multiple of blockSize.</param>
    public MemMapDelayDynamicBlockDropOldestRingBuffer(uint delayMilliseconds, uint blockSize, ulong blockCapacity, string fileDirectory, string fileNamePrefix, System.Diagnostics.Stopwatch? stopwatch = default, ulong targetPageSize = DefaultTargetPageSize)
    {
        if (blockSize == 0u)
            throw new ArgumentException($"Argument blockSize must not be zero.", nameof(blockSize));
        
        if (blockCapacity == 0UL)
            throw new ArgumentException($"Argument blockCapacity must not be zero.", nameof(blockSize));
        
        if (targetPageSize == 0UL)
            throw new ArgumentException($"Argument targetPageSize must not be zero.", nameof(blockSize));
        
        if (blockSize > targetPageSize)
            throw new ArgumentException($"Argument blockSize must be less than {nameof(targetPageSize)}", nameof(blockSize));
        
        _blockSize = blockSize;
        _blockCapacity = blockCapacity;
        _delayMilliseconds = Convert.ToInt64(delayMilliseconds);
        _processed = 0UL;
        _stopwatch = stopwatch ?? System.Diagnostics.Stopwatch.StartNew();
        _writeBuffer = new byte[blockSize];
        _readBuffer = new byte[blockSize];
        _blockNextReadIndex = 0UL;
        _blockNextWriteIndex = 0UL;
        _dataWritePageIndex = 0UL;
        _dataReadPageIndex = 0UL;
        _blockLengthsWritePageIndex = 0UL;
        _blockLengthsReadPageIndex = 0UL;
        _enqueueTimesWritePageIndex = 0UL;
        _enqueueTimesReadPageIndex = 0UL;
        
        ulong dataPageSizeBeforeModulo = Math.Min(targetPageSize, Convert.ToUInt64(blockSize) * _blockCapacity);
        _dataPageSize = dataPageSizeBeforeModulo % Convert.ToUInt64(blockSize) == 0UL 
            ? dataPageSizeBeforeModulo 
            : dataPageSizeBeforeModulo + Convert.ToUInt64(blockSize) - (dataPageSizeBeforeModulo % Convert.ToUInt64(blockSize));
        ulong dataFileSize = (blockCapacity * blockSize) % _dataPageSize == 0UL 
            ? (blockCapacity * blockSize) 
            : (blockCapacity * blockSize) + _dataPageSize - ((blockCapacity * blockSize) % _dataPageSize);
        
        ulong blockLengthsPageSizeBeforeModulo = Math.Min(targetPageSize, Convert.ToUInt64(sizeof(int))  * _blockCapacity);
        _blockLengthsPageSize = blockLengthsPageSizeBeforeModulo % Convert.ToUInt64(sizeof(int)) == 0UL 
            ? blockLengthsPageSizeBeforeModulo 
            : blockLengthsPageSizeBeforeModulo + Convert.ToUInt64(sizeof(int)) - (blockLengthsPageSizeBeforeModulo % Convert.ToUInt64(sizeof(int)));
        ulong blockLengthsFileSize = (blockCapacity * sizeof(int)) % _blockLengthsPageSize == 0UL 
            ? (blockCapacity * sizeof(int)) 
            : (blockCapacity * sizeof(int)) + _blockLengthsPageSize - ((blockCapacity * sizeof(int)) % _blockLengthsPageSize);
        
        ulong enqueueTimesPageSizeBeforeModulo = Math.Min(targetPageSize, Convert.ToUInt64(sizeof(long)) * _blockCapacity);
        _enqueueTimesPageSize = enqueueTimesPageSizeBeforeModulo % Convert.ToUInt64(sizeof(long)) == 0UL 
            ? enqueueTimesPageSizeBeforeModulo 
            : enqueueTimesPageSizeBeforeModulo + Convert.ToUInt64(sizeof(long)) - (enqueueTimesPageSizeBeforeModulo % Convert.ToUInt64(sizeof(long)));
        ulong enqueueTimesFileSize = (blockCapacity * sizeof(long)) % _enqueueTimesPageSize == 0UL 
            ? (blockCapacity * sizeof(long)) 
            : (blockCapacity * sizeof(long)) + _enqueueTimesPageSize - ((blockCapacity * sizeof(long)) % _enqueueTimesPageSize);
        
        _count = 0u;
        _dropCount = 0UL;
        _readLock = new object();
        _writeLock = new object();
        
        string dataFilePath = System.IO.Path.Combine(fileDirectory, $"{fileNamePrefix}_Data.bin");
        string blockLengthsFilePath = System.IO.Path.Combine(fileDirectory, $"{fileNamePrefix}_Lengths.bin");
        string enqueueTimesFilePath = System.IO.Path.Combine(fileDirectory, $"{fileNamePrefix}_EnqueueTimes.bin");
        
        using (var fs = new FileStream(dataFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(Convert.ToInt64(dataFileSize));
        }
        _data = MemoryMappedFile.CreateFromFile(dataFilePath);
        _dataWriteAccessor = _data.CreateViewAccessor(0, Convert.ToInt64(_dataPageSize), MemoryMappedFileAccess.ReadWrite);
        _dataReadAccessor = _data.CreateViewAccessor(0, Convert.ToInt64(_dataPageSize), MemoryMappedFileAccess.Read);
        
        using (var fs = new FileStream(blockLengthsFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(Convert.ToInt64(blockLengthsFileSize));
        }
        _blockLengthsData = MemoryMappedFile.CreateFromFile(blockLengthsFilePath);
        _blockLengthsWriteAccessor = _blockLengthsData.CreateViewAccessor(0, Convert.ToInt64(_blockLengthsPageSize), MemoryMappedFileAccess.ReadWrite);
        _blockLengthsReadAccessor = _blockLengthsData.CreateViewAccessor(0, Convert.ToInt64(_blockLengthsPageSize), MemoryMappedFileAccess.Read);
        
        using (var fs = new FileStream(enqueueTimesFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(Convert.ToInt64(enqueueTimesFileSize));
        }
        _enqueueTimesData = MemoryMappedFile.CreateFromFile(enqueueTimesFilePath);
        _enqueueTimesWriteAccessor = _enqueueTimesData.CreateViewAccessor(0, Convert.ToInt64(_enqueueTimesPageSize), MemoryMappedFileAccess.ReadWrite);
        _enqueueTimesReadAccessor = _enqueueTimesData.CreateViewAccessor(0, Convert.ToInt64(_enqueueTimesPageSize), MemoryMappedFileAccess.Read);
    }

    #endregion //Constructors
    
    public void Dispose()
    {
        GulpObjectDisposedException(() =>
        {
            _dataWriteAccessor.Flush();
            _dataWriteAccessor.Dispose();
            _dataReadAccessor.Dispose();
            _data.Dispose(); 
        });
        
        GulpObjectDisposedException(() =>
        {
            _blockLengthsWriteAccessor.Flush();
            _blockLengthsWriteAccessor.Dispose();
            _blockLengthsReadAccessor.Dispose();
            _blockLengthsData.Dispose(); 
        });
    
        GulpObjectDisposedException(() =>
        {
            _enqueueTimesWriteAccessor.Flush();
            _enqueueTimesWriteAccessor.Dispose();
            _enqueueTimesReadAccessor.Dispose();
            _enqueueTimesData.Dispose();
        });
    }

    private static void GulpObjectDisposedException(Action action)
    {
        try
        {
            action();
        }
        catch (ObjectDisposedException) { }
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
            try
            {
                _dataWriteAccessor.Flush();
                _dataWriteAccessor.Dispose();
            }
            catch (ObjectDisposedException)
            {
                //gulping the exception here because it's not really an issue.  The accessor is being recreated on the next call to this method, and was flushed on disposal.
            }
            _dataWriteAccessor = _data.CreateViewAccessor(Convert.ToInt64(GetPageStartingIndex(nextDesiredPageIndex, _dataPageSize)), Convert.ToInt64(_dataPageSize), MemoryMappedFileAccess.ReadWrite);
            _dataWritePageIndex = nextDesiredPageIndex;
        }
        
        return _dataWriteAccessor;
    }
    
    private MemoryMappedViewAccessor GetBlockLengthsWriteAccessor()
    {
        ulong nextDesiredPageIndex = GetPageIndex(_blockNextWriteIndex, sizeof(int), _blockLengthsPageSize);
        if (nextDesiredPageIndex != _blockLengthsWritePageIndex)
        {
            try
            {
                _blockLengthsWriteAccessor.Flush();
                _blockLengthsWriteAccessor.Dispose();
            }
            catch (ObjectDisposedException)
            {
                //gulping the exception here because it's not really an issue.  The accessor is being recreated on the next call to this method, and was flushed on disposal.
            }
            _blockLengthsWriteAccessor = _blockLengthsData.CreateViewAccessor(Convert.ToInt64(GetPageStartingIndex(nextDesiredPageIndex, _blockLengthsPageSize)), Convert.ToInt64(_blockLengthsPageSize), MemoryMappedFileAccess.ReadWrite);
            _blockLengthsWritePageIndex = nextDesiredPageIndex;
        }
        
        return _blockLengthsWriteAccessor;
    }
    
    private MemoryMappedViewAccessor GetEnqueueTimesWriteAccessor()
    {
        ulong nextDesiredPageIndex = GetPageIndex(_blockNextWriteIndex, sizeof(long), _enqueueTimesPageSize);
        if (nextDesiredPageIndex != _enqueueTimesWritePageIndex)
        {
            try
            {
                _enqueueTimesWriteAccessor.Flush();
                _enqueueTimesWriteAccessor.Dispose();
            }
            catch (ObjectDisposedException)
            {
                //gulping the exception here because it's not really an issue.  The accessor is being recreated on the next call to this method, and was flushed on disposal.
            }
            _enqueueTimesWriteAccessor = _enqueueTimesData.CreateViewAccessor(Convert.ToInt64(GetPageStartingIndex(nextDesiredPageIndex, _enqueueTimesPageSize)), Convert.ToInt64(_enqueueTimesPageSize), MemoryMappedFileAccess.ReadWrite);
            _enqueueTimesWritePageIndex = nextDesiredPageIndex;
        }
        
        return _enqueueTimesWriteAccessor;
    }
    
    private MemoryMappedViewAccessor GetDataReadAccessor()
    {
        ulong nextDesiredPageIndex = GetPageIndex(_blockNextReadIndex, _blockSize, _dataPageSize);
        if (nextDesiredPageIndex != _dataReadPageIndex)
        {
            try
            {
                _dataReadAccessor.Dispose();
            }
            catch (ObjectDisposedException)
            {
                //gulping the exception here because it's not really an issue.  The accessor is being recreated on the next call to this method, and was flushed on disposal.
            }
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
            try
            {
                _blockLengthsReadAccessor.Dispose();
            }
            catch (ObjectDisposedException)
            {
                //gulping the exception here because it's not really an issue.  The accessor is being recreated on the next call to this method, and was flushed on disposal.
            }
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
            try
            {
                _enqueueTimesReadAccessor.Dispose();
            }
            catch (ObjectDisposedException)
            {
                //gulping the exception here because it's not really an issue.  The accessor is being recreated on the next call to this method, and was flushed on disposal.
            }
            _enqueueTimesReadAccessor = _enqueueTimesData.CreateViewAccessor(Convert.ToInt64(GetPageStartingIndex(nextDesiredPageIndex, _enqueueTimesPageSize)), Convert.ToInt64(_enqueueTimesPageSize), MemoryMappedFileAccess.Read);
            _enqueueTimesReadPageIndex = nextDesiredPageIndex;
        }
        
        return _enqueueTimesReadAccessor;
    }
    
    /// <summary>
    /// Thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize!
    /// Full behavior: the oldest block in the ring buffer will be dropped. 
    /// </summary>
    /// <param name="blockToEnqueue">The byte block to copy from.</param>
    public bool TryEnqueue(ReadOnlySpan<byte> blockToEnqueue)
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
            
            int length = Math.Min(blockToEnqueue.Length, Convert.ToInt32(_blockSize));
            ReadOnlySpan<byte> trimmedBlock = blockToEnqueue.Slice(0, length);
            trimmedBlock.CopyTo(_writeBuffer);
            GetDataWriteAccessor().WriteArray(Convert.ToInt64(GetIntraPageOffset(_blockNextWriteIndex, BlockSize, _dataPageSize)), _writeBuffer, 0, _writeBuffer.Length);
            GetBlockLengthsWriteAccessor().Write(Convert.ToInt64(GetIntraPageOffset(_blockNextWriteIndex, sizeof(int), _blockLengthsPageSize)), length);
            GetEnqueueTimesWriteAccessor().Write(Convert.ToInt64(GetIntraPageOffset(_blockNextWriteIndex, sizeof(long), _enqueueTimesPageSize)), _stopwatch.ElapsedMilliseconds);
            
            _blockNextWriteIndex = (++_blockNextWriteIndex) % BlockCapacity;
            Interlocked.Increment(ref _count);
            return true;
        }
    }

    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize or greater!
    /// </summary>
    /// <param name="fullBlockBuffer">The buffer to copy the byte block to.</param>
    public bool TryDequeue(Span<byte> fullBlockBuffer)
    {
        lock (_readLock)
        {
            if (IsEmptyNoLock())
                return false;
            
            long enqueueTime = GetEnqueueTimesReadAccessor().ReadInt64(Convert.ToInt64(GetIntraPageOffset(_blockNextReadIndex, sizeof(long), _enqueueTimesPageSize)));
            if (_delayMilliseconds > (_stopwatch.ElapsedMilliseconds - enqueueTime))
                return false;
            
            GetDataReadAccessor().ReadArray(Convert.ToInt64(GetIntraPageOffset(_blockNextReadIndex, _blockSize, _dataPageSize)), _readBuffer, 0, _readBuffer.Length);
            _readBuffer.CopyTo(fullBlockBuffer);
            
            _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
            Interlocked.Decrement(ref _count);
            Interlocked.Increment(ref _processed);
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
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="trimmedBuffer">The fullBlockBuffer, trimmed down to the original size it enqueued as.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    public bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer)
    {
        lock (_readLock)
        {
            trimmedBuffer = fullBlockBuffer;
            if (IsEmptyNoLock())
                return false;

            long enqueueTime = GetEnqueueTimesReadAccessor().ReadInt64(Convert.ToInt64(GetIntraPageOffset(_blockNextReadIndex, sizeof(long), _enqueueTimesPageSize)));
            if (_delayMilliseconds > (_stopwatch.ElapsedMilliseconds - enqueueTime))
                return false;
            
            GetDataReadAccessor().ReadArray(Convert.ToInt64(GetIntraPageOffset(_blockNextReadIndex, _blockSize, _dataPageSize)), _readBuffer, 0, _readBuffer.Length);
            _readBuffer.CopyTo(fullBlockBuffer);
            int length = GetBlockLengthsReadAccessor().ReadInt32(Convert.ToInt64(GetIntraPageOffset(_blockNextReadIndex, sizeof(int), _blockLengthsPageSize)));
            trimmedBuffer = fullBlockBuffer.Slice(0, length);
            
            _blockNextReadIndex = (++_blockNextReadIndex) % BlockCapacity;
            Interlocked.Decrement(ref _count);
            Interlocked.Increment(ref _processed);
            return true;
        }
    }
}
namespace Intrinio.Collections.RingBuffers;

using System;
using System.Threading;
using System.Runtime.CompilerServices;
using System.IO.MemoryMappedFiles;
using System.IO;

/// <summary>
/// A read thread-safe, write not thread-safe implementation of the <see cref="IDynamicBlockRingBuffer"/> (single producer and multiple consumer), with support for tracking the used size of each byte-block.  Full behavior: the block trying to be enqueued will be dropped. 
/// </summary>
public class MemMapDelayDynamicBlockRingBuffer: IDynamicBlockRingBuffer
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
    private readonly object _writeLock;
    private readonly object _readLock;
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
    public MemMapDelayDynamicBlockRingBuffer(uint delayMilliseconds, uint blockSize, ulong blockCapacity, string fileDirectory, string fileNamePrefix, System.Diagnostics.Stopwatch? stopwatch = default, ulong targetPageSize = DefaultTargetPageSize)
    {
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
        
        if (blockSize > targetPageSize)
            throw new ArgumentException($"Argument blockSize must be less than {nameof(targetPageSize)}", nameof(blockSize));
        
        _dataPageSize = (Math.Min(targetPageSize, Convert.ToUInt64(blockSize) * _blockCapacity) / Convert.ToUInt64(blockSize)) * Convert.ToUInt64(blockSize);
        _blockLengthsPageSize = (Math.Min(targetPageSize, Convert.ToUInt64(sizeof(int)) * _blockCapacity) / Convert.ToUInt64(sizeof(int))) * Convert.ToUInt64(sizeof(int));
        _enqueueTimesPageSize = (Math.Min(targetPageSize, Convert.ToUInt64(sizeof(long)) * _blockCapacity) / Convert.ToUInt64(sizeof(long))) * Convert.ToUInt64(sizeof(long));
        _count = 0u;
        _dropCount = 0UL;
        _writeLock = new object();
        _readLock = new object();
        
        string dataFilePath = System.IO.Path.Combine(fileDirectory, $"{fileNamePrefix}_Data.bin");
        string blockLengthsFilePath = System.IO.Path.Combine(fileDirectory, $"{fileNamePrefix}_Lengths.bin");
        string enqueueTimesFilePath = System.IO.Path.Combine(fileDirectory, $"{fileNamePrefix}_EnqueueTimes.bin");
        
        using (var fs = new FileStream(dataFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(Convert.ToInt64(blockCapacity) * Convert.ToInt64(blockSize));
        }
        _data = MemoryMappedFile.CreateFromFile(dataFilePath);
        _dataWriteAccessor = _data.CreateViewAccessor(0, Convert.ToInt64(_dataPageSize), MemoryMappedFileAccess.ReadWrite);
        _dataReadAccessor = _data.CreateViewAccessor(0, Convert.ToInt64(_dataPageSize), MemoryMappedFileAccess.Read);
        
        using (var fs = new FileStream(blockLengthsFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(Convert.ToInt64(blockCapacity) * Convert.ToInt64(sizeof(int)));
        }
        _blockLengthsData = MemoryMappedFile.CreateFromFile(blockLengthsFilePath);
        _blockLengthsWriteAccessor = _blockLengthsData.CreateViewAccessor(0, Convert.ToInt64(_blockLengthsPageSize), MemoryMappedFileAccess.ReadWrite);
        _blockLengthsReadAccessor = _blockLengthsData.CreateViewAccessor(0, Convert.ToInt64(_blockLengthsPageSize), MemoryMappedFileAccess.Read);
        
        using (var fs = new FileStream(enqueueTimesFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.SetLength(Convert.ToInt64(blockCapacity) * Convert.ToInt64(sizeof(long)));
        }
        _enqueueTimesData = MemoryMappedFile.CreateFromFile(enqueueTimesFilePath);
        _enqueueTimesWriteAccessor = _enqueueTimesData.CreateViewAccessor(0, Convert.ToInt64(_enqueueTimesPageSize), MemoryMappedFileAccess.ReadWrite);
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
    
    /// <summary>
    /// Thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize!
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

        lock (_writeLock)
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
            _blockLengths[_blockNextReadIndex] = length;
            _enqueueTimes[_blockNextWriteIndex] = _stopwatch.ElapsedMilliseconds;
            
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
            if (IsEmptyNoLock() || (_delayMilliseconds > (_stopwatch.ElapsedMilliseconds - _enqueueTimes[_blockNextReadIndex])))
                return false;
            
            Span<byte> target = new Span<byte>(_data, Convert.ToInt32(_blockNextReadIndex * BlockSize), Convert.ToInt32(BlockSize));
            target.CopyTo(fullBlockBuffer);
            
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer)
    {
        lock (_readLock)
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
}
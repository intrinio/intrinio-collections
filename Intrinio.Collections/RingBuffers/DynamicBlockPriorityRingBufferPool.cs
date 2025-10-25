using System;
using System.Collections.Generic;
using System.Threading;

namespace Intrinio.Collections.RingBuffers;

public class DynamicBlockPriorityRingBufferPool : IDynamicBlockPriorityRingBufferPool
{
    private readonly uint _blockSize;
    private readonly List<IDynamicBlockRingBuffer?> _ringBuffers;
    
#if NET9_0_OR_GREATER
    private readonly Lock               _addUpdateLock = new Lock();
#else
    private readonly object             _addUpdateLock = new object();
#endif

    public DynamicBlockPriorityRingBufferPool(uint blockSize)
    {
        _blockSize     = blockSize;
        _ringBuffers   = new List<IDynamicBlockRingBuffer?>();
    }

    public void AddUpdateRingBufferToPool(uint priority, IDynamicBlockRingBuffer ringBuffer)
    {
        if (ringBuffer.BlockSize != _blockSize)
            throw new ArgumentException("RingBuffer BlockSize must match the pool BlockSize");
        
        lock (_addUpdateLock)
        {
            while (_ringBuffers.Count <= priority) //We need to expand
                _ringBuffers.Add(null);

            _ringBuffers[Convert.ToInt32(priority)] = ringBuffer;
        }
    }

    public bool TryEnqueue(uint priority, ReadOnlySpan<byte> blockToWrite)
    {
        if (_ringBuffers.Count <= priority || _ringBuffers[Convert.ToInt32(priority)] == null)
            return false;

        return _ringBuffers[Convert.ToInt32(priority)].TryEnqueue(blockToWrite);
    }

    public bool TryDequeue(Span<byte> blockBuffer)
    {
        int i = 0;
        while (i < _ringBuffers.Count)
        {
            if (_ringBuffers[i] != null && _ringBuffers[i].TryDequeue(blockBuffer))
                return true;
            
            ++i;
        }

        return false;
    }
    
    public bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer)
    {
        int i = 0;
        while (i < _ringBuffers.Count)
        {
            if (_ringBuffers[i] != null && _ringBuffers[i].TryDequeue(fullBlockBuffer, out trimmedBuffer))
                return true;
            
            ++i;
        }

        trimmedBuffer = fullBlockBuffer;
        return false;
    }

    public ulong Count
    {
        get
        {
            ulong total = 0UL;
            for (int i = 0; i < _ringBuffers.Count; i++)
            {
                if (_ringBuffers[i] != null)
                    total += _ringBuffers[i].Count;
            }

            return total;
        }
    }

    public ulong ProcessedCount
    {
        get
        {
            ulong total = 0UL;
            for (int i = 0; i < _ringBuffers.Count; i++)
            {
                if (_ringBuffers[i] != null)
                    total += _ringBuffers[i].ProcessedCount;
            }

            return total;
        }
    }

    public uint BlockSize
    {
        get { return _blockSize; }
    }

    public ulong TotalBlockCapacity
    {
        get
        {
            ulong total = 0UL;
            for (int i = 0; i < _ringBuffers.Count; i++)
            {
                if (_ringBuffers[i] != null)
                    total += _ringBuffers[i].BlockCapacity;
            }

            return total;
        }
    }

    public ulong DropCount
    {
        get
        {
            ulong total = 0UL;
            for (int i = 0; i < _ringBuffers.Count; i++)
            {
                if (_ringBuffers[i] != null)
                    total += _ringBuffers[i].DropCount;
            }

            return total;
        }
    }

    public bool IsEmpty
    {
        get
        {
            for (int i = 0; i < _ringBuffers.Count; i++)
            {
                if (_ringBuffers[i] != null && !_ringBuffers[i].IsEmpty)
                    return false;
            }

            return true;
        }
    }

    public bool IsFull
    {
        get
        {
            for (int i = 0; i < _ringBuffers.Count; i++)
            {
                if (_ringBuffers[i] != null && !_ringBuffers[i].IsFull)
                    return false;
            }

            return true;
        }
    }

    public ulong GetCount(uint priority)
    {
        if (_ringBuffers.Count <= priority || _ringBuffers[Convert.ToInt32(priority)] == null)
            return 0UL;

        return _ringBuffers[Convert.ToInt32(priority)].Count;
    }

    public ulong GetDropCount(uint priority)
    {
        if (_ringBuffers.Count <= priority || _ringBuffers[Convert.ToInt32(priority)] == null)
            return 0UL;

        return _ringBuffers[Convert.ToInt32(priority)].DropCount;
    }
    
    public ulong GetCapacity(uint priority)
    {
        if (_ringBuffers.Count <= priority || _ringBuffers[Convert.ToInt32(priority)] == null)
            return 0UL;

        return _ringBuffers[Convert.ToInt32(priority)].BlockCapacity;
    }
}
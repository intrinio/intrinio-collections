namespace Intrinio.Collections.RingBuffers;

using System;

/// <summary>
/// A pool of <see cref="Intrinio.Collections.RingBuffers.IRingBuffer"/>, each handing a specific priority. Priorities should be numerically compact.
/// </summary>
public interface IDynamicBlockPriorityRingBufferPool
{
    /// <summary>
    /// Add/Update a <see cref="Intrinio.Collections.RingBuffers.IRingBuffer"/> to the pool at the provided priority.
    /// </summary>
    /// <param name="priority"></param>
    /// <param name="ringBuffer"></param>
    void AddUpdateRingBufferToPool(uint priority, IDynamicBlockRingBuffer ringBuffer);
    
    /// <summary>
    /// Try to enqueue in the <see cref="Intrinio.Collections.RingBuffers.IRingBuffer"/> corresponding with the provided priority.
    /// </summary>
    /// <param name="priority">The zero based priority.</param>
    /// <param name="blockToWrite">The byte block to copy from.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    bool TryEnqueue(uint priority, ReadOnlySpan<byte> blockToWrite);
    
    /// <summary>
    /// Try to dequeue.  Parameter "blockBuffer" MUST be of length BlockSize or greater! Dequeues from the ascending highest priority queue first. 
    /// </summary>
    /// <param name="blockBuffer">The buffer to copy the byte block to.</param>
    /// <returns>Whether a block was successfully dequeued or not.</returns>
    bool TryDequeue(Span<byte> blockBuffer);
    
    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer. Dequeues from the ascending highest priority queue first.
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="trimmedBuffer">The fullBlockBuffer, trimmed down to the original size it enqueued as.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer);
    
    /// <summary>
    /// The count of blocks currently in the ring buffer.
    /// </summary>
    ulong Count { get; }
    
    /// <summary>
    /// The quantity of blocks dequeued from the ring buffer.
    /// </summary>
    ulong ProcessedCount { get ; }
    
    /// <summary>
    /// The fixed size of each byte block.
    /// </summary>
    uint BlockSize { get; }
    
    /// <summary>
    /// The fixed total capacity of blocks across all ring buffers.
    /// </summary>
    ulong TotalBlockCapacity { get; }
    
    /// <summary>
    /// The quantity of dropped blocks due to being full. 
    /// </summary>
    ulong DropCount { get; }
    
    /// <summary>
    /// Whether the ring buffer is currently empty.
    /// </summary>
    bool IsEmpty { get; }
    
    /// <summary>
    /// Whether the ring buffer is currently full.
    /// </summary>
    bool IsFull { get; }

    /// <summary>
    /// The count of blocks currently in the ring buffer at the specified priority.
    /// </summary>
    /// <param name="priority">The zero based priority to check.</param>
    /// <returns></returns>
    public ulong GetCount(uint priority);

    /// <summary>
    /// The quantity of dropped blocks at the specified priority.
    /// </summary>
    /// <param name="priority">The zero based priority to check.</param>
    /// <returns></returns>
    public ulong GetDropCount(uint priority);
    
    /// <summary>
    /// The capacity in the ring buffer at the specified priority.
    /// </summary>
    /// <param name="priority">The zero based priority to check.</param>
    /// <returns></returns>
    public ulong GetCapacity(uint priority);
}
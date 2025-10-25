namespace Intrinio.Collections.RingBuffers;

using System;

/// <summary>
/// A pool of <see cref="Intrinio.Collections.RingBuffers.IRingBuffer"/>, each handing a specific priority. Priorities should be numerically compact.
/// </summary>
public interface IPriorityRingBufferPool
{
    /// <summary>
    /// Add/Update a <see cref="Intrinio.Collections.RingBuffers.IRingBuffer"/> to the pool at the provided priority.
    /// </summary>
    /// <param name="priority"></param>
    /// <param name="ringBuffer"></param>
    void AddUpdateRingBufferToPool(uint priority, IRingBuffer ringBuffer);
    
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

/// <summary>
/// A pool of <see cref="Intrinio.Collections.RingBuffers.IRingBuffer"/>, each handing a specific priority. Priorities should be numerically compact.
/// </summary>
public interface IPriorityRingBufferPool<T> where T : struct
{
    /// <summary>
    /// Add/Update a <see cref="Intrinio.Collections.RingBuffers.IRingBuffer"/> to the pool at the provided priority.
    /// </summary>
    /// <param name="priority"></param>
    /// <param name="ringBuffer"></param>
    void AddUpdateRingBufferToPool(uint priority, IRingBuffer<T> ringBuffer);
    
    /// <summary>
    /// Try to enqueue in the <see cref="Intrinio.Collections.RingBuffers.IRingBuffer"/> corresponding with the provided priority.
    /// </summary>
    /// <param name="priority">The zero based priority.</param>
    /// <param name="data">The <see cref="T"/> enqueue.</param>
    /// <returns>Whether the <see cref="T"/> was successfully enqueued or not.</returns>
    bool TryEnqueue(uint priority, T data);
    
    /// <summary>
    /// Try to dequeue a <see cref="T"/>.  Dequeues from the ascending highest priority queue first. 
    /// </summary>
    /// <param name="data">The dequeued <see cref="T"/>.</param>
    /// <returns>Whether a <see cref="T"/> was successfully dequeued or not.</returns>
    bool TryDequeue(out T data);
    
    /// <summary>
    /// The count of <see cref="T"/> currently in the ring buffer.
    /// </summary>
    ulong Count { get; }
    
    /// <summary>
    /// The quantity of <see cref="T"/> dequeued from the ring buffer.
    /// </summary>
    ulong ProcessedCount { get ; }
    
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
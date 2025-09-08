namespace Intrinio.Collections.RingBuffers;

using System;

/// <summary>
/// A fixed-size byte-block circular queue.
/// </summary>
public interface IRingBuffer
{
    /// <summary>
    /// Try to enqueue a byte block via copy from the provided buffer.
    /// </summary>
    /// <param name="blockToWrite">The byte block to copy from.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    bool TryEnqueue(ReadOnlySpan<byte> blockToWrite);
    
    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="fullBlockBuffer">The buffer to copy the byte block to.</param>
    /// <returns>Whether a block was successfully dequeued or not.</returns>
    bool TryDequeue(Span<byte> fullBlockBuffer);
    
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
    /// The fixed capacity of blocks in the ring buffer.
    /// </summary>
    ulong BlockCapacity { get; }
    
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
}

/// <summary>
/// A fixed-size <see cref="T"/> circular queue.
/// </summary>
public interface IRingBuffer<T> where T : struct
{
    /// <summary>
    /// Try to enqueue a <see cref="T"/>.
    /// </summary>
    /// <param name="obj">The <see cref="T"/> to enqueue.</param>
    /// <returns>Whether the <see cref="T"/> was successfully enqueued or not.</returns>
    bool TryEnqueue(T obj);
    
    /// <summary>
    /// Try to dequeue a <see cref="T"/>.
    /// </summary>
    /// <param name="obj">The dequeued <see cref="T"/>.</param>
    /// <returns>Whether a <see cref="T"/> was successfully dequeued or not.</returns>
    bool TryDequeue(out T obj);
    
    /// <summary>
    /// The count of <see cref="T"/> currently in the ring buffer.
    /// </summary>
    ulong Count { get; }
    
    /// <summary>
    /// The quantity of <see cref="T"/> dequeued from the ring buffer.
    /// </summary>
    ulong ProcessedCount { get ; }
    
    /// <summary>
    /// The fixed capacity of <see cref="T"/> in the ring buffer.
    /// </summary>
    ulong Capacity { get; }
    
    /// <summary>
    /// The quantity of dropped <see cref="T"/> due to being full.
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
}
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
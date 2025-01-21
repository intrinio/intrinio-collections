namespace Intrinio.Collections.RingBuffers;

using System;

/// <summary>
/// A fixed-size byte-block circular queue with support for tracking the used size of each byte-block.
/// </summary>
public interface IDynamicBlockRingBuffer
{
    /// <summary>
    /// The count of blocks currently in the ring buffer.
    /// </summary>
    ulong Count { get; }
    
    /// <summary>
    /// The fixed size of each byte block.
    /// </summary>
    uint BlockSize { get; }
    
    /// <summary>
    /// The fixed capacity of blocks in the ring buffer.
    /// </summary>
    uint BlockCapacity { get; }
    
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
    /// The fixed usable size of each byte block.
    /// </summary>
    uint UsableBlockSize { get; }
    
    /// <summary>
    /// Try to enqueue a byte block via copy from the provided buffer.
    /// </summary>
    /// <param name="blockToWrite">The byte block to copy from.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    bool TryEnqueue(Span<byte> blockToWrite);
    
    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="blockBuffer">The buffer to copy the byte block to.</param>
    /// <returns>Whether a block was successfully dequeued or not.</returns>
    bool TryDequeue(Span<byte> blockBuffer);

    /// <summary>
    /// Not thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize! This is not safe for calling concurrently, and intended for use with a single producer.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="fullBlockToWrite">The byte block to copy from.</param>
    /// <param name="usedLength">The length of used space in fullBlockToWrite, not including the section for used size tracking.  Use GetUsableArea to aid in this, and then use the length of that span after your further manipulation here.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    bool TryEnqueue(Span<byte> fullBlockToWrite, uint usedLength);

    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="usedSize">The used size of the full block.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    bool TryDequeue(Span<byte> fullBlockBuffer, out uint usedSize);

    /// <summary>
    /// Slice the full block to the writable area (removing the internally tracked used size section).
    /// </summary>
    /// <param name="fullBlockToTrim">The full sized block.</param>
    /// <returns>The fullBlockToTrim windowed without the internally tracked used size section.</returns>
    static Span<byte> GetUsableArea(Span<byte> fullBlockToTrim)
    {
        throw new NotImplementedException();
    }
}
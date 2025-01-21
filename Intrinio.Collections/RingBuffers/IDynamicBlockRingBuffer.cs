namespace Intrinio.Collections.RingBuffers;

using System;

/// <summary>
/// 
/// </summary>
public interface IDynamicBlockRingBuffer : IRingBuffer
{
    /// <summary>
    /// The fixed usable size of each byte block.
    /// </summary>
    uint UsableBlockSize { get; }

    /// <summary>
    /// Not thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize! This is not safe for calling concurrently, and intended for use with a single producer.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="fullBlockToWrite">The byte block to copy from.</param>
    /// <param name="usedBlock">Full block, windowed for the writable area and trimmed down to the used size.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    bool TryEnqueue(Span<byte> fullBlockToWrite, ReadOnlySpan<byte> usedBlock);

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
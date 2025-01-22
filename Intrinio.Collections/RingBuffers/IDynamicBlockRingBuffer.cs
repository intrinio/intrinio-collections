namespace Intrinio.Collections.RingBuffers;

using System;

/// <summary>
/// A fixed-size byte-block circular queue with support for tracking the used size of each byte-block.
/// </summary>
public interface IDynamicBlockRingBuffer : IRingBuffer
{
    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="trimmedBuffer">The fullBlockBuffer, trimmed down to the original size it enqueued as.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer);
}
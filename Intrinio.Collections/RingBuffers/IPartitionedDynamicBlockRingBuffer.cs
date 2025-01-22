namespace Intrinio.Collections.RingBuffers;

using System;

/// <summary>
/// A fixed size group of single producer <see cref="Intrinio.Collections.RingBuffers.IDynamicBlockRingBuffer"/> partitioned by an index, so that multiple writers may have their own write channel without being locked, while consumption is channel agnostic and thread-safe.
/// </summary>
public interface IPartitionedDynamicBlockRingBuffer : IPartitionedRingBuffer
{
    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to.</param>
    /// <param name="trimmedBuffer">The fullBlockBuffer, trimmed down to the original size it enqueued as.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    bool TryDequeue(Span<byte> fullBlockBuffer, out Span<byte> trimmedBuffer);
}
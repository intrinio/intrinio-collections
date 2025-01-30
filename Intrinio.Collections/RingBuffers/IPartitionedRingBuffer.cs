namespace Intrinio.Collections.RingBuffers;

using System;

/// <summary>
/// A fixed size group of single producer <see cref="Intrinio.Collections.RingBuffers.IRingBuffer"/> partitioned by an index, so that multiple writers may have their own write channel without being locked, while consumption is channel agnostic and thread-safe.
/// </summary>
public interface IPartitionedRingBuffer
{
    /// <summary>
    /// A try enqueue where for each value of threadIndex, only one thread will be calling concurrently at a time.
    /// </summary>
    /// <param name="threadIndex">The zero based index for the channel to try enqueuing to. Max value is concurrency - 1.</param>
    /// <param name="blockToWrite">The byte block to copy from.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    bool TryEnqueue(uint threadIndex, ReadOnlySpan<byte> blockToWrite);
    
    /// <summary>
    /// Thread-safe try dequeue.  Parameter "blockBuffer" MUST be of length BlockSize or greater!
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
    /// The fixed capacity of blocks in each ring buffer.
    /// </summary>
    uint EachQueueBlockCapacity { get; }
    
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
    /// The count of blocks currently in the ring buffer at the specified index.
    /// </summary>
    /// <param name="threadIndex">The zero based index for the channel to try enqueuing to. Max value is concurrency - 1.</param>
    /// <returns></returns>
    public ulong GetCount(int threadIndex);

    /// <summary>
    /// The quantity of dropped blocks due to being full at the specified index.
    /// </summary>
    /// <param name="threadIndex">The zero based index for the channel to try enqueuing to. Max value is concurrency - 1.</param>
    /// <returns></returns>
    public ulong GetDropCount(int threadIndex);
}
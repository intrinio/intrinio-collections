namespace Intrinio.Collections.RingBuffers;

/// <summary>
/// A dynamic block ring buffer where blocks are delayed from being dequeued for a specified time. Dequeues are blocked until a non-partition block is able to be dequeued. Enqueue partition blocks at regular intervals to gate-keep time partitions, as contiguous non-partition blocks between partition blocks will not be delayed.  Provides support for dealing with blocks of varying size less than or equal to block size. 
/// </summary>
public interface IDelayDynamicBlockRingBuffer : IDynamicBlockRingBuffer
{
    /// <summary>
    /// Enqueue a partition packed to gate-keep blocks.  This should be called on a regular interval.
    /// </summary>
    /// <returns></returns>
    bool EnqueuePartitionPacket();
}
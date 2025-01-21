using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Intrinio.Collections.RingBuffers;

using System;

/// <summary>
/// 
/// </summary>
public class DynamicBlockSingleProducerRingBuffer: SingleProducerRingBuffer, IDynamicBlockRingBuffer
{
    public uint UsableBlockSize { get { return base.BlockSize - sizeof(UInt32); } }
    
    #region Constructors

    /// <summary>
    /// A read thread-safe, write not thread-safe implementation of the IRingBuffer (single producer and multiple consumer).  Full behavior: the block trying to be enqueued will be dropped. Provides support for dealing with blocks of varying size less than or equal to block size minus sizeof(UInt32). The first sizeof(UInt32) bytes of each block are reserved for tracking the used size of that block. 
    /// </summary>
    /// <param name="blockSize">The fixed size of each byte block. Internally, the first sizeof(UInt32) of each block is reserved for tracking the used size of each block. /></param>
    /// <param name="blockCapacity">The fixed capacity of block count.</param>
    public DynamicBlockSingleProducerRingBuffer(uint blockSize, uint blockCapacity) : base(blockSize, blockCapacity)
    {
        
    }

    #endregion //Constructors

    /// <summary>
    /// Not thread-safe try enqueue.  Parameter "blockToWrite" MUST be of length BlockSize! This is not safe for calling concurrently, and intended for use with a single producer.
    /// Full behavior: the block trying to be enqueued will be dropped. 
    /// </summary>
    /// <param name="fullBlockToWrite">The byte block to copy from.</param>
    /// <param name="usedBlock">Full block, windowed for the writable area and trimmed down to the used size.</param>
    /// <returns>Whether the block was successfully enqueued or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryEnqueue(Span<byte> fullBlockToWrite, ReadOnlySpan<byte> usedBlock)
    {
        BinaryPrimitives.WriteUInt32BigEndian(fullBlockToWrite, Convert.ToUInt32(usedBlock.Length));
        return base.TryEnqueue(fullBlockToWrite);
    }

    /// <summary>
    /// Try to dequeue a byte block via copy to the provided buffer.
    /// </summary>
    /// <param name="fullBlockBuffer">The full sized buffer to copy the byte block to. Must be of BlockSize.</param>
    /// <param name="usedSize">The used size of the full block.</param>
    /// <returns>Whether the dequeue successfully retrieved a block or not.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryDequeue(Span<byte> fullBlockBuffer, out uint usedSize)
    {
        bool result = base.TryDequeue(fullBlockBuffer);
        usedSize = BinaryPrimitives.ReadUInt32BigEndian(fullBlockBuffer);
        return result;
    }
    
    /// <summary>
    /// Slice the full block to the writable area (removing the internally tracked used size section).
    /// </summary>
    /// <param name="fullBlockToTrim">The full sized block.</param>
    /// <returns>The fullBlockToTrim windowed without the internally tracked used size section.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Span<byte> GetUsableArea(Span<byte> fullBlockToTrim)
    {
        return fullBlockToTrim.Slice(sizeof(UInt32));
    }
}
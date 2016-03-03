package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;

public class MemoryBlockHolder {
    private UnsafeRow row;
    private final int fixedSize;
    private MemoryBlock block;
    private MemoryAllocator alloc;

    public int cursor = 0;

    public MemoryBlockHolder( UnsafeRow aRow ) { this( aRow, 64 ); }
    public MemoryBlockHolder( UnsafeRow aRow, int aSize ) {
        this.row = aRow;

        MemoryAllocator anAllocator;
        if (SparkHadoopUtil.get().conf().getBoolean("spark.memory.offHeap.enabled", false))
            anAllocator = MemoryAllocator.UNSAFE;
        else
            anAllocator = MemoryAllocator.HEAP;

        this.alloc = anAllocator;

        this.fixedSize = UnsafeRow.calculateBitSetWidthInBytes(this.row.numFields()) + 8 * this.row.numFields();
        int totSize = this.fixedSize + aSize;
        this.block = this.alloc.allocate(totSize < 64 ? 64 : totSize);
        this.row.pointTo(this.block, this.block.getBaseOffset(), totSize);
    }

    public MemoryBlock getBaseObject() { return this.block; }

    public long getBaseOffset() { return this.block.getBaseOffset(); }

    /**
     * Grows the buffer to at least neededSize. If row is non-null, points the row to the buffer.
     */
    public void grow(int neededSize) {
        final int length = totalSize() + neededSize;
        if (this.block.size() < length) {
            // This will not happen frequently, because the buffer is re-used.
            //TODO: implement reallocate()
            MemoryBlock tmp = this.alloc.allocate( 2*length );
            Platform.copyMemory(
                    this.block,
                    this.block.getBaseOffset(),
                    tmp,
                    tmp.getBaseOffset(),
                    totalSize());
            this.block = tmp;
            if (this.row != null) {
                this.row.pointTo(this.block, this.block.getBaseOffset(), length * 2);
            }
        }
    }

    public void reset() {
        cursor = this.fixedSize;
    }

    public int totalSize() {
        return this.cursor;
    }
}

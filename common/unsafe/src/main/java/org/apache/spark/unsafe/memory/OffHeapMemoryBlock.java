package org.apache.spark.unsafe.memory;

import org.apache.spark.unsafe.Platform;

public class OffHeapMemoryBlock implements MemoryBlock {
    private Object directBuffer;
    private final long address;
    private final long length;
    private int pageNumber = -1;

    public OffHeapMemoryBlock(Object aDirectBuffer, long address, long size) {
        this.address = address;
        this.length = size;
        this.directBuffer = aDirectBuffer;
    }

    @Override
    public Object getBaseObject() {
        return null;
    }

    @Override
    public long getBaseOffset() {
        return this.address;
    }

    @Override
    public long size() {
        return this.length;
    }

    @Override
    public void setPageNumber(int aPageNum) {
        this.pageNumber = aPageNum;
    }

    @Override
    public int getPageNumber() {
        return this.pageNumber;
    }
}

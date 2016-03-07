package org.apache.spark.unsafe.memory;

public interface MemoryBlock {
    Object getBaseObject();
    long getBaseOffset();
    long size();
    void setPageNumber(int aPageNum);
    int getPageNumber();
}

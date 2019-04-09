package org.apache.spark.storage.pmem;

import org.apache.spark.storage.pmem.PmemBuffer;
public class PersistentMemoryPool {
    static {
        System.load("/usr/local/lib/libjnipmdk.so");
    }
    private static native long nativeOpenDevice(String path, int maxStage, int maxMap, long size);
    private static native long nativeSetMapPartition(long deviceHandler, int numPartitions, int stageId, int mapId, int partutionId, long pmemBufferHandler, boolean clean, int numMaps);
    private static native long nativeSetReducePartition(long deviceHandler, int numPartitions, int stageId, int partutionId, long pmemBufferHandler, boolean clean, int numMaps);
    private static native byte[] nativeGetMapPartition(long deviceHandler, int stageId, int mapId, int partitionId);
    private static native byte[] nativeGetReducePartition(long deviceHandler, int stageId, int mapId, int partitionId);
    private static native long[] nativeGetMapPartitionBlockInfo(long deviceHandler, int stageId, int mapId, int partitionId);
    private static native long[] nativeGetReducePartitionBlockInfo(long deviceHandler, int stageId, int mapId, int partitionId);
    private static native long nativeGetMapPartitionSize(long deviceHandler, int stageId, int mapId, int partitionId);
    private static native long nativeGetReducePartitionSize(long deviceHandler, int stageId, int mapId, int partitionId);
    private static native long nativeDeleteMapPartition(long deviceHandler, int stageId, int mapId, int partitionId);
    private static native long nativeDeleteReducePartition(long deviceHandler, int stageId, int mapId, int partitionId);
    private static native long nativeGetRoot(long deviceHandler);
    private static native int nativeCloseDevice(long deviceHandler);
  
    private static final long DEFAULT_PMPOOL_SIZE = 0L;

    private String device;
    private long deviceHandler;

    PersistentMemoryPool(
        String path,
        int max_stages_num,
        int max_shuffles_num,
        long pool_size) {
      this.device = path; 
      pool_size = pool_size == -1 ? DEFAULT_PMPOOL_SIZE : pool_size;
      this.deviceHandler = nativeOpenDevice(path, max_stages_num, max_shuffles_num, pool_size);
    }

    public long setMapPartition(int partitionNum, int stageId, int shuffleId, int partitionId, PmemBuffer buf, boolean set_clean, int numMaps) {
      return nativeSetMapPartition(this.deviceHandler, partitionNum, stageId, shuffleId, partitionId, buf.getNativeObject(), set_clean, numMaps);
    }

    public long setReducePartition(int partitionNum, int stageId, int partitionId, PmemBuffer buf, boolean set_clean, int numMaps) {
        return nativeSetReducePartition(this.deviceHandler, partitionNum, stageId, partitionId, buf.getNativeObject(), set_clean, numMaps);
    }

    public byte[] getMapPartition(int stageId, int shuffleId, int partitionId) {
      return nativeGetMapPartition(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public byte[] getReducePartition(int stageId, int shuffleId, int partitionId) {
      return nativeGetReducePartition(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public long[] getMapPartitionBlockInfo(int stageId, int shuffleId, int partitionId) {
      return nativeGetMapPartitionBlockInfo(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public long[] getReducePartitionBlockInfo(int stageId, int shuffleId, int partitionId) {
      return nativeGetReducePartitionBlockInfo(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public long getMapPartitionSize(int stageId, int shuffleId, int partitionId) {
      return nativeGetMapPartitionSize(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public long getReducePartitionSize(int stageId, int shuffleId, int partitionId) {
      return nativeGetReducePartitionSize(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public long deleteMapPartition(int stageId, int shuffleId, int partitionId) {
      return nativeDeleteMapPartition(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public long deleteReducePartition(int stageId, int shuffleId, int partitionId) {
      return nativeDeleteReducePartition(this.deviceHandler, stageId, shuffleId, partitionId);
    }

    public long getRootAddr() {
        return nativeGetRoot(this.deviceHandler);
    }

    public void close() {
      nativeCloseDevice(this.deviceHandler);
      try {
        System.out.println("Use pmempool to delete device:" + device);
        Runtime.getRuntime().exec("pmempool rm " + device);
      } catch (Exception e) {
        System.err.println("delete " + device + "failed: " + e.getMessage());
      }
    }
}

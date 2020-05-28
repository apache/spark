package org.apache.spark.network.remoteshuffle;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ShuffleStage {
  private final ShuffleStageFqid stageFqid;
  private final String rootDir;
  private final Map<Integer, PartitionWriter> partitionWriters = new HashMap<>();

  public ShuffleStage(ShuffleStageFqid stageFqid, String rootDir) {
    this.stageFqid = stageFqid;
    this.rootDir = rootDir;
  }

  public synchronized void writeTaskData(int partition, ByteBuffer data) {
    PartitionWriter partitionWriter = getPartitionWriter(partition);
    partitionWriter.writeTaskData(data);
  }

  public synchronized void flush() {
    partitionWriters.values().forEach(w->w.flush());
  }

  private PartitionWriter getPartitionWriter(int partition) {
    PartitionWriter partitionWriter = new PartitionWriter(rootDir);
    PartitionWriter oldValue = partitionWriters.putIfAbsent(partition, partitionWriter);
    if (oldValue != null) {
      partitionWriter = oldValue;
    }
    return partitionWriter;
  }
}

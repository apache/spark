package org.apache.spark.network.remoteshuffle;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ShuffleStage {
  private final ShuffleStageFqid shuffleStageFqid;
  private final String rootDir;
  private final Map<Integer, PartitionWriter> partitionWriters = new HashMap<>();
  private final Set<Long> committedTaskAttempts = new HashSet<>();

  public ShuffleStage(ShuffleStageFqid shuffleStageFqid, String rootDir) {
    this.shuffleStageFqid = shuffleStageFqid;
    this.rootDir = rootDir;
  }

  public synchronized void writeTaskData(int partition, long taskAttemptId, ByteBuffer data) {
    if (committedTaskAttempts.contains(taskAttemptId)) {
      throw new RuntimeException(String.format(
          "Task attempt %s in shuffle %s already committed, cannot write data again", taskAttemptId, shuffleStageFqid));
    }

    PartitionWriter partitionWriter = getPartitionWriter(partition);
    partitionWriter.writeTaskData(data);
  }

  public synchronized void commit(long taskAttemptId) {
    if (committedTaskAttempts.contains(taskAttemptId)) {
      throw new RuntimeException(String.format(
          "Task attempt %s in shuffle %s already committed, cannot commit again", taskAttemptId, shuffleStageFqid));
    }

    partitionWriters.values().forEach(w->w.flush());
    committedTaskAttempts.add(taskAttemptId);

    // TODO close files when finish writing all data
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

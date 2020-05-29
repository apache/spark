package org.apache.spark.network.remoteshuffle;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/***
 * This class is an abstraction for a shuffle stage in the shuffle server. It contains
 * multiple {@link PartitionWriter} and forward each shuffle record to corresponding
 * {@link PartitionWriter}. Then {@link PartitionWriter} writes the record to file.
 */
public class ShuffleStage {
  private final ShuffleStageFqid shuffleStageFqid;
  private final String stageRootDir;
  private final Map<Integer, PartitionWriter> partitionWriters = new HashMap<>();
  private final Set<Long> committedTaskAttempts = new HashSet<>();

  public ShuffleStage(ShuffleStageFqid shuffleStageFqid, String rootDir) {
    this.shuffleStageFqid = shuffleStageFqid;
    this.stageRootDir = Paths.get(rootDir,
        shuffleStageFqid.getAppId(),
        shuffleStageFqid.getExecId(),
        String.valueOf(shuffleStageFqid.getShuffleId()),
        String.valueOf(shuffleStageFqid.getStageAttempt())).toString();
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
    PartitionWriter partitionWriter = new PartitionWriter(stageRootDir);
    PartitionWriter oldValue = partitionWriters.putIfAbsent(partition, partitionWriter);
    if (oldValue != null) {
      partitionWriter = oldValue;
    }
    return partitionWriter;
  }
}

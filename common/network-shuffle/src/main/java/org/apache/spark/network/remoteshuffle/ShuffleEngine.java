package org.apache.spark.network.remoteshuffle;

import org.apache.spark.network.remoteshuffle.protocol.StreamRecord;

import java.nio.ByteBuffer;

public class ShuffleEngine {
  private final String rootDir;

  public ShuffleEngine(String rootDir) {
    this.rootDir = rootDir;
  }

  public long createWriteSession(ShuffleStageFqid shuffleStageFqid) {
    return 0L;
  }

  public void writeTaskData(long sessionId, int partition, long taskAttemptId, ByteBuffer data) {
  }

  public void finishTask(long sessionId, long taskAttemptId) {
  }
}

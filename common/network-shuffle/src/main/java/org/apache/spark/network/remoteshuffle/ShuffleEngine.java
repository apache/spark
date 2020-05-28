package org.apache.spark.network.remoteshuffle;

import org.apache.spark.network.remoteshuffle.protocol.StreamRecord;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ShuffleEngine {
  private static AtomicLong sessionIdGenerator = new AtomicLong();

  private final String rootDir;

  // TODO delete entries from following hash map when application finishes
  private final ConcurrentHashMap<ShuffleStageFqid, ShuffleStage> shuffleStages = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Long, ShuffleStage> sessions = new ConcurrentHashMap<>();

  public ShuffleEngine(String rootDir) {
    this.rootDir = rootDir;
  }

  public long createWriteSession(ShuffleStageFqid shuffleStageFqid) {
    ShuffleStage stage = new ShuffleStage(shuffleStageFqid, rootDir);
    ShuffleStage oldValue = shuffleStages.putIfAbsent(shuffleStageFqid, stage);
    if (oldValue != null) {
      stage = oldValue;
    }

    long sessionId = sessionIdGenerator.getAndIncrement();
    sessions.put(sessionId, stage);

    return sessionId;
  }

  public void writeTaskData(long sessionId, int partition, long taskAttemptId, ByteBuffer data) {
    ShuffleStage stage = sessions.get(sessionId);
    stage.writeTaskData(partition, data);
  }

  public void finishTask(long sessionId, long taskAttemptId) {
    ShuffleStage stage = sessions.get(sessionId);
    // TODO optimize to avoid unnecessary flush
    stage.flush();
  }
}

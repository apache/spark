package org.apache.spark.network.remoteshuffle;

import java.nio.ByteBuffer;

public class PartitionWriter {
  private final String rootDir;

  public PartitionWriter(String rootDir) {
    this.rootDir = rootDir;
  }

  public synchronized void writeTaskData(ByteBuffer data) {
  }

  public synchronized void flush() {
  }
}

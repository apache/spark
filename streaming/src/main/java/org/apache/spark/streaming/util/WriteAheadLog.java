package org.apache.spark.streaming.util;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface WriteAheadLog {
  WriteAheadLogSegment write(ByteBuffer record, long time);
  ByteBuffer read(WriteAheadLogSegment segment);
  Iterator<ByteBuffer> readAll();
  void cleanup(long threshTime, boolean waitForCompletion);
  void close();
}

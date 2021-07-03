/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection.unsafe.sort;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.io.ReadAheadInputStream;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockId;
import org.apache.spark.unsafe.Platform;

import java.io.*;

/**
 * Reads spill files written by {@link UnsafeSorterSpillWriter} (see that class for a description
 * of the file format).
 */
public final class UnsafeSorterSpillReader extends UnsafeSorterIterator implements Closeable {
  public static final int MAX_BUFFER_SIZE_BYTES = 16777216; // 16 mb

  private InputStream in;
  private DataInputStream din;

  // Variables that change with every record read:
  private int recordLength;
  private long keyPrefix;
  private int numRecords;
  private int numRecordsRemaining;

  private byte[] arr = new byte[1024 * 1024];
  private Object baseObject = arr;
  private final TaskContext taskContext = TaskContext.get();

  public UnsafeSorterSpillReader(
      SerializerManager serializerManager,
      File file,
      BlockId blockId) throws IOException {
    assert (file.length() > 0);
    final ConfigEntry<Object> bufferSizeConfigEntry =
        package$.MODULE$.UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE();
    // This value must be less than or equal to MAX_BUFFER_SIZE_BYTES. Cast to int is always safe.
    final int DEFAULT_BUFFER_SIZE_BYTES =
        ((Long) bufferSizeConfigEntry.defaultValue().get()).intValue();
    int bufferSizeBytes = SparkEnv.get() == null ? DEFAULT_BUFFER_SIZE_BYTES :
        ((Long) SparkEnv.get().conf().get(bufferSizeConfigEntry)).intValue();

    final boolean readAheadEnabled = SparkEnv.get() != null && (boolean)SparkEnv.get().conf().get(
        package$.MODULE$.UNSAFE_SORTER_SPILL_READ_AHEAD_ENABLED());

    final InputStream bs =
        new NioBufferedFileInputStream(file, bufferSizeBytes);
    try {
      if (readAheadEnabled) {
        this.in = new ReadAheadInputStream(serializerManager.wrapStream(blockId, bs),
                bufferSizeBytes);
      } else {
        this.in = serializerManager.wrapStream(blockId, bs);
      }
      this.din = new DataInputStream(this.in);
      numRecords = numRecordsRemaining = din.readInt();
    } catch (IOException e) {
      Closeables.close(bs, /* swallowIOException = */ true);
      throw e;
    }
  }

  @Override
  public int getNumRecords() {
    return numRecords;
  }

  @Override
  public long getCurrentPageNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNext() {
    return (numRecordsRemaining > 0);
  }

  @Override
  public void loadNext() throws IOException {
    // Kill the task in case it has been marked as killed. This logic is from
    // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
    // to avoid performance overhead. This check is added here in `loadNext()` instead of in
    // `hasNext()` because it's technically possible for the caller to be relying on
    // `getNumRecords()` instead of `hasNext()` to know when to stop.
    if (taskContext != null) {
      taskContext.killTaskIfInterrupted();
    }
    recordLength = din.readInt();
    keyPrefix = din.readLong();
    if (recordLength > arr.length) {
      arr = new byte[recordLength];
      baseObject = arr;
    }
    ByteStreams.readFully(in, arr, 0, recordLength);
    numRecordsRemaining--;
    if (numRecordsRemaining == 0) {
      close();
    }
  }

  @Override
  public Object getBaseObject() {
    return baseObject;
  }

  @Override
  public long getBaseOffset() {
    return Platform.BYTE_ARRAY_OFFSET;
  }

  @Override
  public int getRecordLength() {
    return recordLength;
  }

  @Override
  public long getKeyPrefix() {
    return keyPrefix;
  }

  @Override
  public void close() throws IOException {
   if (in != null) {
     try {
       in.close();
     } finally {
       in = null;
       din = null;
     }
   }
  }
}

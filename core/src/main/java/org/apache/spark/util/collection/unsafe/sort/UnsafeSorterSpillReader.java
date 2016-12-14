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

import java.io.*;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

import org.apache.spark.SparkEnv;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockId;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads spill files written by {@link UnsafeSorterSpillWriter} (see that class for a description
 * of the file format).
 */
public final class UnsafeSorterSpillReader extends UnsafeSorterIterator implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(UnsafeSorterSpillReader.class);
  private static final int DEFAULT_BUFFER_SIZE_BYTES = 1024 * 1024; // 1 MB
  private static final int MAX_BUFFER_SIZE_BYTES = 16777216; // 16 mb

  private InputStream in;
  private DataInputStream din;

  // Variables that change with every record read:
  private int recordLength;
  private long keyPrefix;
  private int numRecords;
  private int numRecordsRemaining;

  private byte[] arr = new byte[1024 * 1024];
  private Object baseObject = arr;
  private final long baseOffset = Platform.BYTE_ARRAY_OFFSET;

  public UnsafeSorterSpillReader(
      SerializerManager serializerManager,
      File file,
      BlockId blockId) throws IOException {
    assert (file.length() > 0);
    long bufferSizeBytes =
        SparkEnv.get() == null ?
            DEFAULT_BUFFER_SIZE_BYTES:
            SparkEnv.get().conf().getSizeAsBytes("spark.unsafe.sorter.spill.reader.buffer.size",
                                                 DEFAULT_BUFFER_SIZE_BYTES);
    if (bufferSizeBytes > MAX_BUFFER_SIZE_BYTES || bufferSizeBytes < DEFAULT_BUFFER_SIZE_BYTES) {
      // fall back to a sane default value
      logger.warn("Value of config \"spark.unsafe.sorter.spill.reader.buffer.size\" = {} not in " +
        "allowed range [{}, {}). Falling back to default value : {} bytes", bufferSizeBytes,
        DEFAULT_BUFFER_SIZE_BYTES, MAX_BUFFER_SIZE_BYTES, DEFAULT_BUFFER_SIZE_BYTES);
      bufferSizeBytes = DEFAULT_BUFFER_SIZE_BYTES;
    }

    final InputStream bs =
        new NioBufferedFileInputStream(file, (int) bufferSizeBytes);
    try {
      this.in = serializerManager.wrapStream(blockId, bs);
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
  public boolean hasNext() {
    return (numRecordsRemaining > 0);
  }

  @Override
  public void loadNext() throws IOException {
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
    return baseOffset;
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

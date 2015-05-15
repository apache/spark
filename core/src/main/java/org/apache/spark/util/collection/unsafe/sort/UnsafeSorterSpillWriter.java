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

import scala.Tuple2;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockObjectWriter;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.unsafe.PlatformDependent;

final class UnsafeSorterSpillWriter {

  private static final int SER_BUFFER_SIZE = 1024 * 1024;  // TODO: tune this
  static final int EOF_MARKER = -1;

  private byte[] arr = new byte[SER_BUFFER_SIZE];

  private final File file;
  private final BlockId blockId;
  private BlockObjectWriter writer;
  private DataOutputStream dos;

  public UnsafeSorterSpillWriter(
      BlockManager blockManager,
      int fileBufferSize,
      ShuffleWriteMetrics writeMetrics) {
    final Tuple2<TempLocalBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempLocalBlock();
    this.file = spilledFileInfo._2();
    this.blockId = spilledFileInfo._1();
    // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.
    writer = blockManager.getDiskWriter(
      blockId, file, DummySerializerInstance.INSTANCE, fileBufferSize, writeMetrics);
    dos = new DataOutputStream(writer);
  }

  public void write(
    Object baseObject,
    long baseOffset,
    int recordLength,
    long keyPrefix) throws IOException {
    dos.writeInt(recordLength);
    dos.writeLong(keyPrefix);
    PlatformDependent.copyMemory(
      baseObject,
      baseOffset + 4,
      arr,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      recordLength);
    writer.write(arr, 0, recordLength);
    // TODO: add a test that detects whether we leave this call out:
    writer.recordWritten();
  }

  public void close() throws IOException {
    dos.writeInt(EOF_MARKER);
    writer.commitAndClose();
    writer = null;
    dos = null;
    arr = null;
  }

  public long numberOfSpilledBytes() {
    return file.length();
  }

  public UnsafeSorterSpillReader getReader(BlockManager blockManager) throws IOException {
    return new UnsafeSorterSpillReader(blockManager, file, blockId);
  }
}

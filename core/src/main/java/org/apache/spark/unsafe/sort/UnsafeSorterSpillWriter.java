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

package org.apache.spark.unsafe.sort;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.JavaSerializerInstance;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockObjectWriter;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.unsafe.PlatformDependent;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.*;
import java.nio.ByteBuffer;

final class UnsafeSorterSpillWriter {

  private static final int SER_BUFFER_SIZE = 1024 * 1024;  // TODO: tune this
  public static final int EOF_MARKER = -1;
  byte[] arr = new byte[SER_BUFFER_SIZE];

  private final File file;
  private final BlockId blockId;
  BlockObjectWriter writer;
  DataOutputStream dos;

  public UnsafeSorterSpillWriter(
      BlockManager blockManager,
      int fileBufferSize,
      ShuffleWriteMetrics writeMetrics) throws IOException {
    final Tuple2<TempLocalBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempLocalBlock();
    this.file = spilledFileInfo._2();
    this.blockId = spilledFileInfo._1();
    // Dummy serializer:
    final SerializerInstance ser = new SerializerInstance() {
      @Override
      public SerializationStream serializeStream(OutputStream s) {
        return new SerializationStream() {
          @Override
          public void flush() {

          }

          @Override
          public <T> SerializationStream writeObject(T t, ClassTag<T> ev1) {
            return null;
          }

          @Override
          public void close() {

          }
        };
      }

      @Override
      public <T> ByteBuffer serialize(T t, ClassTag<T> ev1) {
        return null;
      }

      @Override
      public DeserializationStream deserializeStream(InputStream s) {
        return null;
      }

      @Override
      public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> ev1) {
        return null;
      }

      @Override
      public <T> T deserialize(ByteBuffer bytes, ClassTag<T> ev1) {
        return null;
      }
    };
    writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, writeMetrics);
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
    arr = null;
  }

  public long numberOfSpilledBytes() {
    return file.length();
  }

  public UnsafeSorterSpillReader getReader(BlockManager blockManager) throws IOException {
    return new UnsafeSorterSpillReader(blockManager, file, blockId);
  }
}
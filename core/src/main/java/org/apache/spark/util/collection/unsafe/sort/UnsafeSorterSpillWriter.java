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

import java.io.File;
import java.io.IOException;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.TempLocalBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.internal.config.package$;

/**
 * Spills a list of sorted records to disk. Spill files have the following format:
 *
 *   [# of records (int)] [[len (int)][prefix (long)][data (bytes)]...]
 */
public final class UnsafeSorterSpillWriter {

  private final SparkConf conf = new SparkConf();

  /** The buffer size to use when writing the sorted records to an on-disk file */
  private final int diskWriteBufferSize =
    (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());

  // Small writes to DiskBlockObjectWriter will be fairly inefficient. Since there doesn't seem to
  // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
  // data through a byte array.
  private byte[] writeBuffer = new byte[diskWriteBufferSize];

  private final File file;
  private final BlockId blockId;
  private final int numRecordsToWrite;
  private DiskBlockObjectWriter writer;
  private int numRecordsSpilled = 0;

  public UnsafeSorterSpillWriter(
      BlockManager blockManager,
      int fileBufferSize,
      ShuffleWriteMetrics writeMetrics,
      int numRecordsToWrite) throws IOException {
    final Tuple2<TempLocalBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempLocalBlock();
    this.file = spilledFileInfo._2();
    this.blockId = spilledFileInfo._1();
    this.numRecordsToWrite = numRecordsToWrite;
    // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.
    writer = blockManager.getDiskWriter(
      blockId, file, DummySerializerInstance.INSTANCE, fileBufferSize, writeMetrics);
    // Write the number of records
    writeIntToBuffer(numRecordsToWrite, 0);
    writer.write(writeBuffer, 0, 4);
  }

  // Based on DataOutputStream.writeLong.
  private void writeLongToBuffer(long v, int offset) {
    writeBuffer[offset + 0] = (byte)(v >>> 56);
    writeBuffer[offset + 1] = (byte)(v >>> 48);
    writeBuffer[offset + 2] = (byte)(v >>> 40);
    writeBuffer[offset + 3] = (byte)(v >>> 32);
    writeBuffer[offset + 4] = (byte)(v >>> 24);
    writeBuffer[offset + 5] = (byte)(v >>> 16);
    writeBuffer[offset + 6] = (byte)(v >>>  8);
    writeBuffer[offset + 7] = (byte)(v >>>  0);
  }

  // Based on DataOutputStream.writeInt.
  private void writeIntToBuffer(int v, int offset) {
    writeBuffer[offset + 0] = (byte)(v >>> 24);
    writeBuffer[offset + 1] = (byte)(v >>> 16);
    writeBuffer[offset + 2] = (byte)(v >>>  8);
    writeBuffer[offset + 3] = (byte)(v >>>  0);
  }

  /**
   * Write a record to a spill file.
   *
   * @param baseObject the base object / memory page containing the record
   * @param baseOffset the base offset which points directly to the record data.
   * @param recordLength the length of the record.
   * @param keyPrefix a sort key prefix
   */
  public void write(
      Object baseObject,
      long baseOffset,
      int recordLength,
      long keyPrefix) throws IOException {
    if (numRecordsSpilled == numRecordsToWrite) {
      throw new IllegalStateException(
        "Number of records written exceeded numRecordsToWrite = " + numRecordsToWrite);
    } else {
      numRecordsSpilled++;
    }
    writeIntToBuffer(recordLength, 0);
    writeLongToBuffer(keyPrefix, 4);
    int dataRemaining = recordLength;
    int freeSpaceInWriteBuffer = diskWriteBufferSize - 4 - 8; // space used by prefix + len
    long recordReadPosition = baseOffset;
    while (dataRemaining > 0) {
      final int toTransfer = Math.min(freeSpaceInWriteBuffer, dataRemaining);
      Platform.copyMemory(
        baseObject,
        recordReadPosition,
        writeBuffer,
        Platform.BYTE_ARRAY_OFFSET + (diskWriteBufferSize - freeSpaceInWriteBuffer),
        toTransfer);
      writer.write(writeBuffer, 0, (diskWriteBufferSize - freeSpaceInWriteBuffer) + toTransfer);
      recordReadPosition += toTransfer;
      dataRemaining -= toTransfer;
      freeSpaceInWriteBuffer = diskWriteBufferSize;
    }
    if (freeSpaceInWriteBuffer < diskWriteBufferSize) {
      writer.write(writeBuffer, 0, (diskWriteBufferSize - freeSpaceInWriteBuffer));
    }
    writer.recordWritten();
  }

  public void close() throws IOException {
    writer.commitAndGet();
    writer.close();
    writer = null;
    writeBuffer = null;
  }

  public File getFile() {
    return file;
  }

  public UnsafeSorterSpillReader getReader(SerializerManager serializerManager) throws IOException {
    return new UnsafeSorterSpillReader(serializerManager, file, blockId);
  }

  public int recordsSpilled() {
    return numRecordsSpilled;
  }
}

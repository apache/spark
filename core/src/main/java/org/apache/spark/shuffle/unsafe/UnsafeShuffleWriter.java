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

package org.apache.spark.shuffle.unsafe;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import scala.Option;
import scala.Product2;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockManager;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private final Logger logger = LoggerFactory.getLogger(UnsafeShuffleWriter.class);

  private static final int SER_BUFFER_SIZE = 1024 * 1024;  // TODO: tune this
  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  private final BlockManager blockManager;
  private final IndexShuffleBlockManager shuffleBlockManager;
  private final TaskMemoryManager memoryManager;
  private final ShuffleMemoryManager shuffleMemoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;

  private MapStatus mapStatus = null;
  private UnsafeShuffleExternalSorter sorter = null;
  private byte[] serArray = null;
  private ByteBuffer serByteBuffer;
  // TODO: we should not depend on this class from Kryo; copy its source or find an alternative
  private SerializationStream serOutputStream;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  public UnsafeShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockManager shuffleBlockManager,
      TaskMemoryManager memoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      UnsafeShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf sparkConf) {
    this.blockManager = blockManager;
    this.shuffleBlockManager = shuffleBlockManager;
    this.memoryManager = memoryManager;
    this.shuffleMemoryManager = shuffleMemoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = Serializer.getSerializer(dep.serializer()).newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = new ShuffleWriteMetrics();
    taskContext.taskMetrics().shuffleWriteMetrics_$eq(Option.apply(writeMetrics));
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
  }

  public void write(Iterator<Product2<K, V>> records) {
    write(JavaConversions.asScalaIterator(records));
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) {
    try {
      while (records.hasNext()) {
        insertRecordIntoSorter(records.next());
      }
      closeAndWriteOutput();
    } catch (Exception e) {
      PlatformDependent.throwException(e);
    }
  }

  private void open() throws IOException {
    assert (sorter == null);
    sorter = new UnsafeShuffleExternalSorter(
      memoryManager,
      shuffleMemoryManager,
      blockManager,
      taskContext,
      4096, // Initial size (TODO: tune this!)
      partitioner.numPartitions(),
      sparkConf);
    serArray = new byte[SER_BUFFER_SIZE];
    serByteBuffer = ByteBuffer.wrap(serArray);
    // TODO: we should not depend on this class from Kryo; copy its source or find an alternative
    serOutputStream = serializer.serializeStream(new ByteBufferOutputStream(serByteBuffer));
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    if (sorter == null) {
      open();
    }
    serArray = null;
    serByteBuffer = null;
    serOutputStream = null;
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    sorter = null;
    final long[] partitionLengths;
    try {
      partitionLengths = mergeSpills(spills);
    } finally {
      for (SpillInfo spill : spills) {
        if (spill.file.exists() && ! spill.file.delete()) {
          logger.error("Error while deleting spill file {}", spill.file.getPath());
        }
      }
    }
    shuffleBlockManager.writeIndexFile(shuffleId, mapId, partitionLengths);
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException{
    if (sorter == null) {
      open();
    }
    final K key = record._1();
    final int partitionId = partitioner.getPartition(key);
    serByteBuffer.position(0);
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();

    final int serializedRecordSize = serByteBuffer.position();
    assert (serializedRecordSize > 0);

    sorter.insertRecord(
      serArray, PlatformDependent.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  private long[] mergeSpills(SpillInfo[] spills) throws IOException {
    final File outputFile = shuffleBlockManager.getDataFile(shuffleId, mapId);
    try {
      if (spills.length == 0) {
        new FileOutputStream(outputFile).close(); // Create an empty file
        return new long[partitioner.numPartitions()];
      } else if (spills.length == 1) {
        // Note: we'll have to watch out for corner-cases in this code path when working on shuffle
        // metrics integration, since any metrics updates that are performed during the merge will
        // also have to be done here. In this branch, the shuffle technically didn't need to spill
        // because we're only trying to merge one file, so we may need to ensure that metrics that
        // would otherwise be counted as spill metrics are actually counted as regular write
        // metrics.
        Files.move(spills[0].file, outputFile);
        return spills[0].partitionLengths;
      } else {
        // Need to merge multiple spills.
        if (transferToEnabled) {
          return mergeSpillsWithTransferTo(spills, outputFile);
        } else {
          return mergeSpillsWithFileStream(spills, outputFile);
        }
      }
    } catch (IOException e) {
      if (outputFile.exists() && !outputFile.delete()) {
        logger.error("Unable to delete output file {}", outputFile.getPath());
      }
      throw e;
    }
  }

  private long[] mergeSpillsWithFileStream(SpillInfo[] spills, File outputFile) throws IOException {
    final int numPartitions = partitioner.numPartitions();
    final long[] partitionLengths = new long[numPartitions];
    final FileInputStream[] spillInputStreams = new FileInputStream[spills.length];
    FileOutputStream mergedFileOutputStream = null;

    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputStreams[i] = new FileInputStream(spills[i].file);
      }
      mergedFileOutputStream = new FileOutputStream(outputFile);

      for (int partition = 0; partition < numPartitions; partition++) {
        for (int i = 0; i < spills.length; i++) {
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          final FileInputStream spillInputStream = spillInputStreams[i];
          ByteStreams.copy
            (new LimitedInputStream(spillInputStream, partitionLengthInSpill),
              mergedFileOutputStream);
          partitionLengths[partition] += partitionLengthInSpill;
        }
      }
    } finally {
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, false);
      }
      Closeables.close(mergedFileOutputStream, false);
    }
    return partitionLengths;
  }

  private long[] mergeSpillsWithTransferTo(SpillInfo[] spills, File outputFile) throws IOException {
    final int numPartitions = partitioner.numPartitions();
    final long[] partitionLengths = new long[numPartitions];
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    final long[] spillInputChannelPositions = new long[spills.length];
    FileChannel mergedFileOutputChannel = null;

    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
      }
      // This file needs to opened in append mode in order to work around a Linux kernel bug that
      // affects transferTo; see SPARK-3948 for more details.
      mergedFileOutputChannel = new FileOutputStream(outputFile, true).getChannel();

      long bytesWrittenToMergedFile = 0;
      for (int partition = 0; partition < numPartitions; partition++) {
        for (int i = 0; i < spills.length; i++) {
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          long bytesToTransfer = partitionLengthInSpill;
          final FileChannel spillInputChannel = spillInputChannels[i];
          while (bytesToTransfer > 0) {
            final long actualBytesTransferred = spillInputChannel.transferTo(
              spillInputChannelPositions[i],
              bytesToTransfer,
              mergedFileOutputChannel);
            spillInputChannelPositions[i] += actualBytesTransferred;
            bytesToTransfer -= actualBytesTransferred;
          }
          bytesWrittenToMergedFile += partitionLengthInSpill;
          partitionLengths[partition] += partitionLengthInSpill;
        }
      }
      // Check the position after transferTo loop to see if it is in the right position and raise an
      // exception if it is incorrect. The position will not be increased to the expected length
      // after calling transferTo in kernel version 2.6.32. This issue is described at
      // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
      if (mergedFileOutputChannel.position() != bytesWrittenToMergedFile) {
        throw new IOException(
          "Current position " + mergedFileOutputChannel.position() + " does not equal expected " +
            "position " + bytesWrittenToMergedFile + " after transferTo. Please check your kernel" +
            " version to see if it is 2.6.32, as there is a kernel bug which will lead to " +
            "unexpected behavior when using transferTo. You can set spark.file.transferTo=false " +
            "to disable this NIO feature."
        );
      }
    } finally {
      for (int i = 0; i < spills.length; i++) {
        assert(spillInputChannelPositions[i] == spills[i].file.length());
        Closeables.close(spillInputChannels[i], false);
      }
      Closeables.close(mergedFileOutputChannel, false);
    }
    return partitionLengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          // The map task failed, so delete our output data.
          shuffleBlockManager.removeDataByMap(shuffleId, mapId);
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        sorter.cleanupAfterError();
      }
    }
  }
}

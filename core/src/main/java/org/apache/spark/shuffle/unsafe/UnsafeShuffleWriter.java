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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import org.apache.spark.shuffle.ShuffleMemoryManager;
import scala.Option;
import scala.Product2;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.esotericsoftware.kryo.io.ByteBufferOutputStream;

import org.apache.spark.*;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockManager;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

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

  private MapStatus mapStatus = null;

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
  }

  public void write(Iterator<Product2<K, V>> records) {
    write(JavaConversions.asScalaIterator(records));
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) {
    try {
      final long[] partitionLengths = mergeSpills(insertRecordsIntoSorter(records));
      shuffleBlockManager.writeIndexFile(shuffleId, mapId, partitionLengths);
      mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
    } catch (Exception e) {
      PlatformDependent.throwException(e);
    }
  }

  private void freeMemory() {
    // TODO
  }

  private SpillInfo[] insertRecordsIntoSorter(
      scala.collection.Iterator<? extends Product2<K, V>> records) throws Exception {
    final UnsafeShuffleExternalSorter sorter = new UnsafeShuffleExternalSorter(
      memoryManager,
      shuffleMemoryManager,
      blockManager,
      taskContext,
      4096, // Initial size (TODO: tune this!)
      partitioner.numPartitions(),
      sparkConf);

    final byte[] serArray = new byte[SER_BUFFER_SIZE];
    final ByteBuffer serByteBuffer = ByteBuffer.wrap(serArray);
    // TODO: we should not depend on this class from Kryo; copy its source or find an alternative
    final SerializationStream serOutputStream =
      serializer.serializeStream(new ByteBufferOutputStream(serByteBuffer));

    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
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

    return sorter.closeAndGetSpills();
  }

  private long[] mergeSpills(SpillInfo[] spills) throws IOException {
    final File outputFile = shuffleBlockManager.getDataFile(shuffleId, mapId);
    final int numPartitions = partitioner.numPartitions();
    final long[] partitionLengths = new long[numPartitions];

    if (spills.length == 0) {
      new FileOutputStream(outputFile).close();
      return partitionLengths;
    }

    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    final long[] spillInputChannelPositions = new long[spills.length];

    // TODO: We need to add an option to bypass transferTo here since older Linux kernels are
    // affected by a bug here that can lead to data truncation; see the comments Utils.scala,
    // in the copyStream() method. I didn't use copyStream() here because we only want to copy
    // a limited number of bytes from the stream and I didn't want to modify / extend that method
    // to accept a length.

    // TODO: special case optimization for case where we only write one file (non-spill case).

    for (int i = 0; i < spills.length; i++) {
      spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
    }

    final FileChannel mergedFileOutputChannel = new FileOutputStream(outputFile).getChannel();

    for (int partition = 0; partition < numPartitions; partition++) {
      for (int i = 0; i < spills.length; i++) {
        final long partitionLengthInSpill = spills[i].partitionLengths[partition];
        long bytesRemainingToBeTransferred = partitionLengthInSpill;
        final FileChannel spillInputChannel = spillInputChannels[i];
        while (bytesRemainingToBeTransferred > 0) {
          final long actualBytesTransferred = spillInputChannel.transferTo(
              spillInputChannelPositions[i],
              bytesRemainingToBeTransferred,
              mergedFileOutputChannel);
          spillInputChannelPositions[i] += actualBytesTransferred;
          bytesRemainingToBeTransferred -= actualBytesTransferred;
        }
        partitionLengths[partition] += partitionLengthInSpill;
      }
    }

    // TODO: should this be in a finally block?
    for (int i = 0; i < spills.length; i++) {
      assert(spillInputChannelPositions[i] == spills[i].file.length());
      spillInputChannels[i].close();
    }
    mergedFileOutputChannel.close();

    return partitionLengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        freeMemory();
        if (success) {
          return Option.apply(mapStatus);
        } else {
          // The map task failed, so delete our output data.
          shuffleBlockManager.removeDataByMap(shuffleId, mapId);
          return Option.apply(null);
        }
      }
    } finally {
      freeMemory();
    }
  }
}

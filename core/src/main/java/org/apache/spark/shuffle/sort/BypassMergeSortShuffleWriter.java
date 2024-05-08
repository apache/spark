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

package org.apache.spark.shuffle.sort;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.zip.Checksum;
import javax.annotation.Nullable;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.io.Closeables;

import org.apache.spark.internal.Logger;
import org.apache.spark.internal.LoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.shuffle.checksum.ShuffleChecksumSupport;
import org.apache.spark.internal.config.package$;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;

/**
 * This class implements sort-based shuffle's hash-style shuffle fallback path. This write path
 * writes incoming records to separate files, one file per reduce partition, then concatenates these
 * per-partition files to form a single output file, regions of which are served to reducers.
 * Records are not buffered in memory. It writes output in a format
 * that can be served / consumed via {@link org.apache.spark.shuffle.IndexShuffleBlockResolver}.
 * <p>
 * This write path is inefficient for shuffles with large numbers of reduce partitions because it
 * simultaneously opens separate serializers and file streams for all partitions. As a result,
 * {@link SortShuffleManager} only selects this write path when
 * <ul>
 *    <li>no map-side combine is specified, and</li>
 *    <li>the number of partitions is less than or equal to
 *      <code>spark.shuffle.sort.bypassMergeThreshold</code>.</li>
 * </ul>
 *
 * This code used to be part of {@link org.apache.spark.util.collection.ExternalSorter} but was
 * refactored into its own class in order to reduce code complexity; see SPARK-7855 for details.
 * <p>
 * There have been proposals to completely remove this code path; see SPARK-6026 for details.
 */
final class BypassMergeSortShuffleWriter<K, V>
  extends ShuffleWriter<K, V>
  implements ShuffleChecksumSupport {

  private static final Logger logger = LoggerFactory.getLogger(BypassMergeSortShuffleWriter.class);

  private final int fileBufferSize;
  private final boolean transferToEnabled;
  private final int numPartitions;
  private final BlockManager blockManager;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final int shuffleId;
  private final long mapId;
  private final Serializer serializer;
  private final ShuffleExecutorComponents shuffleExecutorComponents;

  /** Array of file writers, one for each partition */
  private DiskBlockObjectWriter[] partitionWriters;
  private FileSegment[] partitionWriterSegments;
  @Nullable private MapStatus mapStatus;
  private long[] partitionLengths;
  /** Checksum calculator for each partition. Empty when shuffle checksum disabled. */
  private final Checksum[] partitionChecksums;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  BypassMergeSortShuffleWriter(
      BlockManager blockManager,
      BypassMergeSortShuffleHandle<K, V> handle,
      long mapId,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleExecutorComponents shuffleExecutorComponents) throws SparkException {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSize = (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.transferToEnabled = (boolean) conf.get(package$.MODULE$.SHUFFLE_MERGE_PREFER_NIO());
    this.blockManager = blockManager;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.mapId = mapId;
    this.shuffleId = dep.shuffleId();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = writeMetrics;
    this.serializer = dep.serializer();
    this.shuffleExecutorComponents = shuffleExecutorComponents;
    this.partitionChecksums = createPartitionChecksums(numPartitions, conf);
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    ShuffleMapOutputWriter mapOutputWriter = shuffleExecutorComponents
        .createMapOutputWriter(shuffleId, mapId, numPartitions);
    try {
      if (!records.hasNext()) {
        partitionLengths = mapOutputWriter.commitAllPartitions(
          ShuffleChecksumHelper.EMPTY_CHECKSUM_VALUE).getPartitionLengths();
        mapStatus = MapStatus$.MODULE$.apply(
          blockManager.shuffleServerId(), partitionLengths, mapId);
        return;
      }
      final SerializerInstance serInstance = serializer.newInstance();
      final long openStartTime = System.nanoTime();
      partitionWriters = new DiskBlockObjectWriter[numPartitions];
      partitionWriterSegments = new FileSegment[numPartitions];
      for (int i = 0; i < numPartitions; i++) {
        final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
            blockManager.diskBlockManager().createTempShuffleBlock();
        final File file = tempShuffleBlockIdPlusFile._2();
        final BlockId blockId = tempShuffleBlockIdPlusFile._1();
        DiskBlockObjectWriter writer =
          blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
        if (partitionChecksums.length > 0) {
          writer.setChecksum(partitionChecksums[i]);
        }
        partitionWriters[i] = writer;
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, and can take a long time in aggregate when we open many files, so should be
      // included in the shuffle write time.
      writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

      while (records.hasNext()) {
        final Product2<K, V> record = records.next();
        final K key = record._1();
        partitionWriters[partitioner.getPartition(key)].write(key, record._2());
      }

      for (int i = 0; i < numPartitions; i++) {
        try (DiskBlockObjectWriter writer = partitionWriters[i]) {
          partitionWriterSegments[i] = writer.commitAndGet();
        }
      }

      partitionLengths = writePartitionedData(mapOutputWriter);
      mapStatus = MapStatus$.MODULE$.apply(
        blockManager.shuffleServerId(), partitionLengths, mapId);
    } catch (Exception e) {
      try {
        mapOutputWriter.abort(e);
      } catch (Exception e2) {
        logger.error("Failed to abort the writer after failing to write map output.", e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
  }

  @Override
  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private long[] writePartitionedData(ShuffleMapOutputWriter mapOutputWriter) throws IOException {
    // Track location of the partition starts in the output file
    if (partitionWriters != null) {
      final long writeStartTime = System.nanoTime();
      try {
        for (int i = 0; i < numPartitions; i++) {
          final File file = partitionWriterSegments[i].file();
          ShufflePartitionWriter writer = mapOutputWriter.getPartitionWriter(i);
          if (file.exists()) {
            if (transferToEnabled) {
              // Using WritableByteChannelWrapper to make resource closing consistent between
              // this implementation and UnsafeShuffleWriter.
              Optional<WritableByteChannelWrapper> maybeOutputChannel = writer.openChannelWrapper();
              if (maybeOutputChannel.isPresent()) {
                writePartitionedDataWithChannel(file, maybeOutputChannel.get());
              } else {
                writePartitionedDataWithStream(file, writer);
              }
            } else {
              writePartitionedDataWithStream(file, writer);
            }
            if (!file.delete()) {
              logger.error("Unable to delete file for partition {}",
                MDC.of(LogKeys.PARTITION_ID$.MODULE$, i));
            }
          }
        }
      } finally {
        writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
      }
      partitionWriters = null;
    }
    return mapOutputWriter.commitAllPartitions(getChecksumValues(partitionChecksums))
      .getPartitionLengths();
  }

  private void writePartitionedDataWithChannel(
      File file,
      WritableByteChannelWrapper outputChannel) throws IOException {
    boolean copyThrewException = true;
    try {
      FileInputStream in = new FileInputStream(file);
      try (FileChannel inputChannel = in.getChannel()) {
        Utils.copyFileStreamNIO(
            inputChannel, outputChannel.channel(), 0L, inputChannel.size());
        copyThrewException = false;
      } finally {
        Closeables.close(in, copyThrewException);
      }
    } finally {
      Closeables.close(outputChannel, copyThrewException);
    }
  }

  private void writePartitionedDataWithStream(File file, ShufflePartitionWriter writer)
      throws IOException {
    boolean copyThrewException = true;
    FileInputStream in = new FileInputStream(file);
    OutputStream outputStream;
    try {
      outputStream = writer.openStream();
      try {
        Utils.copyStream(in, outputStream, false, false);
        copyThrewException = false;
      } finally {
        Closeables.close(outputStream, copyThrewException);
      }
    } finally {
      Closeables.close(in, copyThrewException);
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // The map task failed, so delete our output data.
        if (partitionWriters != null) {
          try {
            for (DiskBlockObjectWriter writer : partitionWriters) {
              // This method explicitly does _not_ throw exceptions:
              writer.closeAndDelete();
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}

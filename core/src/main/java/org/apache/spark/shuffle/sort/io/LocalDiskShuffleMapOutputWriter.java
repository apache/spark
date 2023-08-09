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

package org.apache.spark.shuffle.sort.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.internal.config.package$;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage;

/**
 * Implementation of {@link ShuffleMapOutputWriter} that replicates the functionality of shuffle
 * persisting shuffle data to local disk alongside index files, identical to Spark's historic
 * canonical shuffle storage mechanism.
 */
public class LocalDiskShuffleMapOutputWriter implements ShuffleMapOutputWriter {

  private static final Logger log =
    LoggerFactory.getLogger(LocalDiskShuffleMapOutputWriter.class);

  private final int shuffleId;
  private final long mapId;
  private final IndexShuffleBlockResolver blockResolver;
  private final long[] partitionLengths;
  private final int bufferSize;
  private int lastPartitionId = -1;
  private long currChannelPosition;
  private long bytesWrittenToMergedFile = 0L;

  private final File outputFile;
  private File outputTempFile;
  private FileOutputStream outputFileStream;
  private FileChannel outputFileChannel;
  private BufferedOutputStream outputBufferedFileStream;

  public LocalDiskShuffleMapOutputWriter(
      int shuffleId,
      long mapId,
      int numPartitions,
      IndexShuffleBlockResolver blockResolver,
      SparkConf sparkConf) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.blockResolver = blockResolver;
    this.bufferSize =
      (int) (long) sparkConf.get(
        package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    this.partitionLengths = new long[numPartitions];
    this.outputFile = blockResolver.getDataFile(shuffleId, mapId);
    this.outputTempFile = null;
  }

  @Override
  public ShufflePartitionWriter getPartitionWriter(int reducePartitionId) throws IOException {
    if (reducePartitionId <= lastPartitionId) {
      throw new IllegalArgumentException("Partitions should be requested in increasing order.");
    }
    lastPartitionId = reducePartitionId;
    if (outputTempFile == null) {
      outputTempFile = blockResolver.createTempFile(outputFile);
    }
    if (outputFileChannel != null) {
      currChannelPosition = outputFileChannel.position();
    } else {
      currChannelPosition = 0L;
    }
    return new LocalDiskShufflePartitionWriter(reducePartitionId);
  }

  @Override
  public MapOutputCommitMessage commitAllPartitions(long[] checksums) throws IOException {
    // Check the position after transferTo loop to see if it is in the right position and raise a
    // exception if it is incorrect. The position will not be increased to the expected length
    // after calling transferTo in kernel version 2.6.32. This issue is described at
    // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
    if (outputFileChannel != null && outputFileChannel.position() != bytesWrittenToMergedFile) {
      throw new IOException(
          "Current position " + outputFileChannel.position() + " does not equal expected " +
              "position " + bytesWrittenToMergedFile + " after transferTo. Please check your " +
              " kernel version to see if it is 2.6.32, as there is a kernel bug which will lead " +
              "to unexpected behavior when using transferTo. You can set " +
              "spark.file.transferTo=false to disable this NIO feature.");
    }
    cleanUp();
    File resolvedTmp = outputTempFile != null && outputTempFile.isFile() ? outputTempFile : null;
    log.debug("Writing shuffle index file for mapId {} with length {}", mapId,
        partitionLengths.length);
    blockResolver
      .writeMetadataFileAndCommit(shuffleId, mapId, partitionLengths, checksums, resolvedTmp);
    return MapOutputCommitMessage.of(partitionLengths);
  }

  @Override
  public void abort(Throwable error) throws IOException {
    cleanUp();
    if (outputTempFile != null && outputTempFile.exists() && !outputTempFile.delete()) {
      log.warn("Failed to delete temporary shuffle file at {}", outputTempFile.getAbsolutePath());
    }
  }

  private void cleanUp() throws IOException {
    if (outputBufferedFileStream != null) {
      outputBufferedFileStream.close();
    }
    if (outputFileChannel != null) {
      outputFileChannel.close();
    }
    if (outputFileStream != null) {
      outputFileStream.close();
    }
  }

  private void initStream() throws IOException {
    if (outputFileStream == null) {
      outputFileStream = new FileOutputStream(outputTempFile, true);
    }
    if (outputBufferedFileStream == null) {
      outputBufferedFileStream = new BufferedOutputStream(outputFileStream, bufferSize);
    }
  }

  private void initChannel() throws IOException {
    // This file needs to opened in append mode in order to work around a Linux kernel bug that
    // affects transferTo; see SPARK-3948 for more details.
    if (outputFileChannel == null) {
      outputFileChannel = new FileOutputStream(outputTempFile, true).getChannel();
    }
  }

  private class LocalDiskShufflePartitionWriter implements ShufflePartitionWriter {

    private final int partitionId;
    private PartitionWriterStream partStream = null;
    private PartitionWriterChannel partChannel = null;

    private LocalDiskShufflePartitionWriter(int partitionId) {
      this.partitionId = partitionId;
    }

    @Override
    public OutputStream openStream() throws IOException {
      if (partStream == null) {
        if (outputFileChannel != null) {
          throw new IllegalStateException("Requested an output channel for a previous write but" +
              " now an output stream has been requested. Should not be using both channels" +
              " and streams to write.");
        }
        initStream();
        partStream = new PartitionWriterStream(partitionId);
      }
      return partStream;
    }

    @Override
    public Optional<WritableByteChannelWrapper> openChannelWrapper() throws IOException {
      if (partChannel == null) {
        if (partStream != null) {
          throw new IllegalStateException("Requested an output stream for a previous write but" +
              " now an output channel has been requested. Should not be using both channels" +
              " and streams to write.");
        }
        initChannel();
        partChannel = new PartitionWriterChannel(partitionId);
      }
      return Optional.of(partChannel);
    }

    @Override
    public long getNumBytesWritten() {
      if (partChannel != null) {
        try {
          return partChannel.getCount();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else if (partStream != null) {
        return partStream.getCount();
      } else {
        // Assume an empty partition if stream and channel are never created
        return 0;
      }
    }
  }

  private class PartitionWriterStream extends OutputStream {
    private final int partitionId;
    private long count = 0;
    private boolean isClosed = false;

    PartitionWriterStream(int partitionId) {
      this.partitionId = partitionId;
    }

    public long getCount() {
      return count;
    }

    @Override
    public void write(int b) throws IOException {
      verifyNotClosed();
      outputBufferedFileStream.write(b);
      count++;
    }

    @Override
    public void write(byte[] buf, int pos, int length) throws IOException {
      verifyNotClosed();
      outputBufferedFileStream.write(buf, pos, length);
      count += length;
    }

    @Override
    public void close() {
      isClosed = true;
      partitionLengths[partitionId] = count;
      bytesWrittenToMergedFile += count;
    }

    private void verifyNotClosed() {
      if (isClosed) {
        throw new IllegalStateException("Attempting to write to a closed block output stream.");
      }
    }
  }

  private class PartitionWriterChannel implements WritableByteChannelWrapper {

    private final int partitionId;

    PartitionWriterChannel(int partitionId) {
      this.partitionId = partitionId;
    }

    public long getCount() throws IOException {
      long writtenPosition = outputFileChannel.position();
      return writtenPosition - currChannelPosition;
    }

    @Override
    public WritableByteChannel channel() {
      return outputFileChannel;
    }

    @Override
    public void close() throws IOException {
      partitionLengths[partitionId] = getCount();
      bytesWrittenToMergedFile += partitionLengths[partitionId];
    }
  }
}

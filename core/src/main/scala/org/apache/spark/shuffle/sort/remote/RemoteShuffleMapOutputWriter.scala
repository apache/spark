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

package org.apache.spark.shuffle.sort.remote

import java.io.{BufferedOutputStream, IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.util.Optional

import org.apache.hadoop.fs.FSDataOutputStream

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.REMOTE_SHUFFLE_BUFFER_SIZE
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter, WritableByteChannelWrapper}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage
import org.apache.spark.storage.{RemoteShuffleStorage, ShuffleChecksumBlockId, ShuffleDataBlockId}


/** Implements the ShuffleMapOutputWriter interface.
 *  It stores the shuffle output in one shuffle block.
  *
  * This file is based on Spark "LocalDiskShuffleMapOutputWriter.java".
  */

class RemoteShuffleMapOutputWriter(
    conf: SparkConf,
    shuffleId: Int,
    mapId: Long,
    numPartitions: Int
) extends ShuffleMapOutputWriter
    with Logging {

  /* Target block for writing */

  private var stream: FSDataOutputStream = _
  private var bufferedStream: OutputStream = _
  private var bufferedStreamAsChannel: WritableByteChannel = _
  private var reduceIdStreamPosition: Long = 0

  def initStream(): Unit = {
    if (stream == null) {
      val shuffleBlock = ShuffleDataBlockId(shuffleId, mapId, TaskContext.getPartitionId())
      stream = RemoteShuffleStorage.getStream(shuffleBlock)
      val bufferSize: Long = conf.getSizeAsBytes(REMOTE_SHUFFLE_BUFFER_SIZE.key, "64M")
      bufferedStream = new BufferedOutputStream(stream, bufferSize.toInt)
    }
  }

  def initChannel(): Unit = {
    if (bufferedStreamAsChannel == null) {
      initStream()
      bufferedStreamAsChannel = Channels.newChannel(bufferedStream)
    }
  }

  private val partitionLengths = Array.fill[Long](numPartitions)(0)
  private var totalBytesWritten: Long = 0

  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = {
    if (bufferedStream != null) {
      bufferedStream.flush()
    }
    if (stream != null) {
      stream.flush()
      reduceIdStreamPosition = stream.getPos
    }
    new RemoteShufflePartitionWriter(reducePartitionId)
  }

  /**
   * Close all writers and the shuffle block.
   */
  override def commitAllPartitions(checksums: Array[Long]): MapOutputCommitMessage = {
    if (bufferedStream != null) {
      bufferedStream.flush()
    }
    if (stream != null) {
      if (stream.getPos != totalBytesWritten) {
        throw new RuntimeException(
          f"S3ShuffleMapOutputWriter: Unexpected output length ${stream.getPos}," +
            f" expected: $totalBytesWritten."
        )
      }
    }
    if (bufferedStreamAsChannel != null) {
      bufferedStreamAsChannel.close()
    }
    if (bufferedStream != null) {
      // Closes the underlying stream as well!
      bufferedStream.close()
    }

    // Write checksum.
    if (SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ENABLED)) {
      RemoteShuffleStorage.writeCheckSum(ShuffleChecksumBlockId(shuffleId = shuffleId,
        mapId = mapId, reduceId = 0), checksums)
    }
    MapOutputCommitMessage.of(partitionLengths)
  }

  override def abort(error: Throwable): Unit = {
    cleanUp()
  }

  private def cleanUp(): Unit = {
    if (bufferedStreamAsChannel != null) {
      bufferedStreamAsChannel.close()
    }
    if (bufferedStream != null) {
      bufferedStream.close()
    }
    if (stream != null) {
      stream.close()
    }
  }

  private class RemoteShufflePartitionWriter(reduceId: Int) extends ShufflePartitionWriter
    with Logging {
    private var partitionStream: RemoteShuffleOutputStream = _
    private var partitionChannel: RemoteShufflePartitionWriterChannel = _

    override def openStream(): OutputStream = {
      initStream()
      if (partitionStream == null) {
        partitionStream = new RemoteShuffleOutputStream(reduceId)
      }
      partitionStream
    }

    override def openChannelWrapper(): Optional[WritableByteChannelWrapper] = {
      if (partitionChannel == null) {
        initChannel()
        partitionChannel = new RemoteShufflePartitionWriterChannel(reduceId)
      }
      Optional.of(partitionChannel)
    }

    override def getNumBytesWritten: Long = {
      if (partitionChannel != null) {
        return partitionChannel.numBytesWritten
      }
      if (partitionStream != null) {
        return partitionStream.numBytesWritten
      }
      // The partition is empty.
      0
    }
  }

  private class RemoteShuffleOutputStream(reduceId: Int) extends OutputStream {
    private var byteCount: Long = 0
    private var isClosed = false

    def numBytesWritten: Long = byteCount

    override def write(b: Int): Unit = {
      if (isClosed) {
        throw new IOException("RemoteShuffleOutputStream is already closed.")
      }
      bufferedStream.write(b)
      byteCount += 1
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      if (isClosed) {
        throw new IOException("RemoteShuffleOutputStream is already closed.")
      }
      bufferedStream.write(b, off, len)
      byteCount += len
    }

    override def flush(): Unit = {
      if (isClosed) {
        throw new IOException("RemoteShuffleOutputStream is already closed.")
      }
      bufferedStream.flush()
    }

    override def close(): Unit = {
      partitionLengths(reduceId) = byteCount
      totalBytesWritten += byteCount
      isClosed = true
    }
  }

  private class RemoteShufflePartitionWriterChannel(reduceId: Int)
    extends WritableByteChannelWrapper {
    private val partChannel = new RemotePartitionWritableByteChannel(bufferedStreamAsChannel)

    override def channel(): WritableByteChannel = {
      partChannel
    }

    def numBytesWritten: Long = {
      partChannel.numBytesWritten()
    }

    override def close(): Unit = {
      partitionLengths(reduceId) = numBytesWritten
      totalBytesWritten += numBytesWritten
    }
  }

  private class RemotePartitionWritableByteChannel(channel: WritableByteChannel) extends
    WritableByteChannel {

    private var count: Long = 0

    def numBytesWritten(): Long = {
      count
    }

    override def isOpen: Boolean = {
      channel.isOpen
    }

    override def close(): Unit = {}

    override def write(x: ByteBuffer): Int = {
      var c = 0
      while (x.hasRemaining) {
        c += channel.write(x)
      }
      count += c
      c
    }
  }
}

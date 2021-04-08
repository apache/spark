/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.internal

import java.util.concurrent.TimeUnit

import com.esotericsoftware.kryo.io.Input
import net.jpountz.lz4.{LZ4Factory, LZ4FastDecompressor}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.clients.{ShuffleDataReader, TaskDataBlock}
import org.apache.spark.remoteshuffle.exceptions.{RssInvalidDataException, RssInvalidStateException}
import org.apache.spark.remoteshuffle.metrics.M3Stats
import org.apache.spark.remoteshuffle.util.{ByteBufUtils, ExceptionUtils}
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}

class BlockDownloaderPartitionRecordIterator[K, C](
                                                    shuffleId: Int,
                                                    partition: Int,
                                                    serializer: Serializer,
                                                    context: TaskContext,
                                                    downloader: ShuffleDataReader,
                                                    shuffleReadMetrics: ShuffleReadMetricsReporter)
  extends Iterator[Product2[K, C]]
    with Logging {

  private val decompressor: LZ4FastDecompressor = LZ4Factory.fastestInstance.fastDecompressor()

  private var downloaderEof = false

  private var deserializationInput: Input = null
  private var deserializationStream: DeserializationStream = null
  private var deserializationIterator: Iterator[(Any, Any)] = null

  private var numRecords = 0L
  private var numRemoteBytesRead = 0L

  // time when the first record is fetched
  private var fetchStartNanoTime = 0L

  // how much time spent on network (download data from shuffle server)
  private var fetchNanoTime = 0L

  // how much time spent on executing code in this class
  private var executeNanoTime = 0L

  // how much time spent on decompression data
  private var decompressTime = 0L

  // how much time spent on deserialize
  private var deserializeTime = 0L

  private var lastLogMillis = System.currentTimeMillis()
  private val logIntervalMillis = 30000L

  private val serializerInstance = serializer.newInstance()

  override def hasNext: Boolean = {
    val methodStartTime = System.nanoTime()

    if (fetchStartNanoTime == 0) {
      fetchStartNanoTime = methodStartTime
    }

    val deserializeStartTime = System.nanoTime()
    while ((deserializationIterator == null || !deserializationIterator.hasNext) &&
      !downloaderEof) {
      deserializeTime += System.nanoTime() - deserializeStartTime
      fetchNextDeserializationIterator()
    }

    val deserializeStartTime2 = System.nanoTime()
    val result = if (deserializationIterator == null) {
      false
    } else {
      deserializationIterator.hasNext
    }
    deserializeTime += System.nanoTime() - deserializeStartTime2

    if (!result) {
      shuffleReadMetrics.incRecordsRead(numRecords)
      shuffleReadMetrics.incRemoteBytesRead(numRemoteBytesRead)
      shuffleReadMetrics.incFetchWaitTime(TimeUnit.NANOSECONDS.toMillis(fetchNanoTime))
      logShuffleFetchInfo(true)
      context.taskMetrics().mergeShuffleReadMetrics()
    }

    executeNanoTime += (System.nanoTime() - methodStartTime)
    result
  }

  override def next(): Product2[K, C] = {
    val methodStartTime = System.nanoTime()

    if (fetchStartNanoTime == 0) {
      fetchStartNanoTime = methodStartTime
    }

    if (deserializationIterator == null) {
      throw new RssInvalidStateException(
        s"deserializationIterator is null, please check hasNext before trying to get next, " +
          s"shuffle $shuffleId partition $partition, " +
          String.valueOf(downloader))
    }

    val deserializeStartTime = System.nanoTime()
    val result = deserializationIterator.next().asInstanceOf[(K, C)]
    deserializeTime += System.nanoTime() - deserializeStartTime
    if (result != null) {
      numRecords = numRecords + 1
    }

    executeNanoTime += (System.nanoTime() - methodStartTime)
    result
  }

  private def fetchNextDeserializationIterator(): Unit = {
    clearDeserializationStream()

    val readRecordStartNanoTime = System.nanoTime()
    var dataBlock: TaskDataBlock = null;

    try {
      dataBlock = downloader.readDataBlock()
      fetchNanoTime += System.nanoTime() - readRecordStartNanoTime

      while (dataBlock != null &&
        (dataBlock.getPayload == null || dataBlock.getPayload.size == 0)) {
        val readRecordStartNanoTime = System.nanoTime()
        dataBlock = downloader.readDataBlock()
        fetchNanoTime += System.nanoTime() - readRecordStartNanoTime
      }
    } catch {
      case ex: Throwable =>
        downloader.close()
        M3Stats.addException(ex, this.getClass().getSimpleName())
        throw new FetchFailedException(
          RssUtils.createReduceTaskDummyBlockManagerId(shuffleId, partition),
          shuffleId,
          -1,
          -1,
          partition,
          s"Failed to read data fro shuffle $shuffleId partition $partition due to ${
            ExceptionUtils.getSimpleMessage(ex)
          })",
          ex)
    }

    numRemoteBytesRead = downloader.getShuffleReadBytes

    if (dataBlock == null) {
      downloaderEof = true
      downloader.close()
      deserializationIterator = null
      return
    }

    val decompressStartTime = System.nanoTime()
    val bytes = dataBlock.getPayload
    val compressedLen = ByteBufUtils.readInt(bytes, 0)
    val uncompressedLen = ByteBufUtils.readInt(bytes, Integer.BYTES)
    val uncompressedBytes = new Array[Byte](uncompressedLen)
    val count = decompressor
      .decompress(bytes, Integer.BYTES + Integer.BYTES, uncompressedBytes, 0, uncompressedLen)
    decompressTime += (System.nanoTime() - decompressStartTime)
    if (count != compressedLen) {
      throw new RssInvalidDataException(
        s"Data corrupted for shuffle $shuffleId partition $partition, expected compressed " +
          s"length: $compressedLen, but it is: $count, " +
          String.valueOf(downloader))
    }

    deserializationInput = new Input(uncompressedBytes, 0, uncompressedLen)
    deserializationStream = serializerInstance.deserializeStream(deserializationInput)
    deserializationIterator = deserializationStream.asKeyValueIterator

    logShuffleFetchInfo(false)
  }

  private def logShuffleFetchInfo(downloadFinished: Boolean): Unit = {
    val currentMillis = System.currentTimeMillis()
    if (downloadFinished || currentMillis - lastLogMillis > logIntervalMillis) {
      val durationMillisLogString = if (fetchStartNanoTime == 0) {
        "(fetch not started)"
      } else {
        String.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fetchStartNanoTime))
      }
      val executeMillis = TimeUnit.NANOSECONDS.toMillis(executeNanoTime)
      val fetchMillis = TimeUnit.NANOSECONDS.toMillis(fetchNanoTime)
      val decompressMillis = TimeUnit.NANOSECONDS.toMillis(decompressTime)
      val deserializeMillis = TimeUnit.NANOSECONDS.toMillis(deserializeTime)
      logInfo(
        s"ShuffleFetchInfo: shuffle $shuffleId partition $partition, $numRecords records, " +
          s"$numRemoteBytesRead bytes, duration millis: $durationMillisLogString, " +
          s"execute millis: $executeMillis, fetch millis: $fetchMillis, " +
          s"decompress millis: $decompressMillis, deserialize millis: $deserializeMillis, " +
          s"finished: $downloadFinished")
      lastLogMillis = currentMillis
    }
  }

  private def clearDeserializationStream() = {
    if (deserializationInput != null) {
      val remainingBytes = deserializationInput.available()
      if (remainingBytes != 0) {
        throw new RssInvalidDataException(
          s"Data corrupted for shuffle $shuffleId partition $partition, there are " +
            s"still $remainingBytes bytes to read")
      }
      deserializationInput.close()
    }
    if (deserializationStream != null) {
      deserializationStream.close()
    }
    deserializationInput = null
    deserializationStream = null
  }
}

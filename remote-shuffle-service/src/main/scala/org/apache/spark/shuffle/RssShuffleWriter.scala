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

package org.apache.spark.shuffle

import java.nio.ByteBuffer
import java.util.concurrent.{CompletableFuture, TimeUnit}
import net.jpountz.lz4.LZ4Factory
import org.apache.spark.ShuffleDependency
import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.clients.ShuffleDataWriter
import org.apache.spark.remoteshuffle.common.{AppTaskAttemptId, ServerList}
import org.apache.spark.remoteshuffle.exceptions.RssInvalidStateException
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{KryoSerializer, KryoSerializerInstance, Serializer}
import org.apache.spark.shuffle.internal.{BufferManagerOptions, KyroWriteBufferManager, CombinerWriterBufferManager, RecordSerializationBuffer, RssUtils, DefaultWriteBufferManager}

class RssShuffleWriter[K, V, C](
                                 rssServers: ServerList,
                                 writeClient: ShuffleDataWriter,
                                 mapInfo: AppTaskAttemptId,
                                 serializer: Serializer,
                                 bufferOptions: BufferManagerOptions,
                                 shuffleDependency: ShuffleDependency[K, V, C],
                                 shuffleWriteMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V] with Logging {

  logInfo(s"Using ShuffleWriter: ${
    this.getClass.getSimpleName
  }, map task: $mapInfo, buffer: $bufferOptions")

  private val partitioner = shuffleDependency.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shouldPartition = numPartitions > 1

  private val writeClientCloseLock = new Object()
  private var mapStatus: MapStatus = null

  private val createCombiner = if (shuffleDependency.mapSideCombine) {
    Some(shuffleDependency.aggregator.get.createCombiner)
  } else {
    None
  }

  private val bufferManager: RecordSerializationBuffer[K, V] = {
    if (shuffleDependency.aggregator.isEmpty || !bufferOptions.supportAggregate) {
      serializer match {
        case k: KryoSerializer =>
          logInfo(s"Create KyroWriteBufferManager with spill size " +
            s"${bufferOptions.bufferSpillThreshold}")
          new KyroWriteBufferManager[K, V](
            serializerInstance = k.newInstance().asInstanceOf[KryoSerializerInstance],
            bufferSize = bufferOptions.individualBufferSize,
            maxBufferSize = bufferOptions.individualBufferMax,
            spillSize = bufferOptions.bufferSpillThreshold,
            numPartitions = numPartitions,
            createCombiner = createCombiner)
        case _ =>
          logInfo(s"Create WriteBufferManager with spill size " +
            s"${bufferOptions.bufferSpillThreshold}")
          new DefaultWriteBufferManager[K, V](
            serializer = serializer,
            bufferSize = bufferOptions.individualBufferSize,
            spillSize = bufferOptions.bufferSpillThreshold,
            numPartitions = numPartitions,
            createCombiner = createCombiner)
      }

    } else {
      logInfo(s"Create RecordCombinedSerializationBuffer with spill " +
        s"size ${bufferOptions.bufferSpillThreshold}")
      new CombinerWriterBufferManager[K, V, C](
        createCombiner = shuffleDependency.aggregator.get.createCombiner,
        mergeValue = shuffleDependency.aggregator.get.mergeValue,
        serializer = serializer,
        spillSize = bufferOptions.bufferSpillThreshold
      )
    }
  }

  private val compressor = LZ4Factory.fastestInstance.fastCompressor

  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.getPartition(key) else 0
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    logInfo(
      s"Writing shuffle records ($mapInfo), map side combine: ${shuffleDependency.mapSideCombine}")

    var numRecords = 0

    val startUploadStartTime = System.nanoTime()
    val numMaps = Integer.MAX_VALUE // TODO remove this for Spark 3.0
    writeClient.startUpload(mapInfo, numMaps, numPartitions)
    val startUploadTime = System.nanoTime() - startUploadStartTime

    var writeRecordTime = 0L
    var serializeTime = 0L

    var recordFetchStartTime = System.nanoTime()
    var recordFetchTime = 0L

    val partitionLengths: Array[Long] = Array.fill[Long](numPartitions)(0L)

    while (records.hasNext) {
      val record = records.next()
      recordFetchTime += (System.nanoTime() - recordFetchStartTime)

      val writeRecordStartTime = System.nanoTime()

      val partition = getPartition(record._1)

      var spilledData: Seq[(Int, Array[Byte], Int)] = null

      val serializeStartTime = System.nanoTime()
      spilledData = bufferManager.addRecord(partition, record)
      serializeTime += (System.nanoTime() - serializeStartTime)
      sendDataBlocks(spilledData, partitionLengths)

      numRecords = numRecords + 1
      writeRecordTime += (System.nanoTime() - writeRecordStartTime)

      recordFetchStartTime = System.nanoTime()
    }

    val remainingData = bufferManager.clear()
    sendDataBlocks(remainingData, partitionLengths)

    val finishUploadStartTime = System.nanoTime()
    writeClient.finishUpload()
    val finishUploadTime = System.nanoTime() - finishUploadStartTime

    val totalBytes = writeClient.getShuffleWriteBytes()
    logInfo(
      s"Wrote shuffle records ($mapInfo), $numRecords records, $totalBytes bytes, write seconds: ${
        TimeUnit.NANOSECONDS.toSeconds(startUploadTime)
      }, ${TimeUnit.NANOSECONDS.toSeconds(writeRecordTime)}, ${
        TimeUnit.NANOSECONDS.toSeconds(finishUploadTime)
      }, serialize seconds: ${
        TimeUnit.NANOSECONDS.toSeconds(serializeTime)
      }, record fetch seconds: ${TimeUnit.NANOSECONDS.toSeconds(recordFetchTime)}")

    shuffleWriteMetrics.incRecordsWritten(numRecords)
    shuffleWriteMetrics.incBytesWritten(totalBytes)
    shuffleWriteMetrics.incWriteTime(startUploadTime + writeRecordTime + finishUploadTime)

    // fill non-zero length
    val nonZeroPartitionLengths = partitionLengths.map(x => if (x == 0) 1 else x)

    val blockManagerId = RssUtils
      .createMapTaskDummyBlockManagerId(mapInfo.getMapId, mapInfo.getTaskAttemptId, rssServers)
    mapStatus = MapStatus(blockManagerId, nonZeroPartitionLengths, mapInfo.getTaskAttemptId)

    closeWriteClientAsync()
  }

  private def sendDataBlocks(fullFilledData: Seq[(Int, Array[Byte], Int)],
                             partitionLengths: Array[Long]) = {
    fullFilledData.foreach(t => {
      val partitionId = t._1
      val bytes = t._2
      val length = t._3
      if (bytes != null && bytes.length > 0 && length > 0) {
        val dataBlock = createDataBlock(bytes, length)
        writeClient.writeDataBlock(partitionId, dataBlock)

        partitionLengths(partitionId) += length
      }
    })
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    logInfo(s"Stopped shuffle writer ($mapInfo), success: $success")

    closeWriteClientAsync()

    if (success) {
      val remainingBytes = bufferManager.filledBytes
      if (remainingBytes != 0) {
        throw new RssInvalidStateException(
          s"Writer buffer should be empty, but still has $remainingBytes bytes, $mapInfo")
      }
      Option(mapStatus)
    } else {
      None
    }
  }

  override def getPartitionLengths(): Array[Long] = {
    Array.fill[Long](partitioner.numPartitions)(0L)
  }

  private def closeWriteClientAsync() = {
    CompletableFuture.runAsync(new Runnable {
      override def run(): Unit = {
        writeClientCloseLock.synchronized {
          writeClient.close()
        }
      }
    })
  }

  private def createDataBlock(buffer: Array[Byte], length: Int): ByteBuffer = {
    val uncompressedByteCount = length
    val compressedBuffer = new Array[Byte](compressor.maxCompressedLength(uncompressedByteCount))
    val compressedByteCount = compressor.compress(buffer, 0, length, compressedBuffer, 0)
    val dataBlockByteBuffer = ByteBuffer
      .allocate(Integer.BYTES + Integer.BYTES + compressedByteCount)
    dataBlockByteBuffer.putInt(compressedByteCount)
    dataBlockByteBuffer.putInt(uncompressedByteCount)
    dataBlockByteBuffer.put(compressedBuffer, 0, compressedByteCount)
    dataBlockByteBuffer.flip
    dataBlockByteBuffer
  }
}

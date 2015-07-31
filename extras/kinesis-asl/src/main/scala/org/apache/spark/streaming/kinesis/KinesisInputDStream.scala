package org.apache.spark.streaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo
import org.apache.spark.streaming.{Duration, StreamingContext, Time}

private[kinesis] class KinesisInputDStream(
    ssc: StreamingContext,
    streamName: String,
    endpointUrl: String,
    regionName: String,
    initialPositionInStream: InitialPositionInStream,
    checkpointAppName: String,
    checkpointInterval: Duration,
    storageLevel: StorageLevel,
    awsCredentialsOption: Option[SerializableAWSCredentials]
  ) extends ReceiverInputDStream[Array[Byte]](ssc) {

  private[streaming]
  override def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[Array[Byte]] = {

    val allBlocksHaveRanges = blockInfos.map { _.metadataOption }.forall(_.nonEmpty)

    if (blockInfos.nonEmpty && allBlocksHaveRanges) {
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray
      val seqNumRanges =
        blockInfos.map { _.metadataOption.get.asInstanceOf[SequenceNumberRanges] }.toArray
      logDebug(
        s"Creating KBBRDD for $time with seq number ranges = ${seqNumRanges.mkString(", ")} ")
      new KinesisBackedBlockRDD(context.sc, regionName, endpointUrl, blockIds, seqNumRanges)
    } else {
      logWarning("Kinesis sequence number information not present with block metadata, " +
        "may not be possible to recover from failures")
      super.createBlockRDD(time, blockInfos)
    }
  }

  override def getReceiver(): Receiver[Array[Byte]] = {
    new KinesisReceiver(streamName, endpointUrl, regionName, initialPositionInStream,
      checkpointAppName, checkpointInterval, storageLevel, awsCredentialsOption)
  }
}

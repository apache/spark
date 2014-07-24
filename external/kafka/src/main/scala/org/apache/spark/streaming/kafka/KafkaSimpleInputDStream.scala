package org.apache.spark.streaming.kafka

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import kafka.serializer.Decoder

class KafkaSimpleInputDStream[U <: Decoder[_]: Manifest, T <: Decoder[_]: Manifest](
  @transient ssc_ : StreamingContext,
  brokers: Seq[String],
  topic: String,
  partition: Integer,
  startPositionOffset: Long,
  maxBatchByteSize: Int = 1024 * 1024,
  storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2) extends ReceiverInputDStream[(Long, Array[Byte])](ssc_) with Logging {

  def getReceiver(): Receiver[(Long, Array[Byte])] = {
    new KafkaSimpleReceiver[U, T](brokers, topic, partition, startPositionOffset, maxBatchByteSize, storageLevel)
      .asInstanceOf[Receiver[(Long, Array[Byte])]]
  }
}

class KafkaSimpleReceiver[U <: Decoder[_]: Manifest, T <: Decoder[_]: Manifest](
  brokers: Seq[String],
  topic: String,
  partition: Integer,
  startPositionOffset: Long,
  maxBatchByteSize: Int = 1024 * 1024,
  storageLevel: StorageLevel) extends Receiver[Any](storageLevel) with Logging {

  var currentOffset = startPositionOffset
  val kac = new KafkaSimpleConsumer(brokers, topic, partition, maxBatchByteSize);

  def onStop() {
    kac.close
    logInfo("Kafka consumer closed.")
  }

  def onStart() {
    logInfo("Starting Kafka Consumer Stream")
    val firstOffset = kac.getEarliestOffset()
    if (currentOffset < firstOffset) {
      logWarning(s"at present, the first offset is ${firstOffset}, the messages which is from ${currentOffset} to ${firstOffset} might been pruned.")
      currentOffset = firstOffset
    }
    while (true) {
      val messageSet = kac.fetch(currentOffset)

      val itr = messageSet.iterator
      var hasMessage = false
      while (itr.hasNext) {
        val messageAndOffset = itr.next()
        val payload = messageAndOffset.message.payload
        val bytes = new Array[Byte](payload.limit);
        payload.get(bytes);
        currentOffset = messageAndOffset.offset
        store((currentOffset, bytes))
        hasMessage = true
      }
      if (hasMessage) {
        currentOffset = currentOffset + 1
      }
      Thread.sleep(10)
    }
  }
}
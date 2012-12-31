package spark.streaming.dstream

import spark.Logging
import spark.storage.StorageLevel
import spark.streaming.{Time, DStreamCheckpointData, StreamingContext}

import java.util.Properties
import java.util.concurrent.Executors

import kafka.consumer._
import kafka.message.{Message, MessageSet, MessageAndMetadata}
import kafka.serializer.StringDecoder
import kafka.utils.{Utils, ZKGroupTopicDirs}
import kafka.utils.ZkUtils._

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._


// Key for a specific Kafka Partition: (broker, topic, group, part)
case class KafkaPartitionKey(brokerId: Int, topic: String, groupId: String, partId: Int)
// NOT USED - Originally intended for fault-tolerance
// Metadata for a Kafka Stream that it sent to the Master
private[streaming]
case class KafkaInputDStreamMetadata(timestamp: Long, data: Map[KafkaPartitionKey, Long])
// NOT USED - Originally intended for fault-tolerance
// Checkpoint data specific to a KafkaInputDstream
private[streaming]
case class KafkaDStreamCheckpointData(kafkaRdds: HashMap[Time, Any],
  savedOffsets: Map[KafkaPartitionKey, Long]) extends DStreamCheckpointData(kafkaRdds)

/**
 * Input stream that pulls messages from a Kafka Broker.
 * 
 * @param host Zookeper hostname.
 * @param port Zookeper port.
 * @param groupId The group id for this consumer.
 * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
 * in its own thread.
 * @param initialOffsets Optional initial offsets for each of the partitions to consume.
 * By default the value is pulled from zookeper.
 * @param storageLevel RDD storage level.
 */
private[streaming]
class KafkaInputDStream[T: ClassManifest](
    @transient ssc_ : StreamingContext,
    host: String,
    port: Int,
    groupId: String,
    topics: Map[String, Int],
    initialOffsets: Map[KafkaPartitionKey, Long],
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[T](ssc_ ) with Logging {

  // Metadata that keeps track of which messages have already been consumed.
  var savedOffsets = HashMap[Long, Map[KafkaPartitionKey, Long]]()
  
  /* NOT USED - Originally intended for fault-tolerance
 
  // In case of a failure, the offets for a particular timestamp will be restored.
  @transient var restoredOffsets : Map[KafkaPartitionKey, Long] = null

 
  override protected[streaming] def addMetadata(metadata: Any) {
    metadata match {
      case x : KafkaInputDStreamMetadata =>
        savedOffsets(x.timestamp) = x.data
        // TOOD: Remove logging
        logInfo("New saved Offsets: " + savedOffsets)
      case _ => logInfo("Received unknown metadata: " + metadata.toString)
    }
  }

  override protected[streaming] def updateCheckpointData(currentTime: Time) {
    super.updateCheckpointData(currentTime)
    if(savedOffsets.size > 0) {
      // Find the offets that were stored before the checkpoint was initiated
      val key = savedOffsets.keys.toList.sortWith(_ < _).filter(_ < currentTime.millis).last
      val latestOffsets = savedOffsets(key)
      logInfo("Updating KafkaDStream checkpoint data: " + latestOffsets.toString)
      checkpointData = KafkaDStreamCheckpointData(checkpointData.rdds, latestOffsets)
      // TODO: This may throw out offsets that are created after the checkpoint,
      // but it's unlikely we'll need them.
      savedOffsets.clear()
    }
  }

  override protected[streaming] def restoreCheckpointData() {
    super.restoreCheckpointData()
    logInfo("Restoring KafkaDStream checkpoint data.")
    checkpointData match { 
      case x : KafkaDStreamCheckpointData => 
        restoredOffsets = x.savedOffsets
        logInfo("Restored KafkaDStream offsets: " + savedOffsets)
    }
  } */

  def createReceiver(): NetworkReceiver[T] = {
    new KafkaReceiver(id, host, port,  groupId, topics, initialOffsets, storageLevel)
        .asInstanceOf[NetworkReceiver[T]]
  }
}

private[streaming]
class KafkaReceiver(streamId: Int, host: String, port: Int, groupId: String, 
  topics: Map[String, Int], initialOffsets: Map[KafkaPartitionKey, Long], 
  storageLevel: StorageLevel) extends NetworkReceiver[Any](streamId) {

  // Timeout for establishing a connection to Zookeper in ms.
  val ZK_TIMEOUT = 10000

  // Handles pushing data into the BlockManager
  lazy protected val dataHandler = new DataHandler(this, storageLevel)
  // Keeps track of the current offsets. Maps from (broker, topic, group, part) -> Offset
  lazy val offsets = HashMap[KafkaPartitionKey, Long]()
  // Connection to Kafka
  var consumerConnector : ZookeeperConsumerConnector = null

  def onStop() {
    dataHandler.stop()
  }

  def onStart() {

    // Starting the DataHandler that buffers blocks and pushes them into them BlockManager
    dataHandler.start()

    // In case we are using multiple Threads to handle Kafka Messages
    val executorPool = Executors.newFixedThreadPool(topics.values.reduce(_ + _))

    val zooKeeperEndPoint = host + ":" + port
    logInfo("Starting Kafka Consumer Stream with group: " + groupId)
    logInfo("Initial offsets: " + initialOffsets.toString)
    
    // Zookeper connection properties
    val props = new Properties()
    props.put("zk.connect", zooKeeperEndPoint)
    props.put("zk.connectiontimeout.ms", ZK_TIMEOUT.toString)
    props.put("groupid", groupId)

    // Create the connection to the cluster
    logInfo("Connecting to Zookeper: " + zooKeeperEndPoint)
    val consumerConfig = new ConsumerConfig(props)
    consumerConnector = Consumer.create(consumerConfig).asInstanceOf[ZookeeperConsumerConnector]
    logInfo("Connected to " + zooKeeperEndPoint)

    // Reset the Kafka offsets in case we are recovering from a failure
    resetOffsets(initialOffsets)

    // Create Threads for each Topic/Message Stream we are listening
    val topicMessageStreams = consumerConnector.createMessageStreams(topics, new StringDecoder())

    // Start the messages handler for each partition
    topicMessageStreams.values.foreach { streams =>
      streams.foreach { stream => executorPool.submit(new MessageHandler(stream)) }
    }

  }

  // Overwrites the offets in Zookeper.
  private def resetOffsets(offsets: Map[KafkaPartitionKey, Long]) {
    offsets.foreach { case(key, offset) =>
      val topicDirs = new ZKGroupTopicDirs(key.groupId, key.topic)
      val partitionName = key.brokerId + "-" + key.partId
      updatePersistentPath(consumerConnector.zkClient, 
        topicDirs.consumerOffsetDir + "/" + partitionName, offset.toString)
    }
  }

  // Handles Kafka Messages
  private class MessageHandler(stream: KafkaStream[String]) extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      stream.takeWhile { msgAndMetadata => 
        dataHandler += msgAndMetadata.message

        // Updating the offet. The key is (broker, topic, group, partition).
        val key = KafkaPartitionKey(msgAndMetadata.topicInfo.brokerId, msgAndMetadata.topic, 
          groupId, msgAndMetadata.topicInfo.partition.partId)
        val offset = msgAndMetadata.topicInfo.getConsumeOffset
        offsets.put(key, offset)
        // logInfo("Handled message: " + (key, offset).toString)

        // Keep on handling messages
        true
      }  
    }
  }

  // NOT USED - Originally intended for fault-tolerance
  // class KafkaDataHandler(receiver: KafkaReceiver, storageLevel: StorageLevel) 
  // extends DataHandler[Any](receiver, storageLevel) {

  //   override def createBlock(blockId: String, iterator: Iterator[Any]) : Block = {
  //     // Creates a new Block with Kafka-specific Metadata
  //     new Block(blockId, iterator, KafkaInputDStreamMetadata(System.currentTimeMillis, offsets.toMap))
  //   }

  // }

}

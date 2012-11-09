package spark.streaming

import java.lang.reflect.Method
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, Executors}
import kafka.api.{FetchRequest}
import kafka.consumer._
import kafka.cluster.Partition
import kafka.message.{Message, MessageSet, MessageAndMetadata}
import kafka.serializer.StringDecoder
import kafka.utils.{Pool, Utils, ZKGroupTopicDirs}
import kafka.utils.ZkUtils._
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import spark._
import spark.RDD
import spark.storage.StorageLevel


case class KafkaPartitionKey(brokerId: Int, topic: String, groupId: String, partId: Int)
case class KafkaInputDStreamMetadata(timestamp: Long, data: Map[KafkaPartitionKey, Long])
case class KafkaDStreamCheckpointData(kafkaRdds: HashMap[Time, Any], 
  savedOffsets: HashMap[Long, Map[KafkaPartitionKey, Long]]) extends DStreamCheckpointData(kafkaRdds)

/**
 * Input stream that pulls messages form a Kafka Broker.
 */
class KafkaInputDStream[T: ClassManifest](
    @transient ssc_ : StreamingContext,
    host: String,
    port: Int,
    groupId: String,
    topics: Map[String, Int],
    initialOffsets: Map[KafkaPartitionKey, Long],
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[T](ssc_ ) with Logging {

  var savedOffsets = HashMap[Long, Map[KafkaPartitionKey, Long]]()

  override protected[streaming] def addMetadata(metadata: Any) {
    metadata match {
      case x : KafkaInputDStreamMetadata => 
        savedOffsets(x.timestamp) = x.data
        logInfo("Saved Offsets: " + savedOffsets)
      case _ => logInfo("Received unknown metadata: " + metadata.toString)
    }
  }

  override protected[streaming] def updateCheckpointData(currentTime: Time) {
    super.updateCheckpointData(currentTime)
    logInfo("Updating KafkaDStream checkpoint data: " + savedOffsets.toString)
    checkpointData = KafkaDStreamCheckpointData(checkpointData.rdds, savedOffsets)
  }

  override protected[streaming] def restoreCheckpointData() {
    super.restoreCheckpointData()
    logInfo("Restoring KafkaDStream checkpoint data.")
    checkpointData match { 
      case x : KafkaDStreamCheckpointData => 
        savedOffsets = x.savedOffsets
        logInfo("Restored KafkaDStream offsets: " + savedOffsets.toString)
    }
  }

  def createReceiver(): NetworkReceiver[T] = {
    new KafkaReceiver(id, host, port,  groupId, topics, initialOffsets, storageLevel)
      .asInstanceOf[NetworkReceiver[T]]
  }
}

class KafkaReceiver(streamId: Int, host: String, port: Int, groupId: String, 
  topics: Map[String, Int], initialOffsets: Map[KafkaPartitionKey, Long], 
  storageLevel: StorageLevel) extends NetworkReceiver[Any](streamId) {

  // Timeout for establishing a connection to Zookeper in ms.
  val ZK_TIMEOUT = 10000

  // Handles pushing data into the BlockManager
  lazy protected val dataHandler = new KafkaDataHandler(this, storageLevel)
  // Keeps track of the current offsets. Maps from (topic, partitionID) -> Offset
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
    logInfo("Starting Kafka Consumer Stream in group " + groupId)
    logInfo("Initial offsets: " + initialOffsets.toString)
    logInfo("Connecting to " + zooKeeperEndPoint)
    // Specify some Consumer properties
    val props = new Properties()
    props.put("zk.connect", zooKeeperEndPoint)
    props.put("zk.connectiontimeout.ms", ZK_TIMEOUT.toString)
    props.put("groupid", groupId)

    // Create the connection to the cluster
    val consumerConfig = new ConsumerConfig(props)
    consumerConnector = Consumer.create(consumerConfig).asInstanceOf[ZookeeperConsumerConnector]

    // Reset the Kafka offsets in case we are recovering from a failure
    resetOffsets(initialOffsets)
    
    logInfo("Connected to " + zooKeeperEndPoint)

    // Create Threads for each Topic/Message Stream we are listening
    val topicMessageStreams = consumerConnector.createMessageStreams(topics, new StringDecoder())

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

  // Responsible for handling Kafka Messages
  class MessageHandler(stream: KafkaStream[String]) extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      stream.takeWhile { msgAndMetadata => 
        dataHandler += msgAndMetadata.message

        // Updating the offet. The key is (topic, partitionID).
        val key = KafkaPartitionKey(msgAndMetadata.topicInfo.brokerId, msgAndMetadata.topic, 
          groupId, msgAndMetadata.topicInfo.partition.partId)
        val offset = msgAndMetadata.topicInfo.getConsumeOffset
        offsets.put(key, offset)
        logInfo((key, offset).toString)

        // Keep on handling messages
        true
      }  
    }
  }

  class KafkaDataHandler(receiver: KafkaReceiver, storageLevel: StorageLevel) 
  extends DataHandler[Any](receiver, storageLevel) {

    override def createBlock(blockId: String, iterator: Iterator[Any]) : Block = {
      new Block(blockId, iterator, KafkaInputDStreamMetadata(System.currentTimeMillis, offsets.toMap))
    }

  }
}

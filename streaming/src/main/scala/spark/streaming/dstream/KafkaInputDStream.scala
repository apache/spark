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

  def createReceiver(): NetworkReceiver[T] = {
    new KafkaReceiver(host, port,  groupId, topics, initialOffsets, storageLevel)
        .asInstanceOf[NetworkReceiver[T]]
  }
}

private[streaming]
class KafkaReceiver(host: String, port: Int, groupId: String,
  topics: Map[String, Int], initialOffsets: Map[KafkaPartitionKey, Long], 
  storageLevel: StorageLevel) extends NetworkReceiver[Any] {

  // Timeout for establishing a connection to Zookeper in ms.
  val ZK_TIMEOUT = 10000

  // Handles pushing data into the BlockManager
  lazy protected val blockGenerator = new BlockGenerator(storageLevel)
  // Connection to Kafka
  var consumerConnector : ZookeeperConsumerConnector = null

  def onStop() {
    blockGenerator.stop()
  }

  def onStart() {

    blockGenerator.start()

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

    // If specified, set the topic offset
    setOffsets(initialOffsets)

    // Create Threads for each Topic/Message Stream we are listening
    val topicMessageStreams = consumerConnector.createMessageStreams(topics, new StringDecoder())

    // Start the messages handler for each partition
    topicMessageStreams.values.foreach { streams =>
      streams.foreach { stream => executorPool.submit(new MessageHandler(stream)) }
    }

  }

  // Overwrites the offets in Zookeper.
  private def setOffsets(offsets: Map[KafkaPartitionKey, Long]) {
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
        blockGenerator += msgAndMetadata.message

        // Keep on handling messages
        true
      }  
    }
  }
}

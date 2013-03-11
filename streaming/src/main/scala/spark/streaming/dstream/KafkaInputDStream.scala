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
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient._

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._


// Key for a specific Kafka Partition: (broker, topic, group, part)
case class KafkaPartitionKey(brokerId: Int, topic: String, groupId: String, partId: Int)

/**
 * Input stream that pulls messages from a Kafka Broker.
 * 
 * @param kafkaParams Map of kafka configuration paramaters. See: http://kafka.apache.org/configuration.html
 * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
 * in its own thread.
 * @param initialOffsets Optional initial offsets for each of the partitions to consume.
 * By default the value is pulled from zookeper.
 * @param storageLevel RDD storage level.
 */
private[streaming]
class KafkaInputDStream[T: ClassManifest](
    @transient ssc_ : StreamingContext,
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    initialOffsets: Map[KafkaPartitionKey, Long],
    storageLevel: StorageLevel
  ) extends NetworkInputDStream[T](ssc_ ) with Logging {


  def getReceiver(): NetworkReceiver[T] = {
    new KafkaReceiver(kafkaParams, topics, initialOffsets, storageLevel)
        .asInstanceOf[NetworkReceiver[T]]
  }
}

private[streaming]
class KafkaReceiver(kafkaParams: Map[String, String],
  topics: Map[String, Int], initialOffsets: Map[KafkaPartitionKey, Long], 
  storageLevel: StorageLevel) extends NetworkReceiver[Any] {

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

    logInfo("Starting Kafka Consumer Stream with group: " + kafkaParams("groupid"))
    logInfo("Initial offsets: " + initialOffsets.toString)

    // Kafka connection properties
    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))

    // Create the connection to the cluster
    logInfo("Connecting to Zookeper: " + kafkaParams("zk.connect"))
    val consumerConfig = new ConsumerConfig(props)
    consumerConnector = Consumer.create(consumerConfig).asInstanceOf[ZookeeperConsumerConnector]
    logInfo("Connected to " + kafkaParams("zk.connect"))

    // When autooffset.reset is 'smallest', it is our responsibility to try and whack the
    // consumer group zk node.
    if (kafkaParams.get("autooffset.reset").exists(_ == "smallest")) {
      tryZookeeperConsumerGroupCleanup(kafkaParams("zk.connect"), kafkaParams("groupid"))
    }

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

  // Handles cleanup of consumer group znode. Lifted with love from Kafka's
  // ConsumerConsole.scala tryCleanupZookeeper()
  private def tryZookeeperConsumerGroupCleanup(zkUrl: String, groupId: String) {
    try {
      val dir = "/consumers/" + groupId
      logInfo("Cleaning up temporary zookeeper data under " + dir + ".")
      val zk = new ZkClient(zkUrl, 30*1000, 30*1000, ZKStringSerializer)
      zk.deleteRecursive(dir)
      zk.close()
    } catch {
      case _ => // swallow
    }
  }
}

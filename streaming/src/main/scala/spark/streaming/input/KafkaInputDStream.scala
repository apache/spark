package spark.streaming

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{ArrayBlockingQueue, Executors}
import kafka.api.{FetchRequest}
import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.message.{Message, MessageSet, MessageAndMetadata}
import kafka.utils.Utils
import scala.collection.JavaConversions._
import spark._
import spark.RDD
import spark.storage.StorageLevel


/**
 * An input stream that pulls messages form a Kafka Broker.
 */
class KafkaInputDStream[T: ClassManifest](
    @transient ssc_ : StreamingContext,
    host: String,
    port: Int,
    groupId: String,
    storageLevel: StorageLevel,
    timeout: Int = 10000,
    bufferSize: Int = 1024000
  ) extends NetworkInputDStream[T](ssc_ ) with Logging {

  def createReceiver(): NetworkReceiver[T] = {
    new KafkaReceiver(id, host, port, storageLevel, groupId, timeout).asInstanceOf[NetworkReceiver[T]]
  }
}

class KafkaReceiver(streamId: Int, host: String, port: Int, storageLevel: StorageLevel, groupId: String, timeout: Int)
  extends NetworkReceiver[Any](streamId) {

  //var executorPool : = null
  var blockPushingThread : Thread = null

  def onStop() {
    blockPushingThread.interrupt()
  }

  def onStart() {

    val executorPool = Executors.newFixedThreadPool(2)

    logInfo("Starting Kafka Consumer with groupId " + groupId)

    val zooKeeperEndPoint = host + ":" + port
    logInfo("Connecting to " + zooKeeperEndPoint)

    // Specify some consumer properties
    val props = new Properties()
    props.put("zk.connect", zooKeeperEndPoint)
    props.put("zk.connectiontimeout.ms", timeout.toString)
    props.put("groupid", groupId)

    // Create the connection to the cluster
    val consumerConfig = new ConsumerConfig(props)
    val consumerConnector = Consumer.create(consumerConfig)
    logInfo("Connected to " + zooKeeperEndPoint)
    logInfo("")
    logInfo("")

    // Specify which topics we are listening to
    val topicCountMap = Map("test" -> 2)
    val topicMessageStreams = consumerConnector.createMessageStreams(topicCountMap)
    val streams = topicMessageStreams.get("test")
    
    // Queue that holds the blocks
    val queue = new ArrayBlockingQueue[ByteBuffer](2)

    streams.getOrElse(Nil).foreach { stream =>
      executorPool.submit(new MessageHandler(stream, queue))
    }

    blockPushingThread = new DaemonThread {
      override def run() {
        logInfo("Starting BlockPushingThread.")
        var nextBlockNumber = 0
        while (true) {
          val buffer = queue.take()
          val blockId = "input-" + streamId + "-" + nextBlockNumber
          nextBlockNumber += 1
          pushBlock(blockId, buffer, storageLevel)
        }
      }
    }
    blockPushingThread.start()

    // while (true) {
    //   // Create a fetch request for topic “test”, partition 0, current offset, and fetch size of 1MB
    //   val fetchRequest = new FetchRequest("test", 0, offset, 1000000)

    //   // get the message set from the consumer and print them out
    //   val messages = consumer.fetch(fetchRequest)
    //   for(msg <- messages.iterator) {
    //     logInfo("consumed: " + Utils.toString(msg.message.payload, "UTF-8"))
    //     // advance the offset after consuming each message
    //     offset = msg.offset
    //     queue.put(msg.message.payload)
    //   }
    // }
  }

  class MessageHandler(stream: KafkaStream[Message], queue: ArrayBlockingQueue[ByteBuffer]) extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      while(true) {
        stream.foreach { msgAndMetadata => 
          logInfo("Consumed: " + Utils.toString(msgAndMetadata.message.payload, "UTF-8"))
          queue.put(msgAndMetadata.message.payload)
        }
      }      
    }
  }

}

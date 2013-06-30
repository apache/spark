package spark.streaming.examples

import java.util.Properties
import kafka.message.Message
import kafka.producer.SyncProducerConfig
import kafka.producer._
import spark.SparkContext
import spark.streaming._
import spark.streaming.StreamingContext._
import spark.storage.StorageLevel
import spark.streaming.util.RawTextHelper._

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <master> <zkQuorum> <group> <topics> <numThreads>
 *   <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `./run spark.streaming.examples.KafkaWordCount local[2] zoo01,zoo02,zoo03 my-consumer-group topic1,topic2 1`
 */
object KafkaWordCount {
  def main(args: Array[String]) {
    
    if (args.length < 5) {
      System.err.println("Usage: KafkaWordCount <master> <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(master, zkQuorum, group, topics, numThreads) = args

    val ssc =  new StreamingContext(master, "KafkaWordCount", Seconds(2),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    ssc.checkpoint("checkpoint")

    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = ssc.kafkaStream(zkQuorum, group, topicpMap)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1l)).reduceByKeyAndWindow(add _, subtract _, Minutes(10), Seconds(2), 2)
    wordCounts.print()
    
    ssc.start()
  }
}

// Produces some random words between 1 and 100.
object KafkaWordCountProducer {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: KafkaWordCountProducer <zkQuorum> <topic> <messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(zkQuorum, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeper connection properties
    val props = new Properties()
    props.put("zk.connect", zkQuorum)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Send some messages
    while(true) {
      val messages = (1 to messagesPerSec.toInt).map { messageNum =>
        (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString).mkString(" ")
      }.toArray
      println(messages.mkString(","))
      val data = new ProducerData[String, String](topic, messages)
      producer.send(data)
      Thread.sleep(100)
    }
  }

}


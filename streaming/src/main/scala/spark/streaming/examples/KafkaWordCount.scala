package spark.streaming.examples

import java.util.Properties
import kafka.message.Message
import kafka.producer.SyncProducerConfig
import kafka.producer._
import spark.streaming._
import spark.streaming.StreamingContext._
import spark.storage.StorageLevel
import spark.streaming.util.RawTextHelper._

object KafkaWordCount {
  def main(args: Array[String]) {
    
    if (args.length < 6) {
      System.err.println("Usage: KafkaWordCount <master> <hostname> <port> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(master, hostname, port, group, topics, numThreads) = args

    val ssc =  new StreamingContext(master, "KafkaWordCount")
    ssc.checkpoint("checkpoint")
    ssc.setBatchDuration(Seconds(2))

    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = ssc.kafkaStream[String](hostname, port.toInt, group, topicpMap)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1l)).reduceByKeyAndWindow(add _, subtract _, Minutes(10), Seconds(2), 2)
    wordCounts.print()
    
    ssc.start()
  }
}

// Produces some random words between 1 and 100.
object KafkaWordCountProducer {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: KafkaWordCountProducer <hostname> <port> <topic> <messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(hostname, port, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeper connection properties
    val props = new Properties()
    props.put("zk.connect", hostname + ":" + port)
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


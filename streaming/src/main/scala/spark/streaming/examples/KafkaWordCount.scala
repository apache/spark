package spark.streaming.examples

import spark.streaming._
import spark.streaming.StreamingContext._
import spark.storage.StorageLevel
import WordCount2_ExtraFunctions._

object KafkaWordCount {
  def main(args: Array[String]) {
    
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <master> <hostname> <port> <restore>")
      System.exit(1)
    }

    val ssc = args(3) match {
      // Restore the stream from a checkpoint
      case "true" => 
        new StreamingContext("work/checkpoint")
      case _ =>
        val tmp =  new StreamingContext(args(0), "KafkaWordCount")

        tmp.setBatchDuration(Seconds(2))
        tmp.checkpoint("work/checkpoint", Seconds(10))
        
        val lines = tmp.kafkaStream[String](args(1), args(2).toInt, "test_group", Map("test" -> 1),
          Map(KafkaPartitionKey(0,"test","test_group",0) -> 0l))
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1l)).reduceByKeyAndWindow(add _, subtract _, Minutes(10), Seconds(2), 2)
        
        wordCounts.persist().checkpoint(Seconds(10))
        wordCounts.print()
        
        tmp
    }
    ssc.start()

  }
}


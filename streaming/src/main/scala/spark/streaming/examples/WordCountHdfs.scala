package spark.streaming.examples

import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._

object WordCountHdfs {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WordCountHdfs <master> <directory>")
      System.exit(1)
    }

    // Create the context and set the batch size
    val ssc = new StreamingContext(args(0), "WordCountHdfs")
    ssc.setBatchDuration(Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.createTextFileStream(args(1))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
  }
}


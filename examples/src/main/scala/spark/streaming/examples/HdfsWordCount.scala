package spark.streaming.examples

import spark.streaming.{Seconds, StreamingContext}
import spark.streaming.StreamingContext._


/**
 * Counts words in new text files created in the given directory
 * Usage: HdfsWordCount <master> <directory>
 *   <master> is the Spark master URL.
 *   <directory> is the directory that Spark Streaming will use to find and read new text files.
 *
 * To run this on your local machine on directory `localdir`, run this example
 *    `$ ./run spark.streaming.examples.HdfsWordCount local[2] localdir`
 * Then create a text file in `localdir` and the words in the file will get counted.
 */
object HdfsWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: HdfsWordCount <master> <directory>")
      System.exit(1)
    }

    // Create the context
    val ssc = new StreamingContext(args(0), "HdfsWordCount", Seconds(2),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(1))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
  }
}


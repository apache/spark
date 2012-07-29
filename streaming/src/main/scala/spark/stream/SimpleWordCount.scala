package spark.stream

import SparkStreamContext._

import scala.util.Sorting

object SimpleWordCount {

  def main (args: Array[String]) {
    
    if (args.length < 1) {
      println ("Usage: SparkStreamContext <host> [<temp directory>]")
      System.exit(1)
    }
    
    val ssc = new SparkStreamContext(args(0), "WordCount")
    if (args.length > 1) {
      ssc.setTempDir(args(1))
    }
    val sentences = ssc.readNetworkStream[String]("Sentences", Array("localhost:55119"), 2000)
    /*sentences.print*/
    
    val words = sentences.flatMap(_.split(" "))
   
    val counts = words.map(x => (x, 1)).reduceByKey(_ + _, 1)
    counts.print

    ssc.run
  }
}

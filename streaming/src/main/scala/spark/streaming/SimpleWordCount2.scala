package spark.streaming

import spark.SparkContext
import SparkContext._
import SparkStreamContext._

import scala.util.Sorting

object SimpleWordCount2 {

  def moreWarmup(sc: SparkContext) {
    (0 until 20).foreach {i =>
      sc.parallelize(1 to 20000000, 500)
        .map(_ % 100).map(_.toString)
        .map(x => (x, 1)).reduceByKey(_ + _, 10)
        .collect()
    }
  }

  def main (args: Array[String]) {
    
    if (args.length < 2) {
      println ("Usage: SimpleWordCount2 <host> <# sentence streams>")
      System.exit(1)
    }
    
    val ssc = new SparkStreamContext(args(0), "SimpleWordCount2")
   
    val numSentenceStreams = if (args.length > 1) args(1).toInt else 1
    if (args.length > 2) {
      ssc.setTempDir(args(2))
    }
    
    GrepCount2.warmConnectionManagers(ssc.sc)
    moreWarmup(ssc.sc)
    
    val sentences = new UnifiedRDS(
        (1 to numSentenceStreams).map(i => ssc.readTestStream("Sentences-" + i, 1000)).toArray
    )


    val words = sentences.flatMap(_.split(" "))
    
    val counts = words.map(x => (x, 1)).reduceByKey(_ + _, 10)
    counts.foreachRDD(_.collect())
    /*words.foreachRDD(_.countByValue())*/
    
    ssc.run
  }
}


package spark.streaming

import spark.SparkContext
import SparkContext._
import SparkStreamContext._

import spark.storage.StorageLevel

import scala.util.Sorting

object TopKWordCount2 {

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
      println ("Usage: SparkStreamContext <host> <# sentence streams>")
      System.exit(1)
    }
    
    val ssc = new SparkStreamContext(args(0), "WordCount2")
   
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

    def add(v1: Int, v2: Int) = (v1 + v2) 
    def subtract(v1: Int, v2: Int) = (v1 - v2) 
    
    val windowedCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(add _, subtract _, Seconds(10), Seconds(1), 10)
    windowedCounts.persist(StorageLevel.MEMORY_ONLY_DESER, StorageLevel.MEMORY_ONLY_DESER, Seconds(1))

    def topK(data: Iterator[(String, Int)], k: Int): Iterator[(String, Int)] = {
      val taken = new Array[(String, Int)](k)
      
      var i = 0
      var len = 0
      var done = false
      var value: (String, Int) = null
      var swap: (String, Int) = null
      var count = 0

      while(data.hasNext) {
        value = data.next
        count += 1
        println("count = " + count)
        if (len == 0) {
          taken(0) = value
          len = 1
        } else if (len < k || value._2 > taken(len - 1)._2) {
          if (len < k) {
            len += 1
          }
          taken(len - 1) = value
          i = len - 1
          while(i > 0 && taken(i - 1)._2 < taken(i)._2) {
            swap = taken(i)
            taken(i) = taken(i-1)
            taken(i - 1) = swap
            i -= 1
          }
        }
      }
      println("Took " + len + " out of " + count + " items")
      return taken.toIterator  
    }
    
    val k = 10
    val partialTopKWindowedCounts = windowedCounts.mapPartitions(topK(_, k))
    partialTopKWindowedCounts.foreachRDD(rdd => {
      val collectedCounts = rdd.collect
      println("Collected " + collectedCounts.size + " items")
      topK(collectedCounts.toIterator, k).foreach(println)
    })
    
    /*
    windowedCounts.filter(_ == null).foreachRDD(rdd => {
      val count = rdd.count
      println("# of nulls = " + count)
    })*/

    ssc.run
  }
}


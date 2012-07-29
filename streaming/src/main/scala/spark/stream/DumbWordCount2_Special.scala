package spark.stream

import spark.SparkContext
import SparkContext._
import SparkStreamContext._

import spark.storage.StorageLevel

import scala.util.Sorting
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue
import scala.collection.JavaConversions.mapAsScalaMap

import java.lang.{Long => JLong}
import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap => OLMap}

object DumbWordCount2_Special {

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
    
    def add(v1: JLong, v2: JLong) = (v1 + v2) 
    def subtract(v1: JLong, v2: JLong) = (v1 - v2) 
    
    def splitAndCountPartitions(iter: Iterator[String]): Iterator[(String, JLong)] = {
      val map = new java.util.HashMap[String, JLong]
      var i = 0
      var j = 0
      while (iter.hasNext) {
        val s = iter.next()
        i = 0
        while (i < s.length) {
          j = i
          while (j < s.length && s.charAt(j) != ' ') {
            j += 1
          }
          if (j > i) {
            val w = s.substring(i, j)
            val c = map.get(w)
            if (c == null) {
              map.put(w, 1)
            } else {
              map.put(w, c + 1)
            }
          }
          i = j
          while (i < s.length && s.charAt(i) == ' ') {
            i += 1
          }
        }
      }
      
      map.toIterator
    }

    val wordCounts = sentences.mapPartitions(splitAndCountPartitions).reduceByKey(_ + _, 10)
    wordCounts.persist(StorageLevel.MEMORY_ONLY)
    val windowedCounts = wordCounts.window(Seconds(10), Seconds(1)).reduceByKey(_ + _, 10) 
    windowedCounts.foreachRDD(_.collect)

    ssc.run
  }
}


package spark.stream

import spark.SparkContext
import SparkContext._
import SparkStreamContext._

import scala.collection.JavaConversions.mapAsScalaMap
import scala.util.Sorting
import java.lang.{Long => JLong}

object SimpleWordCount2_Special {

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
        (1 to numSentenceStreams).map(i => ssc.readTestStream("Sentences-" + i, 400)).toArray
    )


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


    /*val words = sentences.flatMap(_.split(" "))*/
    /*val counts = words.map(x => (x, 1)).reduceByKey(_ + _, 10)*/
    val counts = sentences.mapPartitions(splitAndCountPartitions).reduceByKey(_ + _, 10)
    counts.foreachRDD(_.collect())
    
    ssc.run
  }
}


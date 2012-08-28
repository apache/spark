package spark.streaming.examples

import spark.SparkContext
import SparkContext._
import spark.streaming._
import SparkStreamContext._

import spark.storage.StorageLevel

import scala.util.Sorting
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue
import scala.collection.JavaConversions.mapAsScalaMap

import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap => OLMap}


object WordCount2_ExtraFunctions {

  def add(v1: Long, v2: Long) = (v1 + v2) 

  def subtract(v1: Long, v2: Long) = (v1 - v2) 

  def splitAndCountPartitions(iter: Iterator[String]): Iterator[(String, Long)] = {
    //val map = new java.util.HashMap[String, Long]
    val map = new OLMap[String]
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
          val c = map.getLong(w)
          map.put(w, c + 1)
/*
          if (c == null) {
            map.put(w, 1)
          } else {
            map.put(w, c + 1)
          }
*/
        }
        i = j
        while (i < s.length && s.charAt(i) == ' ') {
          i += 1
        }
      }
    }
    map.toIterator.map{case (k, v) => (k, v)}
  }
}

object WordCount2 {

  def moreWarmup(sc: SparkContext) {
    (0 until 40).foreach {i =>
      sc.parallelize(1 to 20000000, 1000)
        .map(_ % 1331).map(_.toString)
        .mapPartitions(WordCount2_ExtraFunctions.splitAndCountPartitions).reduceByKey(_ + _, 10)
        .collect()
    }
  }
  
  def main (args: Array[String]) {
    
    if (args.length != 5) {
      println ("Usage: SparkStreamContext <host> <file> <mapTasks> <reduceTasks> <batchMillis>")
      System.exit(1)
    }

    val Array(master, file, mapTasks, reduceTasks, batchMillis) = args

    val BATCH_DURATION = Milliseconds(batchMillis.toLong)
    
    val ssc = new SparkStreamContext(master, "WordCount2")
    ssc.setBatchDuration(BATCH_DURATION)

    val data = ssc.sc.textFile(file, mapTasks.toInt).persist(StorageLevel.MEMORY_ONLY_DESER_2)
    println("Data count: " + data.count())
    println("Data count: " + data.count())
    println("Data count: " + data.count())
    
    val sentences = new ConstantInputDStream(ssc, data)
    ssc.inputStreams += sentences

    import WordCount2_ExtraFunctions._

    val windowedCounts = sentences
      .mapPartitions(splitAndCountPartitions)
      .reduceByKeyAndWindow(add _, subtract _, Seconds(10), BATCH_DURATION, reduceTasks.toInt)
    windowedCounts.persist(StorageLevel.MEMORY_ONLY_DESER, StorageLevel.MEMORY_ONLY_DESER_2, Seconds(10))
    windowedCounts.print()

    ssc.start()

    while(true) { Thread.sleep(1000) }
  }
}



package spark.streaming.examples

import spark.SparkContext
import SparkContext._
import spark.streaming._
import StreamingContext._

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

  def max(v1: Long, v2: Long) = math.max(v1, v2) 

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

  def warmup(sc: SparkContext) {
    (0 until 3).foreach {i =>
      sc.parallelize(1 to 20000000, 500)
        .map(x => (x % 337, x % 1331))
        .reduceByKey(_ + _, 100)
        .count()
    }
  }
  
  def main (args: Array[String]) {
    
    if (args.length != 6) {
      println ("Usage: WordCount2 <host> <file> <mapTasks> <reduceTasks> <batchMillis> <chkptMillis>")
      System.exit(1)
    }

    val Array(master, file, mapTasks, reduceTasks, batchMillis, chkptMillis) = args

    val batchDuration = Milliseconds(batchMillis.toLong)
    
    val ssc = new StreamingContext(master, "WordCount2")
    ssc.setBatchDuration(batchDuration)

    //warmup(ssc.sc)

    val data = ssc.sc.textFile(file, mapTasks.toInt).persist(
      new StorageLevel(false, true, false, 3))  // Memory only, serialized, 3 replicas
    println("Data count: " + data.map(x => if (x == "") 1 else x.split(" ").size / x.split(" ").size).count())
    println("Data count: " + data.count())
    println("Data count: " + data.count())
    
    val sentences = new ConstantInputDStream(ssc, data)
    ssc.registerInputStream(sentences)

    import WordCount2_ExtraFunctions._

    val windowedCounts = sentences
      .mapPartitions(splitAndCountPartitions)
      .reduceByKeyAndWindow(add _, subtract _, Seconds(30), batchDuration, reduceTasks.toInt)
    windowedCounts.persist(StorageLevel.MEMORY_ONLY, 
      StorageLevel.MEMORY_ONLY_2,
      //new StorageLevel(false, true, true, 3),
      Milliseconds(chkptMillis.toLong))
    windowedCounts.foreachRDD(r => println("Element count: " + r.count()))

    ssc.start()

    while(true) { Thread.sleep(1000) }
  }
}



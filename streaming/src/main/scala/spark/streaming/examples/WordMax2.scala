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


object WordMax2 {

  def warmup(sc: SparkContext) {
    (0 until 10).foreach {i =>
      sc.parallelize(1 to 20000000, 1000)
        .map(x => (x % 337, x % 1331))
        .reduceByKey(_ + _)
        .count()
    }
  }
  
  def main (args: Array[String]) {
    
    if (args.length != 6) {
      println ("Usage: WordMax2 <host> <file> <mapTasks> <reduceTasks> <batchMillis> <chkptMillis>")
      System.exit(1)
    }

    val Array(master, file, mapTasks, reduceTasks, batchMillis, chkptMillis) = args

    val batchDuration = Milliseconds(batchMillis.toLong)
    
    val ssc = new StreamingContext(master, "WordMax2")
    ssc.setBatchDuration(batchDuration)

    //warmup(ssc.sc)

    val data = ssc.sc.textFile(file, mapTasks.toInt).persist(
      new StorageLevel(false, true, false, 3))  // Memory only, serialized, 3 replicas
    println("Data count: " + data.count())
    println("Data count: " + data.count())
    println("Data count: " + data.count())
    
    val sentences = new ConstantInputDStream(ssc, data)
    ssc.registerInputStream(sentences)

    import WordCount2_ExtraFunctions._

    val windowedCounts = sentences
      .mapPartitions(splitAndCountPartitions)
      .reduceByKey(add _, reduceTasks.toInt)
      .persist(StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY_2,
                      Milliseconds(chkptMillis.toLong))
      .reduceByKeyAndWindow(max _, Seconds(10), batchDuration, reduceTasks.toInt)
      //.persist(StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY_2,
      //        Milliseconds(chkptMillis.toLong))
    windowedCounts.foreachRDD(r => println("Element count: " + r.count()))

    ssc.start()

    while(true) { Thread.sleep(1000) }
  }
}



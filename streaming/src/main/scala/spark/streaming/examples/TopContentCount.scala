package spark.streaming

import SparkStreamContext._

import spark.storage.StorageLevel

import scala.util.Sorting

object TopContentCount {

  case class Event(val country: String, val content: String) 
  
  object Event { 
    def create(string: String): Event = {
      val parts = string.split(":")
      new Event(parts(0), parts(1))
    }
  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      println ("Usage: GrepCount2 <host> <# sentence streams>")
      System.exit(1)
    }
    
    val ssc = new SparkStreamContext(args(0), "TopContentCount")
    val sc = ssc.sc
    val dummy = sc.parallelize(0 to 1000, 100).persist(StorageLevel.DISK_AND_MEMORY)
    sc.runJob(dummy, (_: Iterator[Int]) => {})


    val numEventStreams = if (args.length > 1) args(1).toInt else 1
    if (args.length > 2) {
      ssc.setTempDir(args(2))
    }

    val eventStrings = new UnifiedRDS(
      (1 to numEventStreams).map(i => ssc.readTestStream("Events-" + i, 1000)).toArray
    )

    def parse(string: String) = {
      val parts = string.split(":")
      (parts(0), parts(1))
    }

    def add(v1: Int, v2: Int) = (v1 + v2) 
    def subtract(v1: Int, v2: Int) = (v1 - v2) 
    
    val events = eventStrings.map(x => parse(x))
    /*events.print*/

    val parallelism = 8
    val counts_per_content_per_country = events
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      /*.reduceByKeyAndWindow(add _, subtract _, Seconds(5), Seconds(1), parallelism)*/
    /*counts_per_content_per_country.print*/
    
    /*
    counts_per_content_per_country.persist(
      StorageLevel.MEMORY_ONLY_DESER, 
      StorageLevel.MEMORY_ONLY_DESER_2, 
      Seconds(1)
    )*/
    
    val counts_per_country = counts_per_content_per_country
      .map(x => (x._1._1, (x._1._2, x._2)))
      .groupByKey()
    counts_per_country.print
    
    
    def topK(data: Seq[(String, Int)], k: Int): Array[(String, Int)] = {
      implicit val countOrdering = new Ordering[(String, Int)] {
        override def compare(count1: (String, Int), count2: (String, Int)): Int = {
          count2._2 - count1._2
        }
      }
      val array = data.toArray
      Sorting.quickSort(array)
      val taken = array.take(k)
      taken
    }
    
    val k = 10
    val topKContents_per_country = counts_per_country
      .map(x => (x._1, topK(x._2, k)))
      .map(x => (x._1, x._2.map(_.toString).reduceLeft(_ + ", " + _)))
    
    topKContents_per_country.print
    
    ssc.run
  }
}


    

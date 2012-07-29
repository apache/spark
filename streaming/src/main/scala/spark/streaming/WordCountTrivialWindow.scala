package spark.streaming

import SparkStreamContext._

import scala.util.Sorting

object WordCountTrivialWindow {

  def main (args: Array[String]) {
    
    if (args.length < 1) {
      println ("Usage: SparkStreamContext <host> [<temp directory>]")
      System.exit(1)
    }
    
    val ssc = new SparkStreamContext(args(0), "WordCountTrivialWindow")
    if (args.length > 1) {
      ssc.setTempDir(args(1))
    }
    val sentences = ssc.readNetworkStream[String]("Sentences", Array("localhost:55119"), 1000)
    /*sentences.print*/
    
    val words = sentences.flatMap(_.split(" "))
   
    /*val counts = words.map(x => (x, 1)).reduceByKey(_ + _, 1)*/
    /*counts.print*/
    
    def add(v1: Int, v2: Int) = (v1 + v2) 
    def subtract(v1: Int, v2: Int) = (v1 - v2) 
    
    val windowedCounts = words.map(x => (x, 1)).window(Seconds(5), Seconds(1)).reduceByKey(add _, 1)
    /*windowedCounts.print */
   
    def topK(data: Seq[(String, Int)], k: Int): Array[(String, Int)] = {
      implicit val countOrdering = new Ordering[(String, Int)] {
        override def compare(count1: (String, Int), count2: (String, Int)): Int = {
          count2._2 - count1._2
        }
      }
      val array = data.toArray
      Sorting.quickSort(array)
      array.take(k)
    }
    
    val k = 10
    val topKWindowedCounts = windowedCounts.glom.flatMap(topK(_, k)).collect.flatMap(topK(_, k))
    topKWindowedCounts.print

    ssc.run
  }
}

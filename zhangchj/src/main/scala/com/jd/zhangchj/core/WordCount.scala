
package com.jd.zhangchj.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val logFile: String = "/home/zhangchj/sources/spark/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
  /*  val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")*/
    val wordCounts = logData.flatMap(line => line.split(" ")).
      map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCounts.foreach(println(_))

    sc.stop()
    //val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
  }
}

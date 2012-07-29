package spark.stream

import spark._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.io.Source

import java.net.InetSocketAddress

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

object SentenceFileGenerator {

  def printUsage () {
    println ("Usage: SentenceFileGenerator <master> <target directory> <# partitions> <sentence file> [<sentences per second>]")
    System.exit(0)
  }

  def main (args: Array[String]) {
    if (args.length < 4) {
      printUsage
    }
    
    val master = args(0)
    val fs = new Path(args(1)).getFileSystem(new Configuration())
    val targetDirectory = new Path(args(1)).makeQualified(fs)
    val numPartitions = args(2).toInt
    val sentenceFile = args(3)
    val sentencesPerSecond = { 
      if (args.length > 4) args(4).toInt
      else 10
    }
    
    val source = Source.fromFile(sentenceFile)
    val lines = source.mkString.split ("\n").toArray
    source.close ()
    println("Read " + lines.length + " lines from file " + sentenceFile)

    val sentences = {
      val buffer = ArrayBuffer[String]()
      val random = new Random()
      var i = 0
      while (i < sentencesPerSecond) {
        buffer += lines(random.nextInt(lines.length))
        i += 1 
      }
      buffer.toArray
    }
    println("Generated " + sentences.length + " sentences")
    
    val sc = new SparkContext(master, "SentenceFileGenerator")
    val sentencesRDD = sc.parallelize(sentences, numPartitions)

    val tempDirectory = new Path(targetDirectory, "_tmp")

    fs.mkdirs(targetDirectory)
    fs.mkdirs(tempDirectory)

    var saveTimeMillis = System.currentTimeMillis
    try {
      while (true) {
        val newDir = new Path(targetDirectory, "Sentences-" + saveTimeMillis)
        val tmpNewDir = new Path(tempDirectory, "Sentences-" + saveTimeMillis) 
        println("Writing to file " + newDir)
        sentencesRDD.saveAsTextFile(tmpNewDir.toString)
        fs.rename(tmpNewDir, newDir)
        saveTimeMillis += 1000
        val sleepTimeMillis = {
          val currentTimeMillis = System.currentTimeMillis
          if (saveTimeMillis < currentTimeMillis) {
            0 
          } else {
            saveTimeMillis - currentTimeMillis
          }
        }
        println("Sleeping for " + sleepTimeMillis + " ms")
        Thread.sleep(sleepTimeMillis)
      }
    } catch {
      case e: Exception => 
    }
  }
}
  




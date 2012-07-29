package spark.stream

import SparkStreamContext._

import scala.util.Sorting

import spark.SparkEnv
import spark.SparkContext
import spark.storage.StorageLevel
import spark.network.Message
import spark.network.ConnectionManagerId

import java.nio.ByteBuffer

object GrepCount2 {

  def startSparkEnvs(sc: SparkContext) {
  
    val dummy = sc.parallelize(0 to 1000, 100).persist(StorageLevel.DISK_AND_MEMORY)
    sc.runJob(dummy, (_: Iterator[Int]) => {})

    println("SparkEnvs started")
    Thread.sleep(1000)
    /*sc.runJob(sc.parallelize(0 to 1000, 100), (_: Iterator[Int]) => {})*/
  }

  def warmConnectionManagers(sc: SparkContext) {
    val slaveConnManagerIds = sc.parallelize(0 to 100, 100).map(
        i => SparkEnv.get.connectionManager.id).collect().distinct
    println("\nSlave ConnectionManagerIds")
    slaveConnManagerIds.foreach(println)
    println

    Thread.sleep(1000)
    val numSlaves = slaveConnManagerIds.size
    val count = 3
    val size = 5 * 1024  * 1024 
    val iterations = (500 * 1024 * 1024 / (numSlaves * size)).toInt
    println("count = " + count + ", size = " + size + ", iterations = " + iterations)
    
    (0 until count).foreach(i => {
      val resultStrs = sc.parallelize(0 until numSlaves, numSlaves).map(i => {
        val connManager = SparkEnv.get.connectionManager
        val thisConnManagerId = connManager.id 
        /*connManager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => { 
          println("Received [" + msg + "] from [" + id + "]")
          None
        })*/


        val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
        buffer.flip
        
        val startTime = System.currentTimeMillis  
        val futures = (0 until iterations).map(i => {
          slaveConnManagerIds.filter(_ != thisConnManagerId).map(slaveConnManagerId => {
            val bufferMessage = Message.createBufferMessage(buffer.duplicate)
            println("Sending [" + bufferMessage + "] to [" + slaveConnManagerId + "]")
            connManager.sendMessageReliably(slaveConnManagerId, bufferMessage)
          })
        }).flatMap(x => x) 
        val results = futures.map(f => f())
        val finishTime = System.currentTimeMillis
        
        
        val mb = size * results.size / 1024.0 / 1024.0
        val ms = finishTime - startTime
        
        val resultStr = "Sent " + mb + " MB in " + ms + " ms at " + (mb / ms * 1000.0) + " MB/s"
        println(resultStr)
        System.gc()
        resultStr
      }).collect()
      
      println("---------------------") 
      println("Run " + i) 
      resultStrs.foreach(println)
      println("---------------------") 
    })
  }
  

  def main (args: Array[String]) {
    
    if (args.length < 2) {
      println ("Usage: GrepCount2 <host> <# sentence streams>")
      System.exit(1)
    }

    val ssc = new SparkStreamContext(args(0), "GrepCount2")

    val numSentenceStreams = if (args.length > 1) args(1).toInt else 1
    if (args.length > 2) {
      ssc.setTempDir(args(2))
    }
   
    /*startSparkEnvs(ssc.sc)*/
    warmConnectionManagers(ssc.sc)

    val sentences = new UnifiedRDS(
      (1 to numSentenceStreams).map(i => ssc.readTestStream("Sentences-"+i, 500)).toArray
    )
    
    val matching = sentences.filter(_.contains("light"))
    matching.foreachRDD(rdd => println(rdd.count))

    ssc.run
  }
}





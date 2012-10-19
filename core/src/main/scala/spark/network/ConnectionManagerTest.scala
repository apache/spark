package spark.network

import spark._
import spark.SparkContext._

import scala.io.Source

import java.nio.ByteBuffer
import java.net.InetAddress

import akka.dispatch.Await
import akka.util.duration._

private[spark] object ConnectionManagerTest extends Logging{
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: ConnectionManagerTest <mesos cluster> <slaves file>")
      System.exit(1)
    }
    
    if (args(0).startsWith("local")) {
      println("This runs only on a mesos cluster")
    }
    
    val sc = new SparkContext(args(0), "ConnectionManagerTest")
    val slavesFile = Source.fromFile(args(1))
    val slaves = slavesFile.mkString.split("\n")
    slavesFile.close()

    /*println("Slaves")*/
    /*slaves.foreach(println)*/
   
    val slaveConnManagerIds = sc.parallelize(0 until slaves.length, slaves.length).map(
        i => SparkEnv.get.connectionManager.id).collect()
    println("\nSlave ConnectionManagerIds")
    slaveConnManagerIds.foreach(println)
    println

    val count = 10
    (0 until count).foreach(i => {
      val resultStrs = sc.parallelize(0 until slaves.length, slaves.length).map(i => {
        val connManager = SparkEnv.get.connectionManager
        val thisConnManagerId = connManager.id 
        connManager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => { 
          logInfo("Received [" + msg + "] from [" + id + "]")
          None
        })

        val size =  100 * 1024  * 1024 
        val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
        buffer.flip
        
        val startTime = System.currentTimeMillis  
        val futures = slaveConnManagerIds.filter(_ != thisConnManagerId).map(slaveConnManagerId => {
          val bufferMessage = Message.createBufferMessage(buffer.duplicate)
          logInfo("Sending [" + bufferMessage + "] to [" + slaveConnManagerId + "]")
          connManager.sendMessageReliably(slaveConnManagerId, bufferMessage)
        })
        val results = futures.map(f => Await.result(f, 1.second))
        val finishTime = System.currentTimeMillis
        Thread.sleep(5000)
        
        val mb = size * results.size / 1024.0 / 1024.0
        val ms = finishTime - startTime
        val resultStr = "Sent " + mb + " MB in " + ms + " ms at " + (mb / ms * 1000.0) + " MB/s"
        logInfo(resultStr)
        resultStr
      }).collect()
      
      println("---------------------") 
      println("Run " + i) 
      resultStrs.foreach(println)
      println("---------------------") 
    })
  }
}


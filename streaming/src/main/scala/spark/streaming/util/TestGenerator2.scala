package spark.streaming.util

import scala.util.Random
import scala.io.Source
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

import java.io.{DataOutputStream, ByteArrayOutputStream, DataInputStream}
import java.net.Socket

object TestGenerator2 {

  def printUsage {
    println ("Usage: SentenceGenerator <target IP> <target port> <sentence file> [<sentences per second>]")
    System.exit(0)
  }

  def sendSentences(streamReceiverHost: String, streamReceiverPort: Int, numSentences: Int, bytes: Array[Byte], intervalTime: Long){
    try {
      println("Connecting to " + streamReceiverHost + ":" + streamReceiverPort)
      val socket = new Socket(streamReceiverHost, streamReceiverPort)

      println("Sending " + numSentences+ " sentences / " + (bytes.length / 1024.0 / 1024.0) + " MB per " + intervalTime + " ms to " + streamReceiverHost + ":" + streamReceiverPort )
      val currentTime = System.currentTimeMillis
      var targetTime = (currentTime / intervalTime + 1).toLong * intervalTime 
      Thread.sleep(targetTime - currentTime)

      while(true) {
        val startTime = System.currentTimeMillis()
        println("Sending at " + startTime + " ms with delay of " + (startTime - targetTime) + " ms")
        val socketOutputStream = socket.getOutputStream
        val parts = 10
        (0 until parts).foreach(i => {
          val partStartTime = System.currentTimeMillis
          
          val offset = (i * bytes.length / parts).toInt
          val len = math.min(((i + 1) * bytes.length / parts).toInt - offset, bytes.length) 
          socketOutputStream.write(bytes, offset, len)
          socketOutputStream.flush()
          val partFinishTime = System.currentTimeMillis
          println("Sending part " + i + " of " + len + " bytes took " + (partFinishTime - partStartTime) + " ms") 
          val sleepTime = math.max(0, 1000 / parts - (partFinishTime - partStartTime) - 1)
          Thread.sleep(sleepTime) 
        })

        socketOutputStream.flush()
        /*val socketInputStream = new DataInputStream(socket.getInputStream)*/
        /*val reply = socketInputStream.readUTF()*/
        val finishTime = System.currentTimeMillis()
        println ("Sent " + bytes.length + " bytes in " + (finishTime - startTime) + " ms for interval [" + targetTime + ", " + (targetTime + intervalTime) + "]")
        /*println("Received = " + reply)*/
        targetTime = targetTime + intervalTime 
        val sleepTime = (targetTime - finishTime) + 10
        if (sleepTime > 0) {
          println("Sleeping for " + sleepTime + " ms")
          Thread.sleep(sleepTime)
        } else {
          println("############################")
          println("###### Skipping sleep ######")
          println("############################")
        }
      }
    } catch {
      case e: Exception =>  println(e)  
    }
    println("Stopped sending")
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      printUsage
    }

    val streamReceiverHost = args(0) 
    val streamReceiverPort = args(1).toInt
    val sentenceFile = args(2)
    val intervalTime = args(3).toLong 
    val sentencesPerInterval = if (args.length > 4) args(4).toInt else 0
    
    println("Reading the file " + sentenceFile)
    val source = Source.fromFile(sentenceFile)
    val lines = source.mkString.split ("\n")
    source.close()

    val numSentences = if (sentencesPerInterval <= 0) {
      lines.length
    } else {
      sentencesPerInterval
    }

    println("Generating sentences")
    val sentences: Array[String] = if (numSentences <= lines.length) {
      lines.take(numSentences).toArray
    } else {
      (0 until numSentences).map(i => lines(i % lines.length)).toArray
    }

    println("Converting to byte array")
    val byteStream = new ByteArrayOutputStream()
    val stringDataStream = new DataOutputStream(byteStream)
    /*stringDataStream.writeInt(sentences.size)*/
    sentences.foreach(stringDataStream.writeUTF)
    val bytes = byteStream.toByteArray()
    stringDataStream.close()
    println("Generated array of " + bytes.length + " bytes")
    
    /*while(true) { */
      sendSentences(streamReceiverHost, streamReceiverPort, numSentences, bytes, intervalTime)
      /*println("Sleeping for 5 seconds")*/
      /*Thread.sleep(5000)*/
      /*System.gc()*/
    /*}*/
  }
}
  



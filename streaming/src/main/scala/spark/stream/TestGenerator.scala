package spark.stream

import scala.util.Random
import scala.io.Source
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

import java.net.InetSocketAddress


object TestGenerator {

  def printUsage {
    println ("Usage: SentenceGenerator <target IP> <target port> <sentence file> [<sentences per second>]")
    System.exit(0)
  }
  /*
  def generateRandomSentences(lines: Array[String], sentencesPerSecond: Int, streamReceiver: AbstractActor) {
    val sleepBetweenSentences = 1000.0 /  sentencesPerSecond.toDouble  - 1
    val random = new Random ()
      
    try {
      var lastPrintTime = System.currentTimeMillis()
      var count = 0
      while(true) {
        streamReceiver ! lines(random.nextInt(lines.length))
        count += 1
        if (System.currentTimeMillis - lastPrintTime >= 1000) {
          println (count + " sentences sent last second")
          count = 0
          lastPrintTime = System.currentTimeMillis
        }
        Thread.sleep(sleepBetweenSentences.toLong)
      }
    } catch {
      case e: Exception => 
    }
  }*/

  def generateSameSentences(lines: Array[String], sentencesPerSecond: Int, streamReceiver: AbstractActor) {
    try {
      val numSentences = if (sentencesPerSecond <= 0) {
        lines.length
      } else {
        sentencesPerSecond
      }
      val sentences = lines.take(numSentences).toArray

      var nextSendingTime = System.currentTimeMillis()
      val sendAsArray = true
      while(true) {
        if (sendAsArray) {
          println("Sending as array")
          streamReceiver !? sentences
        } else {
          println("Sending individually")
          sentences.foreach(sentence => {
            streamReceiver !? sentence
          })
        }
        println ("Sent " + numSentences + " sentences in " + (System.currentTimeMillis - nextSendingTime) + " ms")
        nextSendingTime += 1000
        val sleepTime = nextSendingTime - System.currentTimeMillis
        if (sleepTime > 0) {
          println ("Sleeping for " + sleepTime + " ms")
          Thread.sleep(sleepTime)
        }
      }
    } catch {
      case e: Exception => 
    }
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      printUsage
    }
    
    val generateRandomly = false

    val streamReceiverIP = args(0) 
    val streamReceiverPort = args(1).toInt
    val sentenceFile = args(2)
    val sentencesPerSecond = if (args.length > 3) args(3).toInt else 10
    val sentenceInputName = if (args.length > 4) args(4) else "Sentences"   
    
    println("Sending " + sentencesPerSecond + " sentences per second to " +
      streamReceiverIP + ":" + streamReceiverPort + "/NetworkStreamReceiver-" + sentenceInputName)
    val source = Source.fromFile(sentenceFile)
    val lines = source.mkString.split ("\n")
    source.close ()
   
    val streamReceiver = select(
      Node(streamReceiverIP, streamReceiverPort), 
      Symbol("NetworkStreamReceiver-" + sentenceInputName))  
    if (generateRandomly) {
      /*generateRandomSentences(lines, sentencesPerSecond, streamReceiver)*/
    } else {
      generateSameSentences(lines, sentencesPerSecond, streamReceiver)
    }
  }
}
  



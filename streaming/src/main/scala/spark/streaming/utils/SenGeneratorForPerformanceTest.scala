package spark.streaming

import scala.util.Random
import scala.io.Source
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

import java.net.InetSocketAddress

/*import akka.actor.Actor._*/
/*import akka.actor.ActorRef*/


object SenGeneratorForPerformanceTest {

  def printUsage () {
    println ("Usage: SentenceGenerator <target IP> <target port> <sentence file> [<sentences per second>]")
    System.exit(0)
  }

  def main (args: Array[String]) {
    if (args.length < 3) {
      printUsage
    }
    
    val inputManagerIP = args(0) 
    val inputManagerPort = args(1).toInt
    val sentenceFile = args(2)
    val sentencesPerSecond = { 
      if (args.length > 3) args(3).toInt
      else 10
    }
    
    val source = Source.fromFile(sentenceFile)
    val lines = source.mkString.split ("\n")
    source.close ()
    
    try {
      /*val inputManager = remote.actorFor("InputReceiver-Sentences",*/
      /*    inputManagerIP, inputManagerPort)*/
      val inputManager = select(Node(inputManagerIP, inputManagerPort), Symbol("InputReceiver-Sentences"))  
      val sleepBetweenSentences = 1000.0 /  sentencesPerSecond.toDouble  - 1
      val random = new Random ()
      println ("Sending " + sentencesPerSecond + " sentences per second to " + inputManagerIP + ":" + inputManagerPort)
      var lastPrintTime = System.currentTimeMillis()
      var count = 0
      
      while (true) {
        /*if (!inputManager.tryTell (lines (random.nextInt (lines.length))))*/
          /*throw new Exception ("disconnected")*/
//        inputManager ! lines (random.nextInt (lines.length))
        for (i <- 0 to sentencesPerSecond) inputManager ! lines (0)
        println(System.currentTimeMillis / 1000 + " s")
/*        count += 1

        if (System.currentTimeMillis - lastPrintTime >= 1000) {
          println (count + " sentences sent last second")
          count = 0
          lastPrintTime = System.currentTimeMillis
        }
        
        Thread.sleep (sleepBetweenSentences.toLong)
*/
        val currentMs = System.currentTimeMillis / 1000;
        Thread.sleep ((currentMs * 1000 + 1000) - System.currentTimeMillis)
      }
    } catch {
      case e: Exception => 
      /*Thread.sleep (1000)*/
    }
  }
}
  




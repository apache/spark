package spark.streaming.util

import spark.Logging

import scala.util.Random
import scala.io.Source
import scala.collection.mutable.{ArrayBuffer, Queue}

import java.net._
import java.io._
import java.nio._
import java.nio.charset._
import java.nio.channels._

import it.unimi.dsi.fastutil.io._

class TestGenerator4(targetHost: String, targetPort: Int, sentenceFile: String, intervalDuration: Long, sentencesPerInterval: Int)
extends Logging {

  class SendingConnectionHandler(host: String, port: Int, generator: TestGenerator4)
  extends ConnectionHandler(host, port, true) {

    val buffers = new ArrayBuffer[ByteBuffer] 
    val newBuffers = new Queue[ByteBuffer]
    var activeKey: SelectionKey = null

    def send(buffer: ByteBuffer) {
      logDebug("Sending: " + buffer)
      newBuffers.synchronized {
        newBuffers.enqueue(buffer)
      }
      selector.wakeup()
      buffer.synchronized {
        buffer.wait()
      }
    }

    override def ready(key: SelectionKey) {
      logDebug("Ready")
      activeKey = key
      val channel = key.channel.asInstanceOf[SocketChannel]
      channel.register(selector, SelectionKey.OP_WRITE)
      generator.startSending()
    }

    override def preSelect() {
      newBuffers.synchronized {
        while(!newBuffers.isEmpty) {
          val buffer = newBuffers.dequeue 
          buffers += buffer
          logDebug("Added: " + buffer) 
          changeInterest(activeKey, SelectionKey.OP_WRITE)
        }
      }
    }

    override def write(key: SelectionKey) {
      try {
        /*while(true) {*/
          val channel = key.channel.asInstanceOf[SocketChannel]
          if (buffers.size > 0) {
            val buffer = buffers(0)
            val newBuffer = buffer.slice()
            newBuffer.limit(math.min(newBuffer.remaining, 32768))
            val bytesWritten = channel.write(newBuffer)
            buffer.position(buffer.position + bytesWritten)
            if (bytesWritten == 0) return 
            if (buffer.remaining == 0) {
              buffers -= buffer
              buffer.synchronized {
                buffer.notify()
              }
            }
            /*changeInterest(key, SelectionKey.OP_WRITE)*/
          } else {
            changeInterest(key, 0)
          }
        /*}*/
      } catch {
        case e: IOException => {
          if (e.toString.contains("pipe") || e.toString.contains("reset")) {
            logError("Connection broken")
          } else {
            logError("Connection error", e)
          }
          close(key)
        }
      }
    }

    override def close(key: SelectionKey) {
      buffers.clear()
      super.close(key)
    }
  }

  initLogging()

  val connectionHandler = new SendingConnectionHandler(targetHost, targetPort, this)
  var sendingThread: Thread = null
  var sendCount = 0
  val sendBatches = 5

  def run() {
    logInfo("Connection handler started")
    connectionHandler.start()
    connectionHandler.join()
    if (sendingThread != null && !sendingThread.isInterrupted) {
      sendingThread.interrupt
    }
    logInfo("Connection handler stopped")
  }

  def startSending() {
    sendingThread = new Thread() {
      override def run() {
        logInfo("STARTING TO SEND")
        sendSentences()
        logInfo("SENDING STOPPED AFTER " + sendCount)
        connectionHandler.interrupt()
      }
    }
    sendingThread.start() 
  }

  def stopSending() {
    sendingThread.interrupt()
  }

  def sendSentences() {
    logInfo("Reading the file " + sentenceFile)
    val source = Source.fromFile(sentenceFile)
    val lines = source.mkString.split ("\n")
    source.close()

    val numSentences = if (sentencesPerInterval <= 0) {
      lines.length
    } else {
      sentencesPerInterval
    }

    logInfo("Generating sentence buffer")
    val sentences: Array[String] = if (numSentences <= lines.length) {
      lines.take(numSentences).toArray
    } else {
      (0 until numSentences).map(i => lines(i % lines.length)).toArray
    }
   
    /*
    val sentences: Array[String] = if (numSentences <= lines.length) {
      lines.take((numSentences / sendBatches).toInt).toArray
    } else {
      (0 until (numSentences/sendBatches)).map(i => lines(i % lines.length)).toArray
    }*/

    
    val serializer = new spark.KryoSerializer().newInstance()
    val byteStream = new FastByteArrayOutputStream(100 * 1024 * 1024)
    serializer.serializeStream(byteStream).writeAll(sentences.toIterator.asInstanceOf[Iterator[Any]]).close()
    byteStream.trim()
    val sentenceBuffer = ByteBuffer.wrap(byteStream.array)

    logInfo("Sending " + numSentences+ " sentences / " + sentenceBuffer.limit + " bytes  per " + intervalDuration + " ms to " + targetHost + ":" + targetPort )
    val currentTime = System.currentTimeMillis
    var targetTime = (currentTime / intervalDuration + 1).toLong * intervalDuration 
    Thread.sleep(targetTime - currentTime)

    val totalBytes = sentenceBuffer.limit 
      
    while(true) {
      val batchesInCurrentInterval =  sendBatches // if (sendCount < 10) 1 else sendBatches

      val startTime = System.currentTimeMillis()
      logDebug("Sending # " + sendCount + " at " + startTime + " ms with delay of " + (startTime - targetTime) + " ms")
      
      (0 until batchesInCurrentInterval).foreach(i => {
        try { 
          val position = (i * totalBytes / sendBatches).toInt
          val limit = if (i == sendBatches - 1) {
            totalBytes
          } else {
            ((i + 1) * totalBytes / sendBatches).toInt - 1
          }

          val partStartTime = System.currentTimeMillis
          sentenceBuffer.limit(limit)
          connectionHandler.send(sentenceBuffer) 
          val partFinishTime = System.currentTimeMillis
          val sleepTime = math.max(0, intervalDuration / sendBatches - (partFinishTime - partStartTime) - 1)
          Thread.sleep(sleepTime) 
        
        } catch {
          case ie: InterruptedException => return  
          case e: Exception => e.printStackTrace()
        }
      })
      sentenceBuffer.rewind()

      val finishTime = System.currentTimeMillis()
      /*logInfo ("Sent " + sentenceBuffer.limit + " bytes in " + (finishTime - startTime) + " ms")*/
      targetTime = targetTime + intervalDuration //+ (if (sendCount < 3) 1000 else 0)

      val sleepTime = (targetTime - finishTime) + 20 
      if (sleepTime > 0) {
        logInfo("Sleeping for " + sleepTime + " ms")
        Thread.sleep(sleepTime)
      } else {
        logInfo("###### Skipping sleep ######")
      }
      if (Thread.currentThread.isInterrupted) {
        return
      }
      sendCount += 1
    }
  }
}

object TestGenerator4 {
  def printUsage {
    println("Usage: TestGenerator4 <target IP> <target port> <sentence file> <interval duration> [<sentences per second>]")
    System.exit(0)
  }
  
  def main(args: Array[String]) {
    println("GENERATOR STARTED")
    if (args.length < 4) {
      printUsage
    }


    val streamReceiverHost = args(0) 
    val streamReceiverPort = args(1).toInt
    val sentenceFile = args(2)
    val intervalDuration = args(3).toLong 
    val sentencesPerInterval = if (args.length > 4) args(4).toInt else 0

    while(true) {
      val generator = new TestGenerator4(streamReceiverHost, streamReceiverPort, sentenceFile, intervalDuration, sentencesPerInterval)
      generator.run()
      Thread.sleep(2000)
    }
    println("GENERATOR STOPPED")
  }
}

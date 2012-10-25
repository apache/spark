package spark.streaming

import java.net.{SocketException, Socket, ServerSocket}
import java.io.{File, BufferedWriter, OutputStreamWriter}
import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}
import collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import util.ManualClock
import spark.storage.StorageLevel
import spark.Logging
import scala.util.Random
import org.apache.commons.io.FileUtils


class InputStreamsSuite extends TestSuiteBase {

  test("network input stream") {
    // Start the server
    val serverPort = 9999
    val server = new TestServer(9999)
    server.start()

    // Set up the streaming context and input streams
    val ssc = new StreamingContext(master, framework)
    ssc.setBatchDuration(batchDuration)
    val networkStream = ssc.networkTextStream("localhost", serverPort, StorageLevel.DISK_AND_MEMORY)
    val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String  ]]
    val outputStream = new TestOutputStream(networkStream, outputBuffer)
    ssc.registerOutputStream(outputStream)
    ssc.start()

    // Feed data to the server to send to the Spark Streaming network receiver
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq(1, 2, 3, 4, 5)
    val expectedOutput = input.map(_.toString)
    for (i <- 0 until input.size) {
      server.send(input(i).toString + "\n")
      Thread.sleep(500)
      clock.addToTime(batchDuration.milliseconds)
    }
    val startTime = System.currentTimeMillis()
    while (outputBuffer.size < expectedOutput.size && System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
      logInfo("output.size = " + outputBuffer.size + ", expectedOutput.size = " + expectedOutput.size)
      Thread.sleep(100)
    }
    Thread.sleep(1000)
    val timeTaken = System.currentTimeMillis() - startTime
    assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
    logInfo("Stopping server")
    server.stop()
    logInfo("Stopping context")
    ssc.stop()

    // Verify whether data received by Spark Streaming was as expected
    assert(outputBuffer.size === expectedOutput.size)
    for (i <- 0 until outputBuffer.size) {
      assert(outputBuffer(i).size === 1)
      assert(outputBuffer(i).head === expectedOutput(i))
    }
  }

  test("file input stream") {
    // Create a temporary directory
    val dir = {
      var temp = File.createTempFile(".temp.", Random.nextInt().toString)
      temp.delete()
      temp.mkdirs()
      temp.deleteOnExit()
      println("Created temp dir " + temp)
      temp
    }

    // Set up the streaming context and input streams
    val ssc = new StreamingContext(master, framework)
    ssc.setBatchDuration(batchDuration)
    val filestream = ssc.textFileStream(dir.toString)
    val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String  ]]
    val outputStream = new TestOutputStream(filestream, outputBuffer)
    ssc.registerOutputStream(outputStream)
    ssc.start()

    // Create files in the temporary directory so that Spark Streaming can read data from it
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq(1, 2, 3, 4, 5)
    val expectedOutput = input.map(_.toString)
    Thread.sleep(1000)
    for (i <- 0 until input.size) {
      FileUtils.writeStringToFile(new File(dir, i.toString), input(i).toString + "\n")
      Thread.sleep(500)
      clock.addToTime(batchDuration.milliseconds)
      Thread.sleep(500)
    }
    val startTime = System.currentTimeMillis()
    while (outputBuffer.size < expectedOutput.size && System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
      println("output.size = " + outputBuffer.size + ", expectedOutput.size = " + expectedOutput.size)
      Thread.sleep(100)
    }
    Thread.sleep(1000)
    val timeTaken = System.currentTimeMillis() - startTime
    assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
    println("Stopping context")
    ssc.stop()

    // Verify whether data received by Spark Streaming was as expected
    assert(outputBuffer.size === expectedOutput.size)
    for (i <- 0 until outputBuffer.size) {
      assert(outputBuffer(i).size === 1)
      assert(outputBuffer(i).head === expectedOutput(i))
    }
  }
}


class TestServer(port: Int) extends Logging {

  val queue = new ArrayBlockingQueue[String](100)

  val serverSocket = new ServerSocket(port)

  val servingThread = new Thread() {
    override def run() {
      try {
        while(true) {
          logInfo("Accepting connections on port " + port)
          val clientSocket = serverSocket.accept()
          logInfo("New connection")
          try {
            clientSocket.setTcpNoDelay(true)
            val outputStream = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream))

            while(clientSocket.isConnected) {
              val msg = queue.poll(100, TimeUnit.MILLISECONDS)
              if (msg != null) {
                outputStream.write(msg)
                outputStream.flush()
                logInfo("Message '" + msg + "' sent")
              }
            }
          } catch {
            case e: SocketException => println(e)
          } finally {
            logInfo("Connection closed")
            if (!clientSocket.isClosed) clientSocket.close()
          }
        }
      } catch {
        case ie: InterruptedException =>

      } finally {
        serverSocket.close()
      }
    }
  }

  def start() { servingThread.start() }

  def send(msg: String) { queue.add(msg) }

  def stop() { servingThread.interrupt() }
}

object TestServer {
  def main(args: Array[String]) {
    val s = new TestServer(9999)
    s.start()
    while(true) {
      Thread.sleep(1000)
      s.send("hello")
    }
  }
}

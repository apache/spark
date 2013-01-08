package spark.streaming

import dstream.SparkFlumeEvent
import java.net.{InetSocketAddress, SocketException, Socket, ServerSocket}
import java.io.{File, BufferedWriter, OutputStreamWriter}
import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}
import collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import util.ManualClock
import spark.storage.StorageLevel
import spark.Logging
import scala.util.Random
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter
import org.apache.flume.source.avro.AvroSourceProtocol
import org.apache.flume.source.avro.AvroFlumeEvent
import org.apache.flume.source.avro.Status
import org.apache.avro.ipc.{specific, NettyTransceiver}
import org.apache.avro.ipc.specific.SpecificRequestor
import java.nio.ByteBuffer
import collection.JavaConversions._
import java.nio.charset.Charset

class InputStreamsSuite extends TestSuiteBase with BeforeAndAfter {
    
  System.setProperty("spark.streaming.clock", "spark.streaming.util.ManualClock")

  val testPort = 9999
  var testServer: TestServer = null
  var testDir: File = null

  override def checkpointDir = "checkpoint"

  after {
    FileUtils.deleteDirectory(new File(checkpointDir))
    if (testServer != null) {
      testServer.stop()
      testServer = null
    }
    if (testDir != null && testDir.exists()) {
      FileUtils.deleteDirectory(testDir)
      testDir = null
    }
  }

  test("network input stream") {
    // Start the server
    testServer = new TestServer(testPort)
    testServer.start()

    // Set up the streaming context and input streams
    val ssc = new StreamingContext(master, framework, batchDuration)
    val networkStream = ssc.networkTextStream("localhost", testPort, StorageLevel.MEMORY_AND_DISK)
    val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String  ]]
    val outputStream = new TestOutputStream(networkStream, outputBuffer)
    def output = outputBuffer.flatMap(x => x)
    ssc.registerOutputStream(outputStream)
    ssc.start()

    // Feed data to the server to send to the network receiver
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq(1, 2, 3, 4, 5)
    val expectedOutput = input.map(_.toString)
    Thread.sleep(1000)
    for (i <- 0 until input.size) {
      testServer.send(input(i).toString + "\n")
      Thread.sleep(500)
      clock.addToTime(batchDuration.milliseconds)
    }
    Thread.sleep(1000)
    logInfo("Stopping server")
    testServer.stop()
    logInfo("Stopping context")
    ssc.stop()

    // Verify whether data received was as expected
    logInfo("--------------------------------")
    logInfo("output.size = " + outputBuffer.size)
    logInfo("output")
    outputBuffer.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Verify whether all the elements received are as expected
    // (whether the elements were received one in each interval is not verified)
    assert(output.size === expectedOutput.size)
    for (i <- 0 until output.size) {
      assert(output(i) === expectedOutput(i))
    }
  }

  test("network input stream with checkpoint") {
    // Start the server
    testServer = new TestServer(testPort)
    testServer.start()

    // Set up the streaming context and input streams
    var ssc = new StreamingContext(master, framework, batchDuration)
    ssc.checkpoint(checkpointDir, checkpointInterval)
    val networkStream = ssc.networkTextStream("localhost", testPort, StorageLevel.MEMORY_AND_DISK)
    var outputStream = new TestOutputStream(networkStream, new ArrayBuffer[Seq[String]])
    ssc.registerOutputStream(outputStream)
    ssc.start()

    // Feed data to the server to send to the network receiver
    var clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    for (i <- Seq(1, 2, 3)) {
      testServer.send(i.toString + "\n")
      Thread.sleep(100)
      clock.addToTime(batchDuration.milliseconds)
    }
    Thread.sleep(500)
    assert(outputStream.output.size > 0)
    ssc.stop()

    // Restart stream computation from checkpoint and feed more data to see whether
    // they are being received and processed
    logInfo("*********** RESTARTING ************")
    ssc = new StreamingContext(checkpointDir)
    ssc.start()
    clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    for (i <- Seq(4, 5, 6)) {
      testServer.send(i.toString + "\n")
      Thread.sleep(100)
      clock.addToTime(batchDuration.milliseconds)
    }
    Thread.sleep(500)
    outputStream = ssc.graph.getOutputStreams().head.asInstanceOf[TestOutputStream[String]]
    assert(outputStream.output.size > 0)
    ssc.stop()
  }

  test("flume input stream") {
    // Set up the streaming context and input streams
    val ssc = new StreamingContext(master, framework, batchDuration)
    val flumeStream = ssc.flumeStream("localhost", 33333, StorageLevel.MEMORY_AND_DISK)
    val outputBuffer = new ArrayBuffer[Seq[SparkFlumeEvent]]
      with SynchronizedBuffer[Seq[SparkFlumeEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputBuffer)
    ssc.registerOutputStream(outputStream)
    ssc.start()

    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq(1, 2, 3, 4, 5)

    val transceiver = new NettyTransceiver(new InetSocketAddress("localhost", 33333));
    val client = SpecificRequestor.getClient(
      classOf[AvroSourceProtocol], transceiver);

    for (i <- 0 until input.size) {
      val event = new AvroFlumeEvent
      event.setBody(ByteBuffer.wrap(input(i).toString.getBytes()))
      event.setHeaders(Map[CharSequence, CharSequence]("test" -> "header"))
      client.append(event)
      Thread.sleep(500)
      clock.addToTime(batchDuration.milliseconds)
    }

    val startTime = System.currentTimeMillis()
    while (outputBuffer.size < input.size && System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
      logInfo("output.size = " + outputBuffer.size + ", input.size = " + input.size)
      Thread.sleep(100)
    }
    Thread.sleep(1000)
    val timeTaken = System.currentTimeMillis() - startTime
    assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
    logInfo("Stopping context")
    ssc.stop()

    val decoder = Charset.forName("UTF-8").newDecoder()

    assert(outputBuffer.size === input.length)
    for (i <- 0 until outputBuffer.size) {
      assert(outputBuffer(i).size === 1)
      val str = decoder.decode(outputBuffer(i).head.event.getBody)
      assert(str.toString === input(i).toString)
      assert(outputBuffer(i).head.event.getHeaders.get("test") === "header")
    }
  }

  test("file input stream") {

    // Create a temporary directory
    testDir = {
      var temp = File.createTempFile(".temp.", Random.nextInt().toString)
      temp.delete()
      temp.mkdirs()
      logInfo("Created temp dir " + temp)
      temp
    }

    // Set up the streaming context and input streams
    val ssc = new StreamingContext(master, framework, batchDuration)
    val filestream = ssc.textFileStream(testDir.toString)
    val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String]]
    def output = outputBuffer.flatMap(x => x)
    val outputStream = new TestOutputStream(filestream, outputBuffer)
    ssc.registerOutputStream(outputStream)
    ssc.start()

    // Create files in the temporary directory so that Spark Streaming can read data from it
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq(1, 2, 3, 4, 5)
    val expectedOutput = input.map(_.toString)
    Thread.sleep(1000)
    for (i <- 0 until input.size) {
      FileUtils.writeStringToFile(new File(testDir, i.toString), input(i).toString + "\n")
      Thread.sleep(500)
      clock.addToTime(batchDuration.milliseconds)
      //Thread.sleep(100)
    }
    val startTime = System.currentTimeMillis()
    /*while (output.size < expectedOutput.size && System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
      logInfo("output.size = " + output.size + ", expectedOutput.size = " + expectedOutput.size)
      Thread.sleep(100)
    }*/
    Thread.sleep(1000)
    val timeTaken = System.currentTimeMillis() - startTime
    assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
    logInfo("Stopping context")
    ssc.stop()

    // Verify whether data received by Spark Streaming was as expected
    logInfo("--------------------------------")
    logInfo("output.size = " + outputBuffer.size)
    logInfo("output")
    outputBuffer.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Verify whether all the elements received are as expected
    // (whether the elements were received one in each interval is not verified)
    assert(output.size === expectedOutput.size)
    for (i <- 0 until output.size) {
      assert(output(i).size === 1)
      assert(output(i).head.toString === expectedOutput(i))
    }
  }

  test("file input stream with checkpoint") {
    // Create a temporary directory
    testDir = {
      var temp = File.createTempFile(".temp.", Random.nextInt().toString)
      temp.delete()
      temp.mkdirs()
      logInfo("Created temp dir " + temp)
      temp
    }

    // Set up the streaming context and input streams
    var ssc = new StreamingContext(master, framework, batchDuration)
    ssc.checkpoint(checkpointDir, checkpointInterval)
    val filestream = ssc.textFileStream(testDir.toString)
    var outputStream = new TestOutputStream(filestream, new ArrayBuffer[Seq[String]])
    ssc.registerOutputStream(outputStream)
    ssc.start()

    // Create files and advance manual clock to process them
    var clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    Thread.sleep(1000)
    for (i <- Seq(1, 2, 3)) {
      FileUtils.writeStringToFile(new File(testDir, i.toString), i.toString + "\n")
      Thread.sleep(100)
      clock.addToTime(batchDuration.milliseconds)
    }
    Thread.sleep(500)
    logInfo("Output = " + outputStream.output.mkString(","))
    assert(outputStream.output.size > 0)
    ssc.stop()

    // Restart stream computation from checkpoint and create more files to see whether
    // they are being processed
    logInfo("*********** RESTARTING ************")
    ssc = new StreamingContext(checkpointDir)
    ssc.start()
    clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    Thread.sleep(500)
    for (i <- Seq(4, 5, 6)) {
      FileUtils.writeStringToFile(new File(testDir, i.toString), i.toString + "\n")
      Thread.sleep(100)
      clock.addToTime(batchDuration.milliseconds)
    }
    Thread.sleep(500)
    outputStream = ssc.graph.getOutputStreams().head.asInstanceOf[TestOutputStream[String]]
    logInfo("Output = " + outputStream.output.mkString(","))
    assert(outputStream.output.size > 0)
    ssc.stop()
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
            case e: SocketException => logError("TestServer error", e)
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

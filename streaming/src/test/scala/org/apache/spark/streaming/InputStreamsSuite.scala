/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming

import akka.actor.Actor
import akka.actor.IO
import akka.actor.IOManager
import akka.actor.Props
import akka.util.ByteString

import org.apache.spark.streaming.dstream.{NetworkReceiver, SparkFlumeEvent}
import java.net.{InetSocketAddress, SocketException, Socket, ServerSocket}
import java.io.{File, BufferedWriter, OutputStreamWriter}
import java.util.concurrent.{Executors, TimeUnit, ArrayBlockingQueue}
import collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import util.ManualClock
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receivers.Receiver
import org.apache.spark.{SparkContext, Logging}
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
import com.google.common.io.Files
import java.util.concurrent.atomic.AtomicInteger

class InputStreamsSuite extends TestSuiteBase with BeforeAndAfter {

  val testPort = 9999

  override def checkpointDir = "checkpoint"

  before {
    System.setProperty("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
  }

  after {
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  test("socket input stream") {
    // Start the server
    val testServer = new TestServer()
    testServer.start()

    // Set up the streaming context and input streams
    val ssc = new StreamingContext(master, framework, batchDuration)
    val networkStream = ssc.socketTextStream("localhost", testServer.port, StorageLevel.MEMORY_AND_DISK)
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


  test("flume input stream") {
    // Set up the streaming context and input streams
    val ssc = new StreamingContext(master, framework, batchDuration)
    val flumeStream = ssc.flumeStream("localhost", testPort, StorageLevel.MEMORY_AND_DISK)
    val outputBuffer = new ArrayBuffer[Seq[SparkFlumeEvent]]
      with SynchronizedBuffer[Seq[SparkFlumeEvent]]
    val outputStream = new TestOutputStream(flumeStream, outputBuffer)
    ssc.registerOutputStream(outputStream)
    ssc.start()

    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq(1, 2, 3, 4, 5)
    Thread.sleep(1000)
    val transceiver = new NettyTransceiver(new InetSocketAddress("localhost", testPort))
    val client = SpecificRequestor.getClient(
      classOf[AvroSourceProtocol], transceiver)

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
    // Disable manual clock as FileInputDStream does not work with manual clock
    System.clearProperty("spark.streaming.clock")

    // Set up the streaming context and input streams
    val testDir = Files.createTempDir()
    val ssc = new StreamingContext(master, framework, batchDuration)
    val fileStream = ssc.textFileStream(testDir.toString)
    val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String]]
    def output = outputBuffer.flatMap(x => x)
    val outputStream = new TestOutputStream(fileStream, outputBuffer)
    ssc.registerOutputStream(outputStream)
    ssc.start()

    // Create files in the temporary directory so that Spark Streaming can read data from it
    val input = Seq(1, 2, 3, 4, 5)
    val expectedOutput = input.map(_.toString)
    Thread.sleep(1000)
    for (i <- 0 until input.size) {
      val file = new File(testDir, i.toString)
      FileUtils.writeStringToFile(file, input(i).toString + "\n")
      logInfo("Created file " + file)
      Thread.sleep(batchDuration.milliseconds)
      Thread.sleep(1000)
    }
    val startTime = System.currentTimeMillis()
    Thread.sleep(1000)
    val timeTaken = System.currentTimeMillis() - startTime
    assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
    logInfo("Stopping context")
    ssc.stop()

    // Verify whether data received by Spark Streaming was as expected
    logInfo("--------------------------------")
    logInfo("output, size = " + outputBuffer.size)
    outputBuffer.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output, size = " + expectedOutput.size)
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Verify whether all the elements received are as expected
    // (whether the elements were received one in each interval is not verified)
    assert(output.toList === expectedOutput.toList)

    FileUtils.deleteDirectory(testDir)

    // Enable manual clock back again for other tests
    System.setProperty("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
  }


  test("actor input stream") {
    // Start the server
    val testServer = new TestServer()
    val port = testServer.port
    testServer.start()

    // Set up the streaming context and input streams
    val ssc = new StreamingContext(master, framework, batchDuration)
    val networkStream = ssc.actorStream[String](Props(new TestActor(port)), "TestActor",
      StorageLevel.MEMORY_AND_DISK) //Had to pass the local value of port to prevent from closing over entire scope
    val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String]]
    val outputStream = new TestOutputStream(networkStream, outputBuffer)
    def output = outputBuffer.flatMap(x => x)
    ssc.registerOutputStream(outputStream)
    ssc.start()

    // Feed data to the server to send to the network receiver
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = 1 to 9
    val expectedOutput = input.map(x => x.toString)
    Thread.sleep(1000)
    for (i <- 0 until input.size) {
      testServer.send(input(i).toString)
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

  test("kafka input stream") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    val topics = Map("my-topic" -> 1)
    val test1 = ssc.kafkaStream("localhost:12345", "group", topics)
    val test2 = ssc.kafkaStream("localhost:12345", "group", topics, StorageLevel.MEMORY_AND_DISK)

    // Test specifying decoder
    val kafkaParams = Map("zookeeper.connect"->"localhost:12345","group.id"->"consumer-group")
    val test3 = ssc.kafkaStream[
      String,
      String,
      kafka.serializer.StringDecoder,
      kafka.serializer.StringDecoder](kafkaParams, topics, StorageLevel.MEMORY_AND_DISK)
  }

  test("multi-thread receiver") {
    // set up the test receiver
    val numThreads = 10
    val numRecordsPerThread = 1000
    val numTotalRecords = numThreads * numRecordsPerThread
    val testReceiver = new MultiThreadTestReceiver(numThreads, numRecordsPerThread)
    MultiThreadTestReceiver.haveAllThreadsFinished = false

    // set up the network stream using the test receiver
    val ssc = new StreamingContext(master, framework, batchDuration)
    val networkStream = ssc.networkStream[Int](testReceiver)
    val countStream = networkStream.count
    val outputBuffer = new ArrayBuffer[Seq[Long]] with SynchronizedBuffer[Seq[Long]]
    val outputStream = new TestOutputStream(countStream, outputBuffer)
    def output = outputBuffer.flatMap(x => x)
    ssc.registerOutputStream(outputStream)
    ssc.start()

    // Let the data from the receiver be received
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val startTime = System.currentTimeMillis()
    while((!MultiThreadTestReceiver.haveAllThreadsFinished || output.sum < numTotalRecords) &&
      System.currentTimeMillis() - startTime < 5000) {
      Thread.sleep(100)
      clock.addToTime(batchDuration.milliseconds)
    }
    Thread.sleep(1000)
    logInfo("Stopping context")
    ssc.stop()

    // Verify whether data received was as expected
    logInfo("--------------------------------")
    logInfo("output.size = " + outputBuffer.size)
    logInfo("output")
    outputBuffer.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")
    assert(output.sum === numTotalRecords)
  }
}


/** This is a server to test the network input stream */
class TestServer() extends Logging {

  val queue = new ArrayBlockingQueue[String](100)

  val serverSocket = new ServerSocket(0)

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

  def port = serverSocket.getLocalPort
}

object TestServer {
  def main(args: Array[String]) {
    val s = new TestServer()
    s.start()
    while(true) {
      Thread.sleep(1000)
      s.send("hello")
    }
  }
}

/** This is an actor for testing actor input stream */
class TestActor(port: Int) extends Actor with Receiver {

  def bytesToString(byteString: ByteString) = byteString.utf8String

  override def preStart = IOManager(context.system).connect(new InetSocketAddress(port))

  def receive = {
    case IO.Read(socket, bytes) =>
      pushBlock(bytesToString(bytes))
  }
}

/** This is a receiver to test multiple threads inserting data using block generator */
class MultiThreadTestReceiver(numThreads: Int, numRecordsPerThread: Int)
  extends NetworkReceiver[Int] {
  lazy val executorPool = Executors.newFixedThreadPool(numThreads)
  lazy val blockGenerator = new BlockGenerator(StorageLevel.MEMORY_ONLY)
  lazy val finishCount = new AtomicInteger(0)

  protected def onStart() {
    blockGenerator.start()
    (1 to numThreads).map(threadId => {
      val runnable = new Runnable {
        def run() {
          (1 to numRecordsPerThread).foreach(i =>
            blockGenerator += (threadId * numRecordsPerThread + i) )
          if (finishCount.incrementAndGet == numThreads) {
            MultiThreadTestReceiver.haveAllThreadsFinished = true
          }
          logInfo("Finished thread " + threadId)
        }
      }
      executorPool.submit(runnable)
    })
  }

  protected def onStop() {
    executorPool.shutdown()
  }
}

object MultiThreadTestReceiver {
  var haveAllThreadsFinished = false
}

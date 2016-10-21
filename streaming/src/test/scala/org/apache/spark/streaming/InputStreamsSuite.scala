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

import java.io.{BufferedWriter, File, OutputStreamWriter}
import java.net.{ServerSocket, Socket, SocketException}
import java.nio.charset.StandardCharsets
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.{ManualClock, Utils}

class InputStreamsSuite extends TestSuiteBase with BeforeAndAfter {

  test("socket input stream") {
    withTestServer(new TestServer()) { testServer =>
      // Start the server
      testServer.start()

      // Set up the streaming context and input streams
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        ssc.addStreamingListener(ssc.progressListener)

        val input = Seq(1, 2, 3, 4, 5)
        // Use "batchCount" to make sure we check the result after all batches finish
        val batchCounter = new BatchCounter(ssc)
        val networkStream = ssc.socketTextStream(
          "localhost", testServer.port, StorageLevel.MEMORY_AND_DISK)
        val outputQueue = new ConcurrentLinkedQueue[Seq[String]]
        val outputStream = new TestOutputStream(networkStream, outputQueue)
        outputStream.register()
        ssc.start()

        // Feed data to the server to send to the network receiver
        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        val expectedOutput = input.map(_.toString)
        for (i <- input.indices) {
          testServer.send(input(i).toString + "\n")
          Thread.sleep(500)
          clock.advance(batchDuration.milliseconds)
        }
        // Make sure we finish all batches before "stop"
        if (!batchCounter.waitUntilBatchesCompleted(input.size, 30000)) {
          fail("Timeout: cannot finish all batches in 30 seconds")
        }

        // Ensure progress listener has been notified of all events
        ssc.sparkContext.listenerBus.waitUntilEmpty(500)

        // Verify all "InputInfo"s have been reported
        assert(ssc.progressListener.numTotalReceivedRecords === input.size)
        assert(ssc.progressListener.numTotalProcessedRecords === input.size)

        logInfo("Stopping server")
        testServer.stop()
        logInfo("Stopping context")
        ssc.stop()

        // Verify whether data received was as expected
        logInfo("--------------------------------")
        logInfo("output.size = " + outputQueue.size)
        logInfo("output")
        outputQueue.asScala.foreach(x => logInfo("[" + x.mkString(",") + "]"))
        logInfo("expected output.size = " + expectedOutput.size)
        logInfo("expected output")
        expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
        logInfo("--------------------------------")

        // Verify whether all the elements received are as expected
        // (whether the elements were received one in each interval is not verified)
        val output: Array[String] = outputQueue.asScala.flatMap(x => x).toArray
        assert(output.length === expectedOutput.size)
        for (i <- output.indices) {
          assert(output(i) === expectedOutput(i))
        }
      }
    }
  }

  test("socket input stream - no block in a batch") {
    withTestServer(new TestServer()) { testServer =>
      testServer.start()

      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        ssc.addStreamingListener(ssc.progressListener)

        val batchCounter = new BatchCounter(ssc)
        val networkStream = ssc.socketTextStream(
          "localhost", testServer.port, StorageLevel.MEMORY_AND_DISK)
        val outputQueue = new ConcurrentLinkedQueue[Seq[String]]
        val outputStream = new TestOutputStream(networkStream, outputQueue)
        outputStream.register()
        ssc.start()

        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        clock.advance(batchDuration.milliseconds)

        // Make sure the first batch is finished
        if (!batchCounter.waitUntilBatchesCompleted(1, 30000)) {
          fail("Timeout: cannot finish all batches in 30 seconds")
        }

        networkStream.generatedRDDs.foreach { case (_, rdd) =>
          assert(!rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]])
        }
      }
    }
  }

  test("binary records stream") {
    var testDir: File = null
    try {
      val batchDuration = Seconds(2)
      testDir = Utils.createTempDir()
      // Create a file that exists before the StreamingContext is created:
      val existingFile = new File(testDir, "0")
      Files.write("0\n", existingFile, StandardCharsets.UTF_8)
      assert(existingFile.setLastModified(10000) && existingFile.lastModified === 10000)

      // Set up the streaming context and input streams
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        // This `setTime` call ensures that the clock is past the creation time of `existingFile`
        clock.setTime(existingFile.lastModified + batchDuration.milliseconds)
        val batchCounter = new BatchCounter(ssc)
        val fileStream = ssc.binaryRecordsStream(testDir.toString, 1)
        val outputQueue = new ConcurrentLinkedQueue[Seq[Array[Byte]]]
        val outputStream = new TestOutputStream(fileStream, outputQueue)
        outputStream.register()
        ssc.start()

        // Advance the clock so that the files are created after StreamingContext starts, but
        // not enough to trigger a batch
        clock.advance(batchDuration.milliseconds / 2)

        val input = Seq(1, 2, 3, 4, 5)
        input.foreach { i =>
          Thread.sleep(batchDuration.milliseconds)
          val file = new File(testDir, i.toString)
          Files.write(Array[Byte](i.toByte), file)
          assert(file.setLastModified(clock.getTimeMillis()))
          assert(file.lastModified === clock.getTimeMillis())
          logInfo("Created file " + file)
          // Advance the clock after creating the file to avoid a race when
          // setting its modification time
          clock.advance(batchDuration.milliseconds)
          eventually(eventuallyTimeout) {
            assert(batchCounter.getNumCompletedBatches === i)
          }
        }

        val expectedOutput = input.map(i => i.toByte)
        val obtainedOutput = outputQueue.asScala.flatten.toList.map(i => i(0).toByte)
        assert(obtainedOutput.toSeq === expectedOutput)
      }
    } finally {
      if (testDir != null) Utils.deleteRecursively(testDir)
    }
  }

  test("file input stream - newFilesOnly = true") {
    testFileStream(newFilesOnly = true)
  }

  test("file input stream - newFilesOnly = false") {
    testFileStream(newFilesOnly = false)
  }

  test("file input stream - wildcard") {
    var testDir: File = null
    try {
      val batchDuration = Seconds(2)
      testDir = Utils.createTempDir()
      val testSubDir1 = Utils.createDirectory(testDir.toString, "tmp1")
      val testSubDir2 = Utils.createDirectory(testDir.toString, "tmp2")

      // Create a file that exists before the StreamingContext is created:
      val existingFile = new File(testDir, "0")
      Files.write("0\n", existingFile, StandardCharsets.UTF_8)
      assert(existingFile.setLastModified(10000) && existingFile.lastModified === 10000)

      val pathWithWildCard = testDir.toString + "/*/"

      // Set up the streaming context and input streams
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        clock.setTime(existingFile.lastModified + batchDuration.milliseconds)
        val batchCounter = new BatchCounter(ssc)
        // monitor "testDir/*/"
        val fileStream = ssc.fileStream[LongWritable, Text, TextInputFormat](
          pathWithWildCard).map(_._2.toString)
        val outputQueue = new ConcurrentLinkedQueue[Seq[String]]
        val outputStream = new TestOutputStream(fileStream, outputQueue)
        outputStream.register()
        ssc.start()

        // Advance the clock so that the files are created after StreamingContext starts, but
        // not enough to trigger a batch
        clock.advance(batchDuration.milliseconds / 2)

        def createFileAndAdvenceTime(data: Int, dir: File): Unit = {
          val file = new File(testSubDir1, data.toString)
          Files.write(data + "\n", file, StandardCharsets.UTF_8)
          assert(file.setLastModified(clock.getTimeMillis()))
          assert(file.lastModified === clock.getTimeMillis())
          logInfo("Created file " + file)
          // Advance the clock after creating the file to avoid a race when
          // setting its modification time
          clock.advance(batchDuration.milliseconds)
          eventually(eventuallyTimeout) {
            assert(batchCounter.getNumCompletedBatches === data)
          }
        }
        // Over time, create files in the temp directory 1
        val input1 = Seq(1, 2, 3, 4, 5)
        input1.foreach(i => createFileAndAdvenceTime(i, testSubDir1))

        // Over time, create files in the temp directory 1
        val input2 = Seq(6, 7, 8, 9, 10)
        input2.foreach(i => createFileAndAdvenceTime(i, testSubDir2))

        // Verify that all the files have been read
        val expectedOutput = (input1 ++ input2).map(_.toString).toSet
        assert(outputQueue.asScala.flatten.toSet === expectedOutput)
      }
    } finally {
      if (testDir != null) Utils.deleteRecursively(testDir)
    }
  }

  test("multi-thread receiver") {
    // set up the test receiver
    val numThreads = 10
    val numRecordsPerThread = 1000
    val numTotalRecords = numThreads * numRecordsPerThread
    val testReceiver = new MultiThreadTestReceiver(numThreads, numRecordsPerThread)
    MultiThreadTestReceiver.haveAllThreadsFinished = false
    val outputQueue = new ConcurrentLinkedQueue[Seq[Long]]
    def output: Iterable[Long] = outputQueue.asScala.flatMap(x => x)

    // set up the network stream using the test receiver
    withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
      val networkStream = ssc.receiverStream[Int](testReceiver)
      val countStream = networkStream.count

      val outputStream = new TestOutputStream(countStream, outputQueue)
      outputStream.register()
      ssc.start()

      // Let the data from the receiver be received
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
      val startTime = System.currentTimeMillis()
      while ((!MultiThreadTestReceiver.haveAllThreadsFinished || output.sum < numTotalRecords) &&
        System.currentTimeMillis() - startTime < 5000) {
        Thread.sleep(100)
        clock.advance(batchDuration.milliseconds)
      }
      Thread.sleep(1000)
    }

    // Verify whether data received was as expected
    logInfo("--------------------------------")
    logInfo("output.size = " + outputQueue.size)
    logInfo("output")
    outputQueue.asScala.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")
    assert(output.sum === numTotalRecords)
  }

  test("queue input stream - oneAtATime = true") {
    val input = Seq("1", "2", "3", "4", "5")
    val expectedOutput = input.map(Seq(_))
    val outputQueue = new ConcurrentLinkedQueue[Seq[String]]
    def output: Iterable[Seq[String]] = outputQueue.asScala.filter(_.nonEmpty)

    // Set up the streaming context and input streams
    withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
      val queue = new mutable.Queue[RDD[String]]()
      val queueStream = ssc.queueStream(queue, oneAtATime = true)
      val outputStream = new TestOutputStream(queueStream, outputQueue)
      outputStream.register()
      ssc.start()

      // Setup data queued into the stream
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]

      val inputIterator = input.toIterator
      for (i <- input.indices) {
        // Enqueue more than 1 item per tick but they should dequeue one at a time
        inputIterator.take(2).foreach { i =>
          queue.synchronized {
            queue += ssc.sparkContext.makeRDD(Seq(i))
          }
        }
        clock.advance(batchDuration.milliseconds)
      }
      Thread.sleep(1000)
    }

    // Verify whether data received was as expected
    logInfo("--------------------------------")
    logInfo("output.size = " + outputQueue.size)
    logInfo("output")
    outputQueue.asScala.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Verify whether all the elements received are as expected
    assert(output.size === expectedOutput.size)
    output.zipWithIndex.foreach{case (e, i) => assert(e == expectedOutput(i))}
  }

  test("queue input stream - oneAtATime = false") {
    val outputQueue = new ConcurrentLinkedQueue[Seq[String]]
    def output: Iterable[Seq[String]] = outputQueue.asScala.filter(_.nonEmpty)
    val input = Seq("1", "2", "3", "4", "5")
    val expectedOutput = Seq(Seq("1", "2", "3"), Seq("4", "5"))

    // Set up the streaming context and input streams
    withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
      val queue = new mutable.Queue[RDD[String]]()
      val queueStream = ssc.queueStream(queue, oneAtATime = false)
      val outputStream = new TestOutputStream(queueStream, outputQueue)
      outputStream.register()
      ssc.start()

      // Setup data queued into the stream
      val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]

      // Enqueue the first 3 items (one by one), they should be merged in the next batch
      val inputIterator = input.toIterator
      inputIterator.take(3).foreach { i =>
        queue.synchronized {
          queue += ssc.sparkContext.makeRDD(Seq(i))
        }
      }
      clock.advance(batchDuration.milliseconds)
      Thread.sleep(1000)

      // Enqueue the remaining items (again one by one), merged in the final batch
      inputIterator.foreach { i =>
        queue.synchronized {
          queue += ssc.sparkContext.makeRDD(Seq(i))
        }
      }
      clock.advance(batchDuration.milliseconds)
      Thread.sleep(1000)
    }

    // Verify whether data received was as expected
    logInfo("--------------------------------")
    logInfo("output.size = " + outputQueue.size)
    logInfo("output")
    outputQueue.asScala.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Verify whether all the elements received are as expected
    assert(output.size === expectedOutput.size)
    output.zipWithIndex.foreach{case (e, i) => assert(e == expectedOutput(i))}
  }

  test("test track the number of input stream") {
    withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>

      class TestInputDStream extends InputDStream[String](ssc) {
        def start() {}

        def stop() {}

        def compute(validTime: Time): Option[RDD[String]] = None
      }

      class TestReceiverInputDStream extends ReceiverInputDStream[String](ssc) {
        def getReceiver: Receiver[String] = null
      }

      // Register input streams
      val receiverInputStreams = Array(new TestReceiverInputDStream, new TestReceiverInputDStream)
      val inputStreams = Array(new TestInputDStream, new TestInputDStream, new TestInputDStream)

      assert(ssc.graph.getInputStreams().length ==
        receiverInputStreams.length + inputStreams.length)
      assert(ssc.graph.getReceiverInputStreams().length == receiverInputStreams.length)
      assert(ssc.graph.getReceiverInputStreams() === receiverInputStreams)
      assert(ssc.graph.getInputStreams().map(_.id) === Array.tabulate(5)(i => i))
      assert(receiverInputStreams.map(_.id) === Array(0, 1))
    }
  }

  def testFileStream(newFilesOnly: Boolean) {
    var testDir: File = null
    try {
      val batchDuration = Seconds(2)
      testDir = Utils.createTempDir()
      // Create a file that exists before the StreamingContext is created:
      val existingFile = new File(testDir, "0")
      Files.write("0\n", existingFile, StandardCharsets.UTF_8)
      assert(existingFile.setLastModified(10000) && existingFile.lastModified === 10000)

      // Set up the streaming context and input streams
      withStreamingContext(new StreamingContext(conf, batchDuration)) { ssc =>
        val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
        // This `setTime` call ensures that the clock is past the creation time of `existingFile`
        clock.setTime(existingFile.lastModified + batchDuration.milliseconds)
        val batchCounter = new BatchCounter(ssc)
        val fileStream = ssc.fileStream[LongWritable, Text, TextInputFormat](
          testDir.toString, (x: Path) => true, newFilesOnly = newFilesOnly).map(_._2.toString)
        val outputQueue = new ConcurrentLinkedQueue[Seq[String]]
        val outputStream = new TestOutputStream(fileStream, outputQueue)
        outputStream.register()
        ssc.start()

        // Advance the clock so that the files are created after StreamingContext starts, but
        // not enough to trigger a batch
        clock.advance(batchDuration.milliseconds / 2)

        // Over time, create files in the directory
        val input = Seq(1, 2, 3, 4, 5)
        input.foreach { i =>
          val file = new File(testDir, i.toString)
          Files.write(i + "\n", file, StandardCharsets.UTF_8)
          assert(file.setLastModified(clock.getTimeMillis()))
          assert(file.lastModified === clock.getTimeMillis())
          logInfo("Created file " + file)
          // Advance the clock after creating the file to avoid a race when
          // setting its modification time
          clock.advance(batchDuration.milliseconds)
          eventually(eventuallyTimeout) {
            assert(batchCounter.getNumCompletedBatches === i)
          }
        }

        // Verify that all the files have been read
        val expectedOutput = if (newFilesOnly) {
          input.map(_.toString).toSet
        } else {
          (Seq(0) ++ input).map(_.toString).toSet
        }
        assert(outputQueue.asScala.flatten.toSet === expectedOutput)
      }
    } finally {
      if (testDir != null) Utils.deleteRecursively(testDir)
    }
  }
}


/** This is a server to test the network input stream */
class TestServer(portToBind: Int = 0) extends Logging {

  val queue = new ArrayBlockingQueue[String](100)

  val serverSocket = new ServerSocket(portToBind)

  private val startLatch = new CountDownLatch(1)

  val servingThread = new Thread() {
    override def run() {
      try {
        while (true) {
          logInfo("Accepting connections on port " + port)
          val clientSocket = serverSocket.accept()
          if (startLatch.getCount == 1) {
            // The first connection is a test connection to implement "waitForStart", so skip it
            // and send a signal
            if (!clientSocket.isClosed) {
              clientSocket.close()
            }
            startLatch.countDown()
          } else {
            // Real connections
            logInfo("New connection")
            try {
              clientSocket.setTcpNoDelay(true)
              val outputStream = new BufferedWriter(
                new OutputStreamWriter(clientSocket.getOutputStream, StandardCharsets.UTF_8))

              while (clientSocket.isConnected) {
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
              if (!clientSocket.isClosed) {
                clientSocket.close()
              }
            }
          }
        }
      } catch {
        case ie: InterruptedException =>

      } finally {
        serverSocket.close()
      }
    }
  }

  def start(): Unit = {
    servingThread.start()
    if (!waitForStart(10000)) {
      stop()
      throw new AssertionError("Timeout: TestServer cannot start in 10 seconds")
    }
  }

  /**
   * Wait until the server starts. Return true if the server starts in "millis" milliseconds.
   * Otherwise, return false to indicate it's timeout.
   */
  private def waitForStart(millis: Long): Boolean = {
    // We will create a test connection to the server so that we can make sure it has started.
    val socket = new Socket("localhost", port)
    try {
      startLatch.await(millis, TimeUnit.MILLISECONDS)
    } finally {
      if (!socket.isClosed) {
        socket.close()
      }
    }
  }

  def send(msg: String) { queue.put(msg) }

  def stop() { servingThread.interrupt() }

  def port: Int = serverSocket.getLocalPort
}

/** This is a receiver to test multiple threads inserting data using block generator */
class MultiThreadTestReceiver(numThreads: Int, numRecordsPerThread: Int)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY_SER) with Logging {
  lazy val executorPool = Executors.newFixedThreadPool(numThreads)
  lazy val finishCount = new AtomicInteger(0)

  def onStart() {
    (1 to numThreads).map(threadId => {
      val runnable = new Runnable {
        def run() {
          (1 to numRecordsPerThread).foreach(i =>
            store(threadId * numRecordsPerThread + i) )
          if (finishCount.incrementAndGet == numThreads) {
            MultiThreadTestReceiver.haveAllThreadsFinished = true
          }
          logInfo("Finished thread " + threadId)
        }
      }
      executorPool.submit(runnable)
    })
  }

  def onStop() {
    executorPool.shutdown()
  }
}

object MultiThreadTestReceiver {
  var haveAllThreadsFinished = false
}

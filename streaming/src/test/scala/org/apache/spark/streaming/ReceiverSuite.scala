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

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.{Semaphore, TimeUnit}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.UI._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver._
import org.apache.spark.streaming.receiver.WriteAheadLogBasedBlockHandler._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/** Testsuite for testing the network receiver behavior */
class ReceiverSuite extends TestSuiteBase with TimeLimits with Serializable {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val signaler: Signaler = ThreadSignaler

  test("receiver life cycle") {

    val receiver = new FakeReceiver
    val executor = new FakeReceiverSupervisor(receiver)
    val executorStarted = new Semaphore(0)

    assert(executor.isAllEmpty)

    // Thread that runs the executor
    val executingThread = new Thread() {
      override def run(): Unit = {
        executor.start()
        executorStarted.release(1)
        executor.awaitTermination()
      }
    }

    // Start the receiver
    executingThread.start()

    // Verify that the receiver
    intercept[Exception] {
      failAfter(200.milliseconds) {
        executingThread.join()
      }
    }

    // Ensure executor is started
    executorStarted.acquire()

    // Verify that receiver was started
    assert(receiver.callsRecorder.calls === Seq("onStart"))
    assert(executor.isReceiverStarted())
    assert(receiver.isStarted())
    assert(!receiver.isStopped())
    assert(receiver.otherThread.isAlive)
    eventually(timeout(100.milliseconds), interval(10.milliseconds)) {
      assert(receiver.receiving)
    }

    // Verify whether the data stored by the receiver was sent to the executor
    val byteBuffer = ByteBuffer.allocate(100)
    val arrayBuffer = new ArrayBuffer[Int]()
    val iterator = arrayBuffer.iterator
    receiver.store(1)
    receiver.store(byteBuffer)
    receiver.store(arrayBuffer)
    receiver.store(iterator)
    assert(executor.singles.size === 1)
    assert(executor.singles.head === 1)
    assert(executor.byteBuffers.size === 1)
    assert(executor.byteBuffers.head.eq(byteBuffer))
    assert(executor.iterators.size === 1)
    assert(executor.iterators.head.eq(iterator))
    assert(executor.arrayBuffers.size === 1)
    assert(executor.arrayBuffers.head.eq(arrayBuffer))

    // Verify whether the exceptions reported by the receiver was sent to the executor
    val exception = new Exception
    receiver.reportError("Error", exception)
    assert(executor.errors.size === 1)
    assert(executor.errors.head.eq(exception))

    // Verify restarting actually stops and starts the receiver
    executor.callsRecorder.reset()
    receiver.callsRecorder.reset()
    receiver.restart("restarting", null, 100)
    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      // below verification ensures for now receiver is already restarted
      assert(receiver.isStarted())
      assert(!receiver.isStopped())
      assert(receiver.receiving)

      // both receiver supervisor and receiver should be stopped first, and started
      assert(executor.callsRecorder.calls === Seq("onReceiverStop", "onReceiverStart"))
      assert(receiver.callsRecorder.calls === Seq("onStop", "onStart"))

      // check whether the delay between stop and start is respected
      assert(executor.callsRecorder.timestamps.reverse.reduceLeft { _ - _ } >= 100)
      assert(receiver.callsRecorder.timestamps.reverse.reduceLeft { _ - _ } >= 100)
    }

    // Verify that stopping actually stops the thread
    failAfter(1.second) {
      receiver.stop("test")
      assert(receiver.isStopped())
      assert(!receiver.otherThread.isAlive)

      // The thread that started the executor should complete
      // as stop() stops everything
      executingThread.join()
    }
  }

  ignore("block generator throttling") {
    val blockGeneratorListener = new FakeBlockGeneratorListener
    val blockIntervalMs = 100
    val maxRate = 1001
    val conf = new SparkConf().set("spark.streaming.blockInterval", s"${blockIntervalMs}ms").
      set("spark.streaming.receiver.maxRate", maxRate.toString)
    val blockGenerator = new BlockGenerator(blockGeneratorListener, 1, conf)
    val expectedBlocks = 20
    val waitTime = expectedBlocks * blockIntervalMs
    val expectedMessages = maxRate * waitTime / 1000
    val expectedMessagesPerBlock = maxRate * blockIntervalMs / 1000
    val generatedData = new ArrayBuffer[Int]

    // Generate blocks
    val startTimeNs = System.nanoTime()
    blockGenerator.start()
    var count = 0
    while (System.nanoTime() - startTimeNs < TimeUnit.MILLISECONDS.toNanos(waitTime)) {
      blockGenerator.addData(count)
      generatedData += count
      count += 1
    }
    blockGenerator.stop()

    val recordedBlocks = blockGeneratorListener.arrayBuffers
    val recordedData = recordedBlocks.flatten
    assert(blockGeneratorListener.arrayBuffers.nonEmpty, "No blocks received")
    assert(recordedData.toSet === generatedData.toSet, "Received data not same")

    // recordedData size should be close to the expected rate; use an error margin proportional to
    // the value, so that rate changes don't cause a brittle test
    val minExpectedMessages = expectedMessages - 0.05 * expectedMessages
    val maxExpectedMessages = expectedMessages + 0.05 * expectedMessages
    val numMessages = recordedData.size
    assert(
      numMessages >= minExpectedMessages && numMessages <= maxExpectedMessages,
      s"#records received = $numMessages, not between $minExpectedMessages and $maxExpectedMessages"
    )

    // XXX Checking every block would require an even distribution of messages across blocks,
    // which throttling code does not control. Therefore, test against the average.
    val minExpectedMessagesPerBlock = expectedMessagesPerBlock - 0.05 * expectedMessagesPerBlock
    val maxExpectedMessagesPerBlock = expectedMessagesPerBlock + 0.05 * expectedMessagesPerBlock
    val receivedBlockSizes = recordedBlocks.map { _.size }.mkString(",")

    // the first and last block may be incomplete, so we slice them out
    val validBlocks = recordedBlocks.drop(1).dropRight(1)
    val averageBlockSize = validBlocks.map(block => block.size).sum / validBlocks.size

    assert(
      averageBlockSize >= minExpectedMessagesPerBlock &&
        averageBlockSize <= maxExpectedMessagesPerBlock,
      s"# records in received blocks = [$receivedBlockSizes], not between " +
        s"$minExpectedMessagesPerBlock and $maxExpectedMessagesPerBlock, on average"
    )
  }

  /**
   * Test whether write ahead logs are generated by received,
   * and automatically cleaned up. The clean up must be aware of the
   * remember duration of the input streams. E.g., input streams on which window()
   * has been applied must remember the data for longer, and hence corresponding
   * WALs should be cleaned later.
   */
  test("write ahead log - generating and cleaning") {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")  // must be at least 3 as we are going to start 2 receivers
      .setAppName(framework)
      .set(UI_ENABLED, true)
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      .set("spark.streaming.receiver.writeAheadLog.rollingIntervalSecs", "1")
    val batchDuration = Milliseconds(500)
    val tempDirectory = Utils.createTempDir()
    val logDirectory1 = new File(checkpointDirToLogDir(tempDirectory.getAbsolutePath, 0))
    val logDirectory2 = new File(checkpointDirToLogDir(tempDirectory.getAbsolutePath, 1))
    val allLogFiles1 = new mutable.HashSet[String]()
    val allLogFiles2 = new mutable.HashSet[String]()
    logInfo("Temp checkpoint directory = " + tempDirectory)

    def getBothCurrentLogFiles(): (Seq[String], Seq[String]) = {
      (getCurrentLogFiles(logDirectory1), getCurrentLogFiles(logDirectory2))
    }

    def getCurrentLogFiles(logDirectory: File): Seq[String] = {
      try {
        if (logDirectory.exists()) {
          logDirectory.listFiles().filter { _.getName.startsWith("log") }.map { _.toString }
            .toImmutableArraySeq
        } else {
          Seq.empty
        }
      } catch {
        case e: Exception =>
          Seq.empty
      }
    }

    def printLogFiles(message: String, files: Seq[String]): Unit = {
      logInfo(s"$message (${files.size} files):\n" + files.mkString("\n"))
    }

    withStreamingContext(new StreamingContext(sparkConf, batchDuration)) { ssc =>
      val receiver1 = new FakeReceiver(sendData = true)
      val receiver2 = new FakeReceiver(sendData = true)
      val receiverStream1 = ssc.receiverStream(receiver1)
      val receiverStream2 = ssc.receiverStream(receiver2)
      receiverStream1.register()
      receiverStream2.window(batchDuration * 6).register()  // 3 second window
      ssc.checkpoint(tempDirectory.getAbsolutePath())
      ssc.start()

      // Run until sufficient WAL files have been generated and
      // the first WAL files has been deleted
      eventually(timeout(20.seconds), interval(batchDuration.milliseconds.millis)) {
        val (logFiles1, logFiles2) = getBothCurrentLogFiles()
        allLogFiles1 ++= logFiles1
        allLogFiles2 ++= logFiles2
        if (allLogFiles1.nonEmpty) {
          assert(!logFiles1.contains(allLogFiles1.toSeq.min))
        }
        if (allLogFiles2.nonEmpty) {
          assert(!logFiles2.contains(allLogFiles2.toSeq.min))
        }
        assert(allLogFiles1.size >= 7)
        assert(allLogFiles2.size >= 7)
      }
      ssc.stop(stopSparkContext = true, stopGracefully = true)

      val sortedAllLogFiles1 = allLogFiles1.toSeq.sorted
      val sortedAllLogFiles2 = allLogFiles2.toSeq.sorted
      val (leftLogFiles1, leftLogFiles2) = getBothCurrentLogFiles()

      printLogFiles("Receiver 0: all", sortedAllLogFiles1)
      printLogFiles("Receiver 0: left", leftLogFiles1)
      printLogFiles("Receiver 1: all", sortedAllLogFiles2)
      printLogFiles("Receiver 1: left", leftLogFiles2)

      // Verify that necessary latest log files are not deleted
      //   receiverStream1 needs to retain just the last batch = 1 log file
      //   receiverStream2 needs to retain 3 seconds (3-seconds window) = 3 log files
      assert(sortedAllLogFiles1.takeRight(1).forall(leftLogFiles1.contains))
      assert(sortedAllLogFiles2.takeRight(3).forall(leftLogFiles2.contains))
    }
  }

  /**
   * An implementation of NetworkReceiverExecutor used for testing a NetworkReceiver.
   * Instead of storing the data in the BlockManager, it stores all the data in a local buffer
   * that can used for verifying that the data has been forwarded correctly.
   */
  class FakeReceiverSupervisor(receiver: FakeReceiver)
    extends ReceiverSupervisor(receiver, new SparkConf()) {
    val singles = new ArrayBuffer[Any]
    val byteBuffers = new ArrayBuffer[ByteBuffer]
    val iterators = new ArrayBuffer[Iterator[_]]
    val arrayBuffers = new ArrayBuffer[ArrayBuffer[_]]
    val errors = new ArrayBuffer[Throwable]

    // tracks calls of "onReceiverStart", "onReceiverStop"
    val callsRecorder = new MethodsCallRecorder()

    /** Check if all data structures are clean */
    def isAllEmpty: Boolean = {
      singles.isEmpty && byteBuffers.isEmpty && iterators.isEmpty &&
        arrayBuffers.isEmpty && errors.isEmpty
    }

    def pushSingle(data: Any): Unit = {
      singles += data
    }

    def pushBytes(
        bytes: ByteBuffer,
        optionalMetadata: Option[Any],
        optionalBlockId: Option[StreamBlockId]): Unit = {
      byteBuffers += bytes
    }

    def pushIterator(
        iterator: Iterator[_],
        optionalMetadata: Option[Any],
        optionalBlockId: Option[StreamBlockId]): Unit = {
      iterators += iterator
    }

    def pushArrayBuffer(
        arrayBuffer: ArrayBuffer[_],
        optionalMetadata: Option[Any],
        optionalBlockId: Option[StreamBlockId]): Unit = {
      arrayBuffers +=  arrayBuffer
    }

    def reportError(message: String, throwable: Throwable): Unit = {
      errors += throwable
    }

    override protected def onReceiverStart(): Boolean = {
      callsRecorder.record()
      true
    }

    override protected def onReceiverStop(message: String, error: Option[Throwable]): Unit = {
      callsRecorder.record()
      super.onReceiverStop(message, error)
    }

    override def createBlockGenerator(
        blockGeneratorListener: BlockGeneratorListener): BlockGenerator = {
      null
    }
  }

  /**
   * An implementation of BlockGeneratorListener that is used to test the BlockGenerator.
   */
  class FakeBlockGeneratorListener(pushDelay: Long = 0) extends BlockGeneratorListener {
    // buffer of data received as ArrayBuffers
    val arrayBuffers = new ArrayBuffer[ArrayBuffer[Int]]
    val errors = new ArrayBuffer[Throwable]

    def onAddData(data: Any, metadata: Any): Unit = { }

    def onGenerateBlock(blockId: StreamBlockId): Unit = { }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {
      val bufferOfInts = arrayBuffer.map(_.asInstanceOf[Int])
      arrayBuffers += bufferOfInts
      Thread.sleep(0)
    }

    def onError(message: String, throwable: Throwable): Unit = {
      errors += throwable
    }
  }
}

/**
 * An implementation of Receiver that is used for testing a receiver's life cycle.
 */
class FakeReceiver(sendData: Boolean = false) extends Receiver[Int](StorageLevel.MEMORY_ONLY) {
  @volatile var otherThread: Thread = null
  @volatile var receiving = false

  // tracks calls of "onStart", "onStop"
  @transient lazy val callsRecorder = new MethodsCallRecorder()

  def onStart(): Unit = {
    otherThread = new Thread() {
      override def run(): Unit = {
        receiving = true
        try {
          var count = 0
          while (!isStopped()) {
            if (sendData) {
              store(count)
              count += 1
            }
            Thread.sleep(10)
          }
        } finally {
          receiving = false
        }
      }
    }
    callsRecorder.record()
    otherThread.start()
  }

  def onStop(): Unit = {
    callsRecorder.record()
    otherThread.join()
  }
}

class MethodsCallRecorder {
  // tracks calling methods as (timestamp, methodName)
  private val records = new ArrayBuffer[(Long, String)]

  def record(): Unit = records.append((System.currentTimeMillis(), callerMethodName))

  def reset(): Unit = records.clear()

  def callsWithTimestamp: scala.collection.immutable.Seq[(Long, String)] = records.toList

  def calls: scala.collection.immutable.Seq[String] = records.map(_._2).toList

  def timestamps: scala.collection.immutable.Seq[Long] = records.map(_._1).toList

  private def callerMethodName: String = {
    val stackTrace = new Throwable().getStackTrace
    // it should return method name of two levels deeper
    stackTrace(2).getMethodName
  }
}

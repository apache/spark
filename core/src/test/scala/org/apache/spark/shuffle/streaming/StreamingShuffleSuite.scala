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

package org.apache.spark.shuffle.streaming

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, Semaphore, TimeoutException, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.reflectiveCalls
import scala.reflect.ClassTag

import io.netty.buffer.{ByteBufOutputStream, PooledByteBufAllocator}
import io.netty.util.ResourceLeakDetector
import io.netty.util.concurrent.{Future => NettyFuture}
import org.scalatest.Assertions.intercept
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{SHUFFLE_MANAGER_INCREMENTAL, STREAMING_SHUFFLE_CHECKSUM_ENABLED, STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE, STREAMING_SHUFFLE_READER_MAX_MEMORY, STREAMING_SHUFFLE_WRITER_MAX_MEMORY}
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientFactory}
import org.apache.spark.network.shuffle.streaming.{DataMessage, ShuffleChecksum, StreamingShuffleMessage, StreamingShuffleMessageType, TerminationControlMessage}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MyRDD
import org.apache.spark.serializer.JavaSerializerInstance
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.streaming.StreamingShuffleManager.QUERY_ID_PROPERTY_KEY
import org.apache.spark.util.{ErrorNotifier, NextIterator, ThreadUtils}

class StreamingShuffleSuite
  extends SparkFunSuite
  with LocalSparkContext
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar with Logging {

  private var sparkConf: SparkConf = null
  private val queryId = "dummy-id"
  private val onTerminationAckReceived = (_: Int, _: Long) => {}
  private val shuffleId = 0

  private def createTaskContext(conf: SparkConf, partitionId: Int): TaskContextImpl = {
    val properties = new Properties()
    properties.setProperty(QUERY_ID_PROPERTY_KEY, queryId)
    val taskMemoryManager = new TaskMemoryManager(new TestMemoryManager(conf), 0)
    val metricsSystem = mock[MetricsSystem]
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId,
      taskAttemptId = 0,
      attemptNumber = 0,
      numPartitions = 1,
      taskMemoryManager = taskMemoryManager,
      localProperties = properties,
      metricsSystem = metricsSystem,
      cpus = 1
    )
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    sparkConf = new SparkConf()
      // StreamingShuffleManager is pipelined, so it occupies the incremental slot (the default
      // spark.shuffle.manager must be a BlockingShuffleManager). This still initializes the
      // streaming output tracker these tests rely on.
      .set(SHUFFLE_MANAGER_INCREMENTAL, classOf[StreamingShuffleManager].getName)
      .set(STREAMING_SHUFFLE_CHECKSUM_ENABLED, true)
  }

  override def afterEach(): Unit = System.gc()

  var leakDetectorLevel: ResourceLeakDetector.Level = null
  override def beforeAll(): Unit = {
    leakDetectorLevel = ResourceLeakDetector.getLevel()
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)
  }

  override def afterAll(): Unit = {
    ResourceLeakDetector.setLevel(leakDetectorLevel)
  }

  private class MockRDD[K, V](sc: SparkContext, numPartitions: Int, deps: List[Dependency[_]])
    extends RDD[(K, V)](sc, deps) with Serializable {

    override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = null

    override def getPartitions: Array[Partition] = Array.tabulate(numPartitions) { i =>
      new Partition {
        override def index: Int = i
      }
    }

    override def toString: String = "StreamingShuffleSuite-" + id
  }

  /**
   * Test base for StreamingShuffleReader. Some tests subclass it to override createClient (e.g.
   * to simulate a connection failure); most instantiate it directly as a plain reader.
   */
  private class MockStreamingShuffleReader[K, C](
      handle: ShuffleHandle,
      context: TaskContext,
      clientHandler: Option[StreamingShuffleClientHandler] = None,
      errorNotifier: ErrorNotifier = new ErrorNotifier())
    extends StreamingShuffleReader[K, C](handle, context, clientHandler, errorNotifier)

  private class ShuffleGroup[T: ClassTag](sc: SparkContext, mappers: Int, reducers: Int) {
    assert(SparkEnv.get.pipelinedShuffleManager.exists(_.isInstanceOf[StreamingShuffleManager]))
    assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
    SparkEnv.get.streamingShuffleOutputTracker.get
      .asInstanceOf[StreamingShuffleOutputTrackerMaster]
      .registerShuffle(shuffleId, mappers, reducers, 0)
    val rdd = new MockRDD[T, T](sc, mappers, Nil)
    val dep = new ShuffleDependency[T, T, T](rdd, new HashPartitioner(reducers))
    val handle = new StreamingShuffleHandle(shuffleId, dep)
    val writers: Array[StreamingShuffleWriter[T, T]] = Array.tabulate(mappers) { i =>
      val taskContext = createTaskContext(sc.conf, i)
      new StreamingShuffleWriter[T, T](handle, i, taskContext) {
        override def write(records: Iterator[Product2[T, T]]): Unit = {
          try {
            super.write(records)
          } catch {
            case e: Throwable =>
              try {
                taskContext.markTaskFailed(e)
              } catch {
                case t: Throwable =>
                  e.addSuppressed(t)
              }
              try {
                taskContext.markTaskCompleted(Some(e))
              } catch {
                case t: Throwable =>
                  e.addSuppressed(t)
              }
              throw e
          } finally {
            taskContext.markTaskCompleted(None)
          }
        }
      }
    }
    val readers: Array[StreamingShuffleReader[T, T]] = Array.tabulate(reducers) { i =>
      val taskContext = createTaskContext(sc.conf, i)
      new StreamingShuffleReader[T, T](handle, taskContext) {
        override def read(): Iterator[Product2[T, T]] = {

          val it = super.read()
          new NextIterator[Product2[T, T]] {

            override protected def getNext() = {
              try {
                if (it.hasNext) {
                  it.next()
                } else {
                  finished = true
                  null.asInstanceOf[Product2[T, T]]
                }
              } catch {
                case e: Throwable =>
                  try {
                    taskContext.markTaskFailed(e)
                  } catch {
                    case t: Throwable =>
                      e.addSuppressed(t)
                  }
                  try {
                    taskContext.markTaskCompleted(Some(e))
                  } catch {
                    case t: Throwable =>
                      e.addSuppressed(t)
                  }
                  throw e
              }
            }

            override protected def close() = {
              taskContext.markTaskCompleted(None)
            }
          }
        }
      }
    }

    def write(writerId: Int, data: Iterator[(T, T)]): Future[Any] = {
      val writer = writers(writerId)
      Future {
        TaskContext.withTaskContext(writer.context) {
          writer.write(data)
        }
      }(ExecutionContext.global)
    }

    // Returns a function to get the current result and a future for the final result.
    def read(readerId: Int): (() => Set[(T, T)], Future[Set[(T, T)]]) = {
      val s = new scala.collection.concurrent.TrieMap[(T, T), Unit]()
      (() => s.keySet.toSet, Future {
        TaskContext.withTaskContext(readers(readerId).context) {
          readers(readerId).read().map { r =>
            s.put((r._1, r._2), ())
            (r._1, r._2)
          }.toSet
        }
      }(ExecutionContext.global))
    }

    def sendDataMessage(writerId: Int, readerId: Int, datum: T): Unit = {
      val dataMsgFunc = StreamingShuffleSuite.newDataMsgFunc(dep, readerId)
      writers(writerId).shards(readerId).send(dataMsgFunc(writerId, datum))
    }
  }

  /**
   * This test creates one shuffle writer and 2 shuffle readers. The shuffle writer writes
   * a few records, sends termination messages, and then waits for termination acks from the
   * readers.
   *
   * We verify that all termination acks are received by the one shuffle writer, and that its
   * channel is closed after all acks are received.
   */
  test("shuffle writer servers shut down after all readers ack termination") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 2)
      g.writers(0).write(Iterator((1, 1), (2, 2)))
      eventually(Timeout(10.seconds)) {
        g.writers(0).allAcksReceived.getCount should be(0)
        g.writers(0).server.channelFuture() should be(null)
      }
    }
  }

  test("test task connection") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 2, 1)
      eventually(Timeout(1.minute)) {
        g.readers(0).clientMap.size should be(2)
        g.readers(0).taskDiscoveryExecutor.getActiveCount should be(0)
        g.writers(0).shards(0).hasClient
        g.writers(1).shards(0).hasClient
      }
    }
  }

  def await[T](f: Future[T]): T = ThreadUtils.awaitResult(f, 30.seconds)

  test("read and write") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 2, 2)

      g.writers(0).write(Iterator((1, 1), (2, 2)))
      logInfo("Writer 1 done writing")

      g.writers(1).write(Iterator((3, 3), (4, 4)))
      logInfo("Writer 2 done writing")

      await(g.read(0)._2) should be(Set((2, 2), (4, 4)))
      await(g.read(1)._2) should be(Set((1, 1), (3, 3)))
    }
  }

  test("reader handles concurrent client creation and reader termination") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 2, 1)

      g.writers(0).write(Iterator((1, 1), (2, 2)))

      val (partialRows, finalRows) = g.read(0)
      eventually(Timeout(30.seconds)) {
        partialRows().size should be (2)
      }

      g.writers(1).write(Iterator((3, 3)))
      await(finalRows) should be(Set((1, 1), (2, 2), (3, 3)))
    }
  }

  test("read and write with pauses using java serializer") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 2, 2)
      val semaphore1 = new Semaphore(0)
      val it1 = new Iterator[(Int, Int)] {
        val _it = Iterator((1, 1), (3, 3))

        override def hasNext: Boolean = {
          _it.hasNext
        }

      override def next(): (Int, Int) = {
        semaphore1.acquire(1)
        _it.next()
        }
      }

      new Thread(new Runnable {
        override def run(): Unit = {
          g.writers(0).write(it1)
          logInfo("Writer 1 done writing")
        }
      }).start()


      val semaphore2 = new Semaphore(0)
      val it2 = new Iterator[(Int, Int)] {
        val _it = Iterator((2, 2), (4, 4))

        override def hasNext: Boolean = {
          _it.hasNext
        }

        override def next(): (Int, Int) = {
          semaphore2.acquire(1)
          _it.next()
        }
      }

      new Thread(new Runnable {
        override def run(): Unit = {
          g.writers(1).write(it2)
          logInfo("Writer 2 done writing")
        }
      }).start()

      // wait until first messages are sent
      semaphore1.release(1)
      semaphore2.release(1)

      val (partialRows0, finalRows0) = g.read(0)
      val (partialRows1, finalRows1) = g.read(1)
      eventually (Timeout(30.seconds)) {
        partialRows0() should be(Set((2, 2)))
        partialRows1() should be(Set((1, 1)))
      }

      semaphore1.release(1)
      semaphore2.release(1)

      await(finalRows0) should be(Set((2, 2), (4, 4)))
      await(finalRows1) should be(Set((1, 1), (3, 3)))
    }
  }

  private trait WaitForTaskExecuted {
    private var taskExecuted = false
    def informTaskExecuted(): Unit = synchronized {
      taskExecuted = true
      this.notify()
    }

    def waitForTaskExecuted(): Unit = synchronized {
      while (!taskExecuted) {
        this.wait(100)
      }
    }
  }

  private val testError = SparkException.internalError("").asInstanceOf[Throwable]
  private def checkTestError(error: SparkException): Unit = {
    checkError(error, condition = "INTERNAL_ERROR", parameters = Map("message" -> ""))
  }

  test("Exceptions bubbled up in reader and writer threads.") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      g.readers(0).taskDiscoveryExecutor.execute { () =>
        g.readers(0).context.markTaskFailed(testError)
      }

      checkTestError(
        intercept[SparkException] {
          g.readers(0).read().hasNext
        })

      val it = Iterator((1, 1), (2, 2))

      g.writers(0).context.markTaskFailed(testError)

      checkTestError(
        intercept[SparkException] {
          g.writers(0).write(it)
        })
    }
  }

  test("Exceptions bubbled up in Netty threads.") {
    class TestStreamingShuffleServerHandler(
        onTerminationAckReceived: (Int, Long) => Unit,
        shuffleId: Int = 0,
        numReaders: Int,
        taskContext: TaskContext = null,
        errorNotifier: ErrorNotifier = new ErrorNotifier())
      extends StreamingShuffleServerHandler(
        onTerminationAckReceived,
        shuffleId,
        numReaders,
        taskContext,
        errorNotifier)
      with WaitForTaskExecuted

    class TestStreamingShuffleClientHandler(
        shuffleWriterId: Int,
        shuffleReaderId: Int,
        queue: LinkedBlockingQueue[StreamingShuffleMessage],
        shuffleId: Int = 0,
        taskContext: TaskContext = null,
        errorNotifier: ErrorNotifier = new ErrorNotifier())
      extends StreamingShuffleClientHandler(
        shuffleWriterId,
        shuffleReaderId,
        queue,
        shuffleId,
        byteLimit = Long.MaxValue,
        context = taskContext,
        errorNotifier = errorNotifier)
      with WaitForTaskExecuted

    val context = createTaskContext(sparkConf, 0)

    def OneReaderOneWriterWithCustomHandler(
        serverHandlerFactory: Option[() => StreamingShuffleServerHandler],
        clientHandlerFactory: Option[() => StreamingShuffleClientHandler]): Unit = {
      withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
        SparkEnv.get.streamingShuffleOutputTracker.get
          .asInstanceOf[StreamingShuffleOutputTrackerMaster]
          .registerShuffle(shuffleId, 1, 1, 0)


        val shuffleMapRdd = new MyRDD(sc, 1, Nil)
        val shuffleDep = new ShuffleDependency[Int, Int, Int](shuffleMapRdd, new HashPartitioner(1))

        val serverHandler = serverHandlerFactory.map(_())
        val clientHandler = clientHandlerFactory.map(_())

        val writer = new StreamingShuffleWriter[Int, Int](
          new StreamingShuffleHandle(shuffleId, shuffleDep),
          0,
          context,
          serverHandler = serverHandler)

        val reader = new MockStreamingShuffleReader[Int, Int](
          new StreamingShuffleHandle(shuffleId, shuffleDep),
          context,
          clientHandler = clientHandler)

        checkTestError(
          intercept[SparkException] {
            val it = Iterator((1, 1), (2, 2))
            reader.read()
            writer.write(it)

            if (serverHandler != None) {
              serverHandler.get.asInstanceOf[
                TestStreamingShuffleServerHandler].waitForTaskExecuted()
            } else {
              clientHandler.get.asInstanceOf[
                TestStreamingShuffleClientHandler].waitForTaskExecuted()
            }

            context.getTaskFailure.foreach(throw _)
          })
      }
    }

    OneReaderOneWriterWithCustomHandler(
      Some(() => new TestStreamingShuffleServerHandler(onTerminationAckReceived, 0, 1, context) {
        override def receive(
            client: TransportClient,
            message: ByteBuffer,
            callback: RpcResponseCallback): Unit = {
          this.context.markTaskFailed(testError)
          logError(log"Streaming shuffle server handler receive failed", testError)
          informTaskExecuted()
        }
      }),
      None)

    OneReaderOneWriterWithCustomHandler(
      None,
      Some(() => new TestStreamingShuffleClientHandler(0, 0, null, shuffleId, context) {
        override def receive(
            client: TransportClient,
            message: ByteBuffer,
            callback: RpcResponseCallback): Unit = {
          this.context.markTaskFailed(testError)
          logError(log"Streaming shuffle client handler receive failed", testError)
          informTaskExecuted()
        }
      }))
  }

  test("client handler records an error when the connection closes before termination") {
    // A writer that dies mid-stream closes its connection without sending a
    // TerminationControlMessage. Netty then fires channelInactive on the reader's client handler.
    // Because no termination was received, the handler must record an error so the reader's read
    // loop fails via checkTaskFailure() instead of polling the message queue forever. (The reader
    // surfacing an ErrorNotifier error as a task failure is covered by the send-failure tests.)
    val errorNotifier = new ErrorNotifier()
    val handler = new StreamingShuffleClientHandler(
      0, 0, new LinkedBlockingQueue[StreamingShuffleMessage](), shuffleId, Long.MaxValue,
      context = null, errorNotifier = errorNotifier)

    // channelInactive ignores its client argument, so a null is fine here.
    handler.channelInactive(null)

    val error = errorNotifier.getError()
    error.isDefined should be(true)
    error.get.getMessage should include("closed before termination")
  }

  test("client handler records no error when the connection closes after termination") {
    // The mirror of the premature-disconnect test: once a TerminationControlMessage has been
    // received, the subsequent channelInactive (a clean end-of-stream close) must NOT be treated
    // as a failure. Driven deterministically at the handler level -- feeding receive() a real
    // termination message sets terminationReceived through the production code path -- so there is
    // no reliance on Netty event-loop timing.
    val errorNotifier = new ErrorNotifier()
    val handler = new StreamingShuffleClientHandler(
      0, 0, new LinkedBlockingQueue[StreamingShuffleMessage](), shuffleId, Long.MaxValue,
      context = null, errorNotifier = errorNotifier) {
      // The reader would normally send an ack over the network here; suppress it since this test
      // has no client/channel. terminationReceived is set in receive() before this is called.
      override def sendTerminationAckMessage(client: TransportClient, shuffleWriterId: Int): Unit =
        ()
    }

    // Encode a TerminationControlMessage on the wire and hand it to receive(), exactly as a real
    // writer would. The header is: message-type id (int), sequence number (long), writer id (int),
    // reader id (int); the sequence number must be 0 since the handler expects a gapless sequence
    // starting at 0.
    val encoded = ByteBuffer.allocate(20)
    encoded.putInt(StreamingShuffleMessageType.TERMINATION_CONTROL_MESSAGE.id())
    encoded.putLong(0L)
    encoded.putInt(0) // shuffleWriterId
    encoded.putInt(0) // shuffleReaderId
    encoded.flip()
    handler.receive(null, encoded, null)

    // A clean close after termination: the handler must record no error.
    handler.channelInactive(null)
    errorNotifier.getError() should be(None)
  }

  test("reader catches out of order message sequence number from writer - duplicate") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)
      g.sendDataMessage(0, 0, 0)
      // Corrupt the sequence ID.
      g.writers(0).shards(0).lastSentSequenceNum.decrementAndGet()
      g.sendDataMessage(0, 0, 0)

      var error: Option[SparkRuntimeException] = None
      eventually(Timeout(10.seconds)) {
        // client handler of reader begins prefetching the messages from writer
        error = Some(intercept[SparkRuntimeException] {
          g.readers(0).read().next()
        })
        assert(error.isDefined)
      }

      checkError(
        error.get,
        condition = "STREAMING_SHUFFLE_INCORRECT_SEQUENCE_NUMBER",
        parameters = Map(
          "messageType" -> "DATA_MESSAGE_UNSAFE_ROW",
          "writerId" -> "0",
          "readerId" -> "0",
          "expSeqNum" -> "1",
          "actSeqNum" -> "0")
      )
    }
  }

  test("reader catches out of order message sequence number from writer - missing msg in between") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)
      val readerId = 0
      val writerId = 0

      g.sendDataMessage(writerId, readerId, 0)
      // Skip a sequence ID.
      g.writers(writerId).shards(readerId).lastSentSequenceNum.incrementAndGet()
      g.sendDataMessage(writerId, readerId, 1)

      var error: Option[SparkRuntimeException] = None
      eventually(Timeout(10.seconds)) {
        // client handler of reader begins prefetching the messages from writer
        error = Some(intercept[SparkRuntimeException] {
          g.readers(readerId).read().next()
        })
        assert(error.isDefined)
      }

      checkError(
        error.get,
        condition = "STREAMING_SHUFFLE_INCORRECT_SEQUENCE_NUMBER",
        parameters = Map(
          "messageType" -> "DATA_MESSAGE_UNSAFE_ROW",
          "writerId" -> writerId.toString,
          "readerId" -> readerId.toString,
          "expSeqNum" -> "1",
          "actSeqNum" -> "2")
      )
    }
  }

  test("reader catches out of order message sequence number from writer - missing msg at the end") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)
      val readerId = 0
      val writerId = 0
      g.sendDataMessage(writerId, readerId, 0)
      // Skip a sequence ID.
      g.writers(writerId).shards(readerId).lastSentSequenceNum.incrementAndGet()
      g.writers(writerId).shards(readerId).send(new TerminationControlMessage(writerId, readerId))

      var error: Option[SparkRuntimeException] = None
      eventually(Timeout(10.seconds)) {
        // client handler of reader begins prefetching the messages from writer
        error = Some(intercept[SparkRuntimeException] {
          g.readers(readerId).read().next()
        })
        assert(error.isDefined)
      }

      checkError(
        error.get,
        condition = "STREAMING_SHUFFLE_INCORRECT_SEQUENCE_NUMBER",
        parameters = Map(
          "messageType" -> "TERMINATION_CONTROL_MESSAGE",
          "writerId" -> writerId.toString,
          "readerId" -> readerId.toString,
          "expSeqNum" -> "1",
          "actSeqNum" -> "2")
      )
    }
  }

  test("writer catches wrong sequence number when reader send termination ack") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)
      checkError(
        intercept[SparkRuntimeException] { g.writers(0).onTerminationAckReceived(0, 2L) },
        condition = "STREAMING_SHUFFLE_INCORRECT_SEQUENCE_NUMBER",
        parameters = Map(
          "messageType" -> "TERMINATION_ACK_MESSAGE",
          "writerId" -> "0",
          "readerId" -> "0",
          "expSeqNum" -> "-1",
          "actSeqNum" -> "2"))
    }
  }

  test("termination ack with a wrong sequence number is rejected before counting " +
    "toward completion") {
    // The sequence-number check must run before the ack is counted. A mismatched ack must
    // never decrement the completion latch -- otherwise a bad ack from the last outstanding
    // reader could let write() finish "successfully" while the error is swallowed.
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)
      val writer = g.writers(0)
      // One reader => this ack is the last outstanding one; counting it would complete the writer.
      writer.allAcksReceived.getCount should be(1L)
      checkError(
        intercept[SparkRuntimeException] { writer.onTerminationAckReceived(0, 2L) },
        condition = "STREAMING_SHUFFLE_INCORRECT_SEQUENCE_NUMBER",
        parameters = Map(
          "messageType" -> "TERMINATION_ACK_MESSAGE",
          "writerId" -> "0",
          "readerId" -> "0",
          "expSeqNum" -> "-1",
          "actSeqNum" -> "2"))
      // The mismatched ack must NOT have been counted toward completion.
      writer.allAcksReceived.getCount should be(1L)
    }
  }

  test("writer write blocks when message queue is full") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      // one writer
      val writerId = 0
      // one reader
      val readerId = 0
      val shuffleMapRdd = new MyRDD(sc, 1, Nil)
      val shuffleDep = new ShuffleDependency[Int, Int, Int](shuffleMapRdd, new HashPartitioner(1))
      val shuffleHandle = new StreamingShuffleHandle(shuffleId, shuffleDep)
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId, 1, 1, 0)

      val context = createTaskContext(sc.conf, writerId)

      val newDataMsg = StreamingShuffleSuite.newDataMsgFunc(shuffleDep, readerId)
      // Configure the ShuffleWriter to buffer at most two messages, and then fill the queue.
      val writer = new StreamingShuffleWriter[Int, Int](shuffleHandle, writerId, context)
      for (i <- 0 until 2) {
        writer.shards(readerId).send(newDataMsg(writerId, writerId))
      }

      val future = StreamingShuffleSuite.verifyBlockingCall { () =>
        val it = Iterator((1, 1), (2, 2))
        // writer tried to write another DataMessage containing two records
        // It should be blocked
        writer.write(it)
      }

      // reader start reading, this should unblock the write
      val reader = new MockStreamingShuffleReader[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep),
        context
      )
      eventually(Timeout(10.seconds)) {
        future.isCompleted shouldBe true
      }

      // four records in total read
      reader.read().toList.size shouldBe 4
    }
  }

  test("writer write blocks if reader doesn't catch up") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf
        .set(STREAMING_SHUFFLE_WRITER_MAX_MEMORY, 128 << 10)
        .set(STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE, 64 << 10)
        .set(STREAMING_SHUFFLE_READER_MAX_MEMORY, 1))) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val it = new Iterator[(Int, Int)] {
        var hasBlocked = false
        def hasNext: Boolean = !hasBlocked

        var lastCalledTime = System.nanoTime()
        def next(): (Int, Int) = synchronized {
          lastCalledTime = System.nanoTime()
          (1, 1)
        }

        def isBlocking: Boolean = synchronized {
          val blocked = (System.nanoTime() - lastCalledTime).nanoseconds > 1.second
          hasBlocked = hasBlocked || blocked
          hasBlocked
        }
      }

      val writeFinished = g.write(0, it)

      eventually(Timeout(10.seconds)) {
        writeFinished.isCompleted shouldBe false
        it.isBlocking shouldBe true
      }

      // We are allowed to buffer 128KB on the writer including TCP buffers, 32KB in the reader TCP
      // buffer, and one block in the reader queue, for at least 3 messages of 64KB each to be sent.
      g.writers(0).shards(0).lastSentSequenceNum.get() should be >= 3L
      g.readers(0).clientMap.get(0L).getChannel.config().isAutoRead should be (false)
      // Since we configured the reader to have 1 byte of memory, reported memory use may be
      // capped at 1.
      g.readers(0).context.taskMemoryManager().getMemoryConsumptionForThisTask should be > 0L

      // reader reads, this will unblock the writer's write
      g.read(0)

      eventually(Timeout(5.seconds)) {
        writeFinished.isCompleted shouldBe true
      }
    }
  }

  test("Proper error handling in task discovery thread") {
    class TestStreamingShuffleReader[K, C](
        handle: ShuffleHandle,
        context: TaskContext,
        clientHandler: Option[StreamingShuffleClientHandler] = None)
        extends MockStreamingShuffleReader[K, C](handle, context, clientHandler) {

      override def createClient(
          mapId: Int,
          remoteHost: String,
          remotePort: Int): TransportClient = {
        throw testError
      }
    }

    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      assert(SparkEnv.get.pipelinedShuffleManager.exists(_.isInstanceOf[StreamingShuffleManager]))
      assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId, 1, 1, 0)
      val shuffleMapRdd = new MyRDD(sc, 1, Nil)
      val shuffleDep = new ShuffleDependency[Int, Int, Int](shuffleMapRdd, new HashPartitioner(1))

      val writerContext = createTaskContext(sc.conf, 0)

      new StreamingShuffleWriter[Int, Int](
          new StreamingShuffleHandle(shuffleId, shuffleDep),
          0,
          writerContext
        )

      val streamingShuffleHandle = new StreamingShuffleHandle(shuffleId, shuffleDep)
      val readerContext = createTaskContext(sc.conf, 0)

      val reader = new TestStreamingShuffleReader[Int, Int](streamingShuffleHandle,
        readerContext)

      checkTestError(intercept[SparkException] {
        reader.read().next()
      })
    }
  }

  test("write should only exit after receiving all termination acks") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId, 1, 2, 0)
      val shuffleMapRdd = new MyRDD(sc, 1, Nil)
      val shuffleDep = new ShuffleDependency[Int, Int, Int](shuffleMapRdd, new HashPartitioner(2))

      val writer =
        new StreamingShuffleWriter[Int, Int](
          new StreamingShuffleHandle(shuffleId, shuffleDep),
          0,
          createTaskContext(sc.conf, 0)
        )

      val future = StreamingShuffleSuite.verifyBlockingCall { () =>
        // writer should enqueue a DataMessage and a termination message in the message queue
        // after which wait on the termination ack from the reader
        writer.write(Iterator((1, 1), (2, 2)))
      }

      val reader1 = new MockStreamingShuffleReader[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep),
        createTaskContext(sc.conf, 0)
      )
      // writer should receive 1 termination ack but still blocked
      assert(reader1.read().size == 1)
      intercept[TimeoutException] {
        ThreadUtils.awaitResult(future, 2.seconds)
      }

      val reader2 = new MockStreamingShuffleReader[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep),
        createTaskContext(sc.conf, 1)
      )
      // writer should receive all termination acks and exit write
      assert(reader2.read().size == 1)
      eventually(Timeout(10.seconds)) {
        future.isCompleted shouldBe true
      }
    }
  }

  test("write should wait after finishing write to message queue and report any error " +
    "of server handler") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId, 1, 1, 0)
      val shuffleMapRdd = new MyRDD(sc, 1, Nil)
      val shuffleDep = new ShuffleDependency[Int, Int, Int](shuffleMapRdd, new HashPartitioner(1))

      // simulate asynchronous message sending of server handler fails after write finishes
      val context = createTaskContext(sc.conf, 0)

      val writer = new StreamingShuffleWriter[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep), 0, context,
        Some(new StreamingShuffleServerHandler(null, 0, 1, context, new ErrorNotifier()) {
          override def receive(c: TransportClient, b: ByteBuffer, r: RpcResponseCallback) = {
            this.context.markTaskFailed(testError)
            logError("task discovery failed", testError)
          }
        }))

      val future = StreamingShuffleSuite.verifyBlockingCall { () =>
        // writer should enqueue a DataMessage and a termination message in the message queue
        // after which wait on the termination ack from the reader
        writer.write(Iterator((1, 1), (2, 2)))
      }

      eventually(Timeout(30.seconds)) {
        writer.shards(0).lastSentSequenceNum.get() should be(1)  // 1 data message + 1 term msg
      }

      // reader start reading, this should invoke server handler's receive method
      new MockStreamingShuffleReader[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep),
        context
      )

      checkTestError(intercept[SparkException] {
        ThreadUtils.awaitResult(future, 10.seconds)
      }.getCause.asInstanceOf[SparkException])
    }
  }

  test("credit control message send failure") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      assert(SparkEnv.get.pipelinedShuffleManager.exists(_.isInstanceOf[StreamingShuffleManager]))
      assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId, 1, 1, 0)
      val context = createTaskContext(sc.conf, 0)

      val shuffleMapRdd = new MyRDD(sc, 1, Nil)
      val shuffleDep = new ShuffleDependency[Int, Int, Int](shuffleMapRdd, new HashPartitioner(1))

      new StreamingShuffleWriter(
        new StreamingShuffleHandle(shuffleId, shuffleDep),
        0,
        context
      )

      val semaphore = new Semaphore(0)
      val sharedErrorNotifier = new ErrorNotifier()
      val clientHandler = new StreamingShuffleClientHandler(
        0, 0, new LinkedBlockingQueue[StreamingShuffleMessage](), shuffleId, 1, context,
        sharedErrorNotifier) {
        override def handleResponse(future: NettyFuture[Void],
                                    errorMsg: String): Unit = {
          if (errorMsg.contains("credit control")) {
            semaphore.release()
            throw testError
          } else {
            super.handleResponse(future, errorMsg)
          }
        }
      }

      val reader = new MockStreamingShuffleReader[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep),
        context,
        clientHandler = Some(clientHandler),
        errorNotifier = sharedErrorNotifier
      )

      eventually(Timeout(1.minute)) {
        reader.clientMap.size should be(1)
        reader.taskDiscoveryExecutor.getActiveCount should be(0)
      }

      // make sure we read a response back before checking for error
      semaphore.acquire()

      checkTestError(
        intercept[SparkException] {
          reader.read().hasNext
        })
    }
  }

  test("term ack message message send failure") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      assert(SparkEnv.get.pipelinedShuffleManager.exists(_.isInstanceOf[StreamingShuffleManager]))
      assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId, 1, 1, 0)
      val writerContext = createTaskContext(sc.conf, 0)
      val readerContext = createTaskContext(sc.conf, 0)

      val shuffleMapRdd = new MyRDD(sc, 1, Nil)
      val shuffleDep = new ShuffleDependency[Int, Int, Int](shuffleMapRdd, new HashPartitioner(1))

      val writer = new StreamingShuffleWriter[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep),
        0,
        writerContext
      )

      val readerSawTermAckFailure = new CountDownLatch(1)
      val sharedErrorNotifier = new ErrorNotifier()
      val clientHandler = new StreamingShuffleClientHandler(
        0,
        0,
        new LinkedBlockingQueue[StreamingShuffleMessage](),
        shuffleId = shuffleId,
        byteLimit = Long.MaxValue,
        readerContext,
        sharedErrorNotifier) {
        override def handleResponse(future: NettyFuture[Void], errorMsg: String): Unit = {
          if (errorMsg.contains("termination acknowledgment")) {
            readerSawTermAckFailure.countDown()
            throw testError
          } else {
            super.handleResponse(future, errorMsg)
          }
        }
      }

      val reader = new MockStreamingShuffleReader[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep),
        readerContext,
        clientHandler = Some(clientHandler),
        errorNotifier = sharedErrorNotifier
      )

      eventually(Timeout(1.minute)) {
        reader.clientMap.size should be(1)
        reader.taskDiscoveryExecutor.getActiveCount should be(0)
      }

      writer.write(Iterator((1, 1)))

      // make sure we read a response back before checking for error
      readerSawTermAckFailure.await(30, TimeUnit.SECONDS) should be(true)

      checkTestError(intercept[SparkException] {
        reader.read().hasNext
      })
    }
  }

  test("large rows trigger warning with 32KB strings") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite",
      sparkConf
        .set(STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE, 16 << 10)
        .set(STREAMING_SHUFFLE_WRITER_MAX_MEMORY, 256 << 10))) { sc =>
      // ShuffleGroup below already registers `shuffleId` with the tracker; registerShuffle
      // throws on a duplicate shuffleId (SPARK-56962), so do not pre-register here.
      val logAppender = new LogAppender("large row warning")
      withLogAppender(logAppender) {
        val g = new ShuffleGroup[String](sc, 1, 1)

        val largeStr = "x" * (32 << 10)  // > block size, <25% of writer mem
        val hugeStr = "y" * (128 << 10)  // >25% of writer mem

        val writeFinished = g.write(0, Iterator((largeStr, largeStr), (hugeStr, hugeStr)))

        // Verify the data was written and can be read back correctly
        val result = g.read(0)._2
        await(result) should be(Set((largeStr, largeStr), (hugeStr, hugeStr)))
        await(writeFinished)

        val messages = logAppender.loggingEvents.map(_.getMessage.toString).toList

        messages.exists(_.contains("larger than the block size")) should be(true)
        messages.exists(_.contains(">25% of total writer memory")) should be(true)
      }
    }
  }

  test("checksum validation failure: fail on mismatch") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val corruptedDataMsgFunc = StreamingShuffleSuite.newDataMsgFunc(g.dep, 0)
      val msg = corruptedDataMsgFunc(0, 42)
      msg.data.setByte(0, msg.data.getByte(0) + 1) // Corrupt the message.
      g.writers(0).shards(0).send(msg)

      // Reader should throw an exception due to checksum mismatch
      val error = Some(intercept[SparkException] {
        await(g.read(0)._2)
      })
      error.isDefined should be(true)
      error.get.getCause.isInstanceOf[SparkRuntimeException] should be(true)
      error.get.getCause.asInstanceOf[SparkRuntimeException]
        .getCondition should be("STREAMING_SHUFFLE_CHECKSUM_VERIFICATION_FAILED")
    }
  }

  // Verify that resources are cleaned up via task completion listener
  // when tasks fail or are interrupted before the iterator is fully consumed.
  test("reader resources are cleaned up on task completion even without full iteration") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val reader = g.readers(0)
      // Wait for task discovery to connect
      eventually(Timeout(5.seconds)) {
        reader.clientMap.size should be(1)
      }

      // Mark the reader's task as failed, then trigger task completion.
      // Before the fix, this would leave threads and resources leaked because
      // cleanup only happened in the iterator's close() method which is never
      // called if the task fails before consuming the iterator.
      reader.context.markTaskFailed(testError)
      reader.context.markTaskCompleted(Some(testError))
      // Verify that reader thread pools are shut down via the task completion listener
      eventually(Timeout(30.seconds)) {
        reader.taskDiscoveryExecutor.isShutdown should be(true)
        reader.clientCreationExecutor.isShutdown should be(true)
        reader.messageQueue.isEmpty should be(true)
      }
    }
  }

  test("reader read is called before task failure, reader resources are cleaned up") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val reader = g.readers(0)
      // Wait for task discovery to connect
      eventually(Timeout(5.seconds)) {
        reader.clientMap.size should be(1)
      }

      val it = reader.read()
      reader.context.markTaskFailed(testError)
      val e = intercept[SparkException] {
        while (it.hasNext) it.next()
      }
      checkTestError(e)

      // Verify that reader thread pools are shut down via the task completion listener
      eventually(Timeout(30.seconds)) {
        reader.taskDiscoveryExecutor.isShutdown should be(true)
        reader.clientCreationExecutor.isShutdown should be(true)
        reader.messageQueue.isEmpty should be(true)
      }
    }
  }

  test("reader iteration is called during task failure, reader resources are cleaned up") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val reader = g.readers(0)

      g.sendDataMessage(0, 0, 1)

      val it = reader.read()
      it.next() should be((1, 1))

      g.sendDataMessage(0, 0, 2)

      reader.context.markTaskFailed(testError)
      val e = intercept[SparkException] {
        while (it.hasNext) it.next()
      }
      checkTestError(e)

      // Verify that reader thread pools are shut down via the task completion listener
      eventually(Timeout(10.seconds)) {
        reader.taskDiscoveryExecutor.isShutdown should be(true)
        reader.clientCreationExecutor.isShutdown should be(true)
        reader.messageQueue.isEmpty should be(true)
      }
    }
  }

  test(
    "reader read is called and task is marked failed" +
    " concurrently from another thread, reader resources are cleaned up"
  ) {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val reader = g.readers(0)
      // Wait for task discovery to connect
      eventually(Timeout(5.seconds)) {
        reader.clientMap.size should be(1)
      }

      val it = reader.read()

      new Thread("mark task completed thread") {
        setDaemon(true)
        override def run(): Unit = {
          reader.context.markTaskFailed(testError)
        }
      }.start()

      try {
        // may or may not throw exception depending on the timing of when task failure is marked
        while (it.hasNext) it.next()
      } catch {
        case e: SparkException =>
          checkTestError(e)
      }

      // Verify that reader thread pools are shut down via the task completion listener
      eventually(Timeout(10.seconds)) {
        reader.taskDiscoveryExecutor.isShutdown should be(true)
        reader.clientCreationExecutor.isShutdown should be(true)
        reader.messageQueue.isEmpty should be(true)
      }
    }
  }

  test("writer resources are cleaned up on task completion even without full iteration") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val writer = g.writers(0)
      // Server should be running initially thus channelFuture should not be null.
      // When cleanup happens after task completion, channelFuture should be set to null.
      writer.server.channelFuture() should not be null

      writer.context.markTaskFailed(testError)
      writer.context.markTaskCompleted(Some(testError))

      // Verify that writer resources are cleaned up
      eventually(Timeout(30.seconds)) {
        writer.server.channelFuture() should be(null)
        writer.bufferPool.size() should be(0)
      }
    }
  }

  test("writer write is called after task failure, writer resources are cleaned up") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val writer = g.writers(0)
      // Server should be running initially thus channelFuture should not be null.
      // When cleanup happens after task completion, channelFuture should be set to null.
      writer.server.channelFuture() should not be null

      writer.context.markTaskFailed(testError)

      val e = intercept[SparkException] {
        writer.write(Iterator((1, 1), (2, 2)))
      }
      checkTestError(e)

      // Verify that writer resources are cleaned up
      eventually(Timeout(30.seconds)) {
        writer.server.channelFuture() should be(null)
        writer.bufferPool.size() should be(0)
      }
    }
  }

  test("writer write is called and task is marked completed" +
    " concurrently from another thread, writer resources are cleaned up") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val writer = g.writers(0)
      // Server should be running initially thus channelFuture should not be null.
      // When cleanup happens after task completion, channelFuture should be set to null.
      writer.server.channelFuture() should not be null

      new Thread("mark task completed thread") {
          setDaemon(true)
          override def run(): Unit = {
            writer.context.markTaskFailed(testError)
          }
        }.start()

      try {
        // may or may not throw exception depending on the timing of when task failure is marked
        writer.write(Iterator((1, 1), (2, 2)))
      } catch {
        case e: SparkException =>
          checkTestError(e)
      }

      // Verify that writer resources are cleaned up
      eventually(Timeout(30.seconds)) {
        writer.server.channelFuture() should be(null)
        writer.bufferPool.size() should be(0)
      }
    }
  }


  test("writer write is called before task completion, writer resources are cleaned up") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val writer = g.writers(0)
      // Server should be running initially thus channelFuture should not be null.
      // When cleanup happens after task completion, channelFuture should be set to null.
      writer.server.channelFuture() should not be null

      writer.write(Iterator((1, 1), (2, 2)))

      writer.context.markTaskFailed(testError)

      // Verify that writer resources are cleaned up
      eventually(Timeout(30.seconds)) {
        writer.server.channelFuture() should be(null)
        writer.bufferPool.size() should be(0)
      }
    }
  }

  test("writer write is called during task completion, writer resources are cleaned up") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)

      val writer = g.writers(0)
      // Server should be running initially thus channelFuture should not be null.
      // When cleanup happens after task completion, channelFuture should be set to null.
      writer.server.channelFuture() should not be null

      val t = new Thread("writer thread") {
        override def run(): Unit = {
          writer.write(new Iterator[(Int, Int)] {
            var hasNext = true
            def next(): (Int, Int) = {
              // Simulate a write call that is blocked
              while (true) this.synchronized { this.wait() }
              (1, 1)
            }
          })
        }
      }
      t.start()

      writer.context.markTaskFailed(testError)
      t.interrupt()

      // Verify that writer resources are cleaned up
      eventually(Timeout(30.seconds)) {
        writer.server.channelFuture() should be(null)
        writer.bufferPool.size() should be(0)
      }
    }
  }

  test("client factories are closed and EventLoopGroup threads are cleaned up after read") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 2, 1)

      g.writers(0).write(Iterator((1, 1), (2, 2)))
      g.writers(1).write(Iterator((3, 3), (4, 4)))

      await(g.read(0)._2) should be(Set((1, 1), (2, 2), (3, 3), (4, 4)))

      val reader = g.readers(0)
      val factoriesField = classOf[StreamingShuffleReader[_, _]].getDeclaredFields
        .find(_.getName.endsWith("clientFactories"))
        .getOrElse(fail("Could not find clientFactories field via reflection"))
      factoriesField.setAccessible(true)
      val factories = factoriesField.get(reader)
        .asInstanceOf[java.util.concurrent.ConcurrentLinkedQueue[TransportClientFactory]]
      assert(factories.size() > 0, "Expected at least one client factory")

      val workerGroupField = classOf[TransportClientFactory].getDeclaredField("workerGroup")
      workerGroupField.setAccessible(true)

      eventually(Timeout(10.seconds)) {
        factories.forEach { factory =>
          val workerGroup = workerGroupField.get(factory)
            .asInstanceOf[_root_.io.netty.channel.EventLoopGroup]
          assert(workerGroup.isTerminated,
            "TransportClientFactory EventLoopGroup was not shut down after read completed.")
        }
      }
    }
  }

  // Demonstrates why background threads must not call context.markTaskFailed() directly:
  // doing so can cause the task thread's markTaskCompleted() to silently skip completion
  // listeners. In production, cleanupResources() is a completion listener, so skipping it
  // leaks threads. The reader avoids this by routing background errors through a
  // CompletableFuture so markTaskFailed/markTaskCompleted are both called from the task thread.
  test("background markTaskFailed races with task thread markTaskCompleted") {
    val readerContext = createTaskContext(sparkConf, partitionId = 0)
    TaskContext.setTaskContext(readerContext)

    // Latches to force the exact interleaving that triggers the race.
    val failureListenerEntered = new CountDownLatch(1)
    val failureListenerProceed = new CountDownLatch(1)

    @volatile var completionListenerRan = false

    // Add a failure listener that blocks, holding the listenerInvocationThread guard.
    readerContext.addTaskFailureListener(new org.apache.spark.util.TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
        failureListenerEntered.countDown()
        failureListenerProceed.await()
      }
    })

    // Add a completion listener -- stands in for cleanupResources() in production.
    readerContext.addTaskCompletionListener[Unit] { _ =>
      completionListenerRan = true
    }

    val error = new RuntimeException("background discovery failure")

    // Background thread calls markTaskFailed -> enters invokeListeners -> acquires
    // listenerInvocationThread guard -> blocks in the failure listener above.
    val bgThread = new Thread(() => {
      TaskContext.setTaskContext(readerContext)
      readerContext.markTaskFailed(error)
    })
    bgThread.start()

    // Wait for the failure listener to start (background thread now holds the guard).
    failureListenerEntered.await()

    // Task thread calls markTaskCompleted while the guard is held.
    // invokeListeners sees listenerInvocationThread is set -> returns immediately,
    // permanently skipping the completion listener.
    readerContext.markTaskCompleted(Some(error))

    // Release the failure listener so bgThread can finish.
    failureListenerProceed.countDown()
    bgThread.join()

    // This documents the race: the completion listener was skipped. The reader avoids it by
    // never calling markTaskFailed from a background thread.
    assert(!completionListenerRan,
      "Expected completion listener to be skipped due to the race condition. " +
      "If this starts passing, TaskContextImpl.invokeListeners may have been fixed " +
      "and this test can be updated.")

    TaskContext.unset()
  }

  // With the CompletableFuture approach, the reader's cleanupResources runs because
  // markTaskFailed is called from the task thread (no race with the completion listener).
  test("reader resources are cleaned up when client creation fails") {
    class FailingClientReader[K, C](
        handle: ShuffleHandle,
        context: TaskContext)
        extends MockStreamingShuffleReader[K, C](handle, context) {

      override def createClient(
          mapId: Int,
          remoteHost: String,
          remotePort: Int): TransportClient = {
        throw new java.io.IOException("Connection refused (simulated)")
      }
    }

    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      assert(SparkEnv.get.pipelinedShuffleManager.exists(_.isInstanceOf[StreamingShuffleManager]))
      assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId, 1, 1, 0)
      val shuffleMapRdd = new MyRDD(sc, 1, Nil)
      val shuffleDep =
        new ShuffleDependency[Int, Int, Int](shuffleMapRdd, new HashPartitioner(1))

      val writerContext = createTaskContext(sc.conf, 0)
      new StreamingShuffleWriter[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep), 0, writerContext)

      val streamingShuffleHandle = new StreamingShuffleHandle(shuffleId, shuffleDep)
      val readerContext = createTaskContext(sc.conf, 0)
      val reader = new FailingClientReader[Int, Int](streamingShuffleHandle, readerContext)

      val error = intercept[Exception] {
        reader.read().next()
      }

      // Simulate the task framework (runTaskWithListeners): markTaskFailed then
      // markTaskCompleted, both on the task thread. With the fix, the background
      // thread does NOT call markTaskFailed, so there's no contention on the
      // listenerInvocationThread guard, and completion listeners run normally.
      readerContext.markTaskFailed(error)
      readerContext.markTaskCompleted(Some(error))

      reader.taskDiscoveryExecutor.isShutdown should be(true)
      reader.clientCreationExecutor.isShutdown should be(true)
    }
  }

  // On the writer side: when a background Netty thread records an error via
  // errorNotifier.markError (instead of calling context.markTaskFailed directly), the
  // writer's write() method surfaces the error on the task thread. Because markTaskFailed
  // and markTaskCompleted are both called from the task thread, there is no contention on
  // the listenerInvocationThread guard in TaskContextImpl, and the completion listener
  // (cleanupResources) runs normally.
  test("writer resources are cleaned up when server handler " +
    "receive fails via errorNotifier") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      assert(SparkEnv.get.pipelinedShuffleManager.exists(_.isInstanceOf[StreamingShuffleManager]))
      assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId, 1, 1, 0)
      val shuffleMapRdd = new MyRDD(sc, 1, Nil)
      val shuffleDep =
        new ShuffleDependency[Int, Int, Int](shuffleMapRdd, new HashPartitioner(1))

      val writerContext = createTaskContext(sc.conf, 0)

      val writerErrorNotifier = new ErrorNotifier()
      // Inject a failing server handler whose receive method records the error in
      // errorNotifier (simulating a Netty event loop thread error).  In production this
      // is exactly what happens when StreamingShuffleServerHandler.receive catches an
      // exception and calls errorNotifier.markError.
      val writer = new StreamingShuffleWriter[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep), 0, writerContext,
        Some(new StreamingShuffleServerHandler(
            null, 0, 1, writerContext, writerErrorNotifier) {
          override def receive(
              c: TransportClient, b: ByteBuffer, r: RpcResponseCallback): Unit = {
            writerErrorNotifier.markError(testError)
            logError("Simulated server handler receive failure", testError)
          }
        }),
        writerErrorNotifier)

      // Server should be running initially.
      writer.server.channelFuture() should not be null

      // write() will block waiting for termination acks; run it asynchronously.
      val future = StreamingShuffleSuite.verifyBlockingCall { () =>
        writer.write(Iterator((1, 1), (2, 2)))
      }

      // Wait for the writer to send data and termination messages.
      eventually(Timeout(30.seconds)) {
        writer.shards(0).lastSentSequenceNum.get() should be(1) // 1 data msg + 1 term msg
      }

      // A reader connecting triggers the server handler's receive method,
      // which injects the error into writerErrorNotifier.
      new MockStreamingShuffleReader[Int, Int](
        new StreamingShuffleHandle(shuffleId, shuffleDep),
        writerContext
      )

      // write() should surface the error via throwErrorIfExists() in the ack-wait loop.
      val writeError = intercept[Exception] {
        ThreadUtils.awaitResult(future, 30.seconds)
      }

      // Simulate the task framework (runTaskWithListeners): markTaskFailed then
      // markTaskCompleted, both on the task thread.  With the ErrorNotifier fix, the
      // background Netty thread does NOT call markTaskFailed, so there is no contention
      // on the listenerInvocationThread guard, and the completion listener
      // (cleanupResources) runs normally.
      writerContext.markTaskFailed(writeError)
      writerContext.markTaskCompleted(Some(writeError))

      // Verify that cleanupResources ran: server closed and buffer pool drained.
      writer.server.channelFuture() should be(null)
      writer.bufferPool.size() should be(0)
    }
  }

  test("writer sequence numbers are monotonically increasing") {
    withSpark(new SparkContext("local", "StreamingShuffleSuite", sparkConf)) { sc =>
      val g = new ShuffleGroup[Int](sc, 1, 1)
      g.writers(0).write(Iterator.range(0, 10).map(i => (i, i)))
      val lastSeq = g.writers(0).shards(0).lastSentSequenceNum.get()
      assert(lastSeq >= 1, s"Expected at least one message sent, got seqNum $lastSeq")
      assert(await(g.read(0)._2) == (0 until 10).map(i => (i, i)).toSet)
    }
  }
}

object StreamingShuffleSuite {
  def newDataMsgFunc[T: ClassTag](
      shuffleDep: ShuffleDependency[T, T, T],
      readerId: Int): (Int, T) => DataMessage = { (writerId: Int, datum: T) =>
    val BUFFER_SIZE = 1000
    // a DataMessage data containing only a dummy (K,V) record
    val partitionBuffer = PooledByteBufAllocator.DEFAULT.buffer(BUFFER_SIZE)
    val serializerInstance = shuffleDep.serializer.newInstance()
    val partitionSerializationStream =
      serializerInstance.serializeStream(new ByteBufOutputStream(partitionBuffer))
    if (serializerInstance.isInstanceOf[JavaSerializerInstance]) {
      partitionSerializationStream.writeKey(datum.asInstanceOf[Any])
    }
    partitionSerializationStream.writeValue(datum.asInstanceOf[Any])
    partitionSerializationStream.flush()

    // Calculate checksum
    val shuffleChecksum = new ShuffleChecksum()
    shuffleChecksum.updateChecksum(
      partitionBuffer, partitionBuffer.readerIndex(), partitionBuffer.readableBytes())
    val calculatedChecksum = shuffleChecksum.getValue()

    val msg = new DataMessage(writerId, readerId, partitionBuffer.writerIndex(),
      partitionBuffer, calculatedChecksum)
    partitionBuffer.release()
    msg
  }

  def verifyBlockingCall[T](blockingFn: () => T): Future[T] = {
    val future = Future {
      blockingFn()
    }(ExecutionContext.global)

    val timeout = 2.seconds
    // future should not finish
    intercept[TimeoutException] {
      ThreadUtils.awaitResult(future, timeout)
    }

    future
  }
}

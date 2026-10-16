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

package org.apache.spark.api.python

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, Closeable, DataInputStream, DataOutputStream, EOFException}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ClosedByInterruptException, ServerSocketChannel, SocketChannel}
import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList, HashMap => JHashMap}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.duration._

import org.apache.logging.log4j.Level
import org.scalatest.concurrent.Eventually.{eventually, timeout}

import org.apache.spark.{LocalSparkContext, SparkContext, SparkEnv, SparkException, SparkFunSuite, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.{DirectByteBufferOutputStream, TaskCompletionListener}

class BasePythonRunnerSuite extends SparkFunSuite {

  test("SPARK-58192: pipelined writer pool admits more concurrent writers than processors") {
    // A fractional spark.task.cpus admits more concurrent tasks -- each holding one writer
    // thread -- than the host has processors. Writers of a barrier stage block until every
    // task's worker makes progress, so a pool that queues writers beyond a processor-based
    // cap can deadlock; all writers must be able to start concurrently.
    val nWriters = Runtime.getRuntime.availableProcessors() + 2
    val started = new CountDownLatch(nWriters)
    val release = new CountDownLatch(1)
    val futures = (1 to nWriters).map { _ =>
      BasePythonRunner.pipelinedWriterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          started.countDown()
          release.await(1, TimeUnit.MINUTES)
        }
      })
    }
    try {
      assert(started.await(1, TimeUnit.MINUTES),
        s"expected all $nWriters writers to start concurrently")
    } finally {
      release.countDown()
      futures.foreach(_.get(1, TimeUnit.MINUTES))
    }
  }

  /**
   * A writer that, once started, runs `onStart` and then keeps running until `release` is
   * counted down, ignoring interrupts the way a writer in the middle of copying a row into a
   * HybridRowQueue page does.
   */
  private class UninterruptibleWriter(
      onStart: UninterruptibleWriter => Unit = _ => ()) extends Runnable {
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    @volatile var exited = false

    override def run(): Unit = {
      try {
        onStart(this)
        started.countDown()
        awaitIgnoringInterrupts(release)
      } finally {
        exited = true
      }
    }
  }

  /** Waits for `latch`, ignoring interrupts like a writer in the middle of a row copy. */
  private def awaitIgnoringInterrupts(latch: CountDownLatch): Unit = {
    var done = false
    while (!done) {
      try {
        done = latch.await(1, TimeUnit.MINUTES)
      } catch {
        case _: InterruptedException => // keep going
      }
    }
  }

  /** Adds a completion listener that records whether `writer` had exited when it ran. */
  private def addCleanupListener(
      context: TaskContext,
      writer: UninterruptibleWriter): AtomicReference[Option[Boolean]] = {
    val exitedBeforeCleanup = new AtomicReference[Option[Boolean]](None)
    context.addTaskCompletionListener[Unit] { _ => exitedBeforeCleanup.set(Some(writer.exited)) }
    exitedBeforeCleanup
  }

  /**
   * Adds a pre-completion listener that runs right before the one added by
   * `startPipelinedWriter`, so that the test knows task completion has reached the writer.
   */
  private def addReachedListener(context: TaskContext): CountDownLatch = {
    val reached = new CountDownLatch(1)
    context.addTaskPreCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = reached.countDown()
    })
    reached
  }

  private def startThread(body: => Unit): Thread = {
    val thread = new Thread(() => body)
    thread.setDaemon(true)
    thread.start()
    thread
  }

  test("SPARK-60113: task completion waits for the pipelined writer to exit") {
    val context = TaskContext.empty()
    val stopListenerAdded = new CountDownLatch(1)
    @volatile var cleanupAddedByWriter: AtomicReference[Option[Boolean]] = null
    val writer = new UninterruptibleWriter(self => {
      // Like an upstream reader that adds its close listener when the writer first pulls it.
      // Waits so that it is always added after the stop listener, as when the writer pulls its
      // input after startPipelinedWriter returns, which an ordinary completion listener for the
      // stop would let run first.
      awaitIgnoringInterrupts(stopListenerAdded)
      cleanupAddedByWriter = addCleanupListener(context, self)
    })
    // Like the listener that closes the HybridRowQueue, added on the task thread before the
    // writer starts.
    val cleanupAddedBefore = addCleanupListener(context, writer)
    var completion: Thread = null
    try {
      BasePythonRunner.startPipelinedWriter(writer, context)
      stopListenerAdded.countDown()
      assert(writer.started.await(1, TimeUnit.MINUTES))
      val reached = addReachedListener(context)

      completion = startThread(context.markTaskCompleted(None))
      assert(reached.await(1, TimeUnit.MINUTES))
      // Task completion must stay blocked while the writer is still running.
      completion.join(500)
      assert(completion.isAlive)
      assert(cleanupAddedBefore.get.isEmpty)
      assert(cleanupAddedByWriter.get.isEmpty)
    } finally {
      stopListenerAdded.countDown()
      writer.release.countDown()
    }
    completion.join(TimeUnit.MINUTES.toMillis(1))
    assert(!completion.isAlive)
    assert(cleanupAddedBefore.get === Some(true))
    assert(cleanupAddedByWriter.get === Some(true))
  }

  test("SPARK-60113: a listener the writer adds once completion has started waits for it") {
    val context = TaskContext.empty()
    val addNow = new CountDownLatch(1)
    val added = new CountDownLatch(1)
    // Whether the writer had exited, and the thread, when the listener added by the writer ran.
    val lateCleanup = new AtomicReference[Option[(Boolean, Thread)]](None)
    val writerStarted = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    // Like a writer that adds a listener when it pulls more input after completion has started.
    val lateWriter: Runnable = new Runnable {
      @volatile var exited = false
      override def run(): Unit = {
        try {
          writerStarted.countDown()
          awaitIgnoringInterrupts(addNow)
          context.addTaskCompletionListener[Unit] { _ =>
            lateCleanup.set(Some((exited, Thread.currentThread())))
          }
          added.countDown()
          awaitIgnoringInterrupts(release)
        } finally {
          exited = true
        }
      }
    }
    var completion: Thread = null
    try {
      BasePythonRunner.startPipelinedWriter(lateWriter, context)
      assert(writerStarted.await(1, TimeUnit.MINUTES))
      val reached = addReachedListener(context)

      completion = startThread(context.markTaskCompleted(None))
      assert(reached.await(1, TimeUnit.MINUTES))
      addNow.countDown()
      assert(added.await(1, TimeUnit.MINUTES))
      // The writer thread only queued the listener: task completion still waits for the writer.
      completion.join(500)
      assert(completion.isAlive)
      assert(lateCleanup.get.isEmpty)
    } finally {
      addNow.countDown()
      release.countDown()
    }
    completion.join(TimeUnit.MINUTES.toMillis(1))
    assert(!completion.isAlive)
    assert(lateCleanup.get === Some((true, completion)))
  }

  test("SPARK-60113: an interrupt does not stop the wait and is not passed to later listeners") {
    val context = TaskContext.empty()
    val writer = new UninterruptibleWriter
    val cleanup = addCleanupListener(context, writer)
    val interruptedInCleanup = new AtomicReference[Option[Boolean]](None)
    context.addTaskCompletionListener[Unit] { _ =>
      interruptedInCleanup.set(Some(Thread.currentThread().isInterrupted))
    }
    BasePythonRunner.startPipelinedWriter(writer, context)
    assert(writer.started.await(1, TimeUnit.MINUTES))
    val reached = addReachedListener(context)

    val completion = startThread(context.markTaskCompleted(None))
    try {
      assert(reached.await(1, TimeUnit.MINUTES))
      // Interrupt the completion thread once it blocks in the wait, e.g. a task kill that lands
      // while the listeners run.
      eventually(timeout(1.minute)) {
        assert(completion.getState == Thread.State.TIMED_WAITING)
      }
      completion.interrupt()
      completion.join(500)
      assert(completion.isAlive)
      assert(cleanup.get.isEmpty)
    } finally {
      writer.release.countDown()
    }
    completion.join(TimeUnit.MINUTES.toMillis(1))
    assert(!completion.isAlive)
    assert(cleanup.get === Some(true))
    assert(interruptedInCleanup.get === Some(false))
  }

  test("SPARK-60113: a pipelined writer stopped before it starts never runs") {
    val writer = new UninterruptibleWriter
    val writerTask =
      new BasePythonRunner.PipelinedWriterTask(writer, TaskContext.empty(), "test task")
    // The pool has not started the writer yet: stopping must not wait for it.
    writerTask.stopAndAwaitExit()
    // Stopping again does not wait either.
    writerTask.stopAndAwaitExit()
    // Even if the pool thread reaches the task afterwards, the writer must not run. Release it
    // up front so that a writer that wrongly runs fails the test instead of hanging it.
    writer.release.countDown()
    writerTask.run()
    assert(writer.started.getCount === 1)
    assert(!writer.exited)
  }

  test("SPARK-60113: a fatal error of the pipelined writer is logged") {
    val writerTask = new BasePythonRunner.PipelinedWriterTask(
      // scalastyle:off throwerror
      () => throw new LinkageError("fatal writer error"), TaskContext.empty(), "test task")
      // scalastyle:on throwerror
    val logAppender = new LogAppender("pipelined writer failure")
    withLogAppender(logAppender) {
      intercept[LinkageError] {
        writerTask.run()
      }
    }
    assert(logAppender.loggingEvents.exists { event =>
      event.getLevel == Level.ERROR &&
        event.getMessage.getFormattedMessage.contains("Pipelined Python writer of test task") &&
        event.getThrown.getMessage == "fatal writer error"
    })
    // The writer has exited, so stopping does not wait.
    writerTask.stopAndAwaitExit()
  }

  test("SPARK-60113: stopping on the writer thread itself does not wait for the writer") {
    val taskRef = new AtomicReference[BasePythonRunner.PipelinedWriterTask]()
    val stopReturned = new CountDownLatch(1)
    val interruptedAfterStop = new AtomicReference[Option[Boolean]](None)
    val writerTask = new BasePythonRunner.PipelinedWriterTask(() => {
      // Like task completion listeners handed to the writer thread while it pulls its input.
      taskRef.get.stopAndAwaitExit()
      interruptedAfterStop.set(Some(Thread.currentThread().isInterrupted))
      stopReturned.countDown()
    }, TaskContext.empty(), "test task")
    taskRef.set(writerTask)
    val writerThread = startThread(writerTask.run())
    assert(stopReturned.await(1, TimeUnit.MINUTES))
    // Stopping leaves the interrupt status of the thread it runs on as it was, so that the
    // listeners that run next on it can clean up.
    assert(interruptedAfterStop.get === Some(false))
    writerThread.join(TimeUnit.MINUTES.toMillis(1))
    assert(!writerThread.isAlive)
    // The writer has exited, so stopping from another thread does not wait either.
    writerTask.stopAndAwaitExit()
  }

  test("SPARK-60113: the writer is interrupted again if it consumed the interrupt") {
    val interrupts = new AtomicInteger(0)
    val writerTask = new BasePythonRunner.PipelinedWriterTask(() => {
      // Like upstream code that catches the first InterruptedException and carries on, and then
      // blocks again (e.g. in channel.write), where the next interrupt stops it.
      val blocker = new CountDownLatch(1)
      var stopped = false
      while (!stopped) {
        try {
          blocker.await()
        } catch {
          case _: InterruptedException =>
            stopped = interrupts.incrementAndGet() >= 2
        }
      }
    }, TaskContext.empty(), "test task", warnIntervalMs = 100)
    val writerThread = startThread(writerTask.run())
    eventually(timeout(1.minute)) {
      assert(writerThread.getState == Thread.State.WAITING)
    }

    val stopper = startThread(writerTask.stopAndAwaitExit())
    stopper.join(TimeUnit.MINUTES.toMillis(1))
    assert(!stopper.isAlive)
    assert(interrupts.get >= 2)
    writerThread.join(TimeUnit.MINUTES.toMillis(1))
  }

  test("SPARK-60113: an interrupt pending before stopping the writer is kept") {
    // The writer never started.
    val notStarted =
      new BasePythonRunner.PipelinedWriterTask(() => (), TaskContext.empty(), "test task")
    // The writer runs and exits once interrupted.
    val running = new BasePythonRunner.PipelinedWriterTask(() => {
      try {
        new CountDownLatch(1).await()
      } catch {
        case _: InterruptedException =>
      }
    }, TaskContext.empty(), "test task")
    val writerThread = startThread(running.run())
    eventually(timeout(1.minute)) {
      assert(writerThread.getState == Thread.State.WAITING)
    }

    Seq(notStarted, running).foreach { writerTask =>
      val interruptedAfterStop = new AtomicReference[Option[Boolean]](None)
      val stopper = startThread {
        Thread.currentThread().interrupt()
        writerTask.stopAndAwaitExit()
        interruptedAfterStop.set(Some(Thread.currentThread().isInterrupted))
      }
      stopper.join(TimeUnit.MINUTES.toMillis(1))
      assert(interruptedAfterStop.get === Some(true))
    }
    writerThread.join(TimeUnit.MINUTES.toMillis(1))
  }

  test("SPARK-60113: stopping interrupts a running writer right away") {
    // A writer that exits once interrupted, with no warning (and so no repeated interrupt)
    // before the test times out.
    val writerTask = new BasePythonRunner.PipelinedWriterTask(() => {
      try {
        new CountDownLatch(1).await()
      } catch {
        case _: InterruptedException =>
      }
    }, TaskContext.empty(), "test task", warnIntervalMs = TimeUnit.MINUTES.toMillis(10))
    val writerThread = startThread(writerTask.run())
    eventually(timeout(1.minute)) {
      assert(writerThread.getState == Thread.State.WAITING)
    }
    val stopper = startThread(writerTask.stopAndAwaitExit())
    stopper.join(TimeUnit.MINUTES.toMillis(1))
    assert(!stopper.isAlive)
    writerThread.join(TimeUnit.MINUTES.toMillis(1))
    assert(!writerThread.isAlive)
  }

  test("SPARK-60113: an interrupt that arrives while stopping the writer is not passed on") {
    val writerTask =
      new BasePythonRunner.PipelinedWriterTask(() => (), TaskContext.empty(), "test task")
    val interruptedAfterStop = new AtomicReference[Option[Boolean]](None)
    var stopper: Thread = null
    writerTask.synchronized {
      // Blocks on the task's lock, where the interrupt then arrives.
      stopper = startThread {
        writerTask.stopAndAwaitExit()
        interruptedAfterStop.set(Some(Thread.currentThread().isInterrupted))
      }
      eventually(timeout(1.minute)) {
        assert(stopper.getState == Thread.State.BLOCKED)
      }
      stopper.interrupt()
    }
    stopper.join(TimeUnit.MINUTES.toMillis(1))
    assert(!stopper.isAlive)
    assert(interruptedAfterStop.get === Some(false))
  }

  test("SPARK-60113: the writer is not interrupted while it creates a resource uninterruptibly") {
    val context = TaskContext.empty()
    val inBuilder = new CountDownLatch(1)
    val releaseBuilder = new CountDownLatch(1)
    val interruptedInBuilder = new AtomicReference[Option[Boolean]](None)
    val writerTask = new BasePythonRunner.PipelinedWriterTask(() => {
      // Like an upstream scan that opens a file through createResourceUninterruptibly.
      context.createResourceUninterruptibly {
        inBuilder.countDown()
        var interrupted = false
        var done = false
        while (!done) {
          try {
            done = releaseBuilder.await(1, TimeUnit.MINUTES)
          } catch {
            case _: InterruptedException => interrupted = true
          }
        }
        interruptedInBuilder.set(Some(interrupted || Thread.currentThread().isInterrupted))
        new Closeable {
          override def close(): Unit = {}
        }
      }
      // Then blocks until it is interrupted.
      try {
        new CountDownLatch(1).await()
      } catch {
        case _: InterruptedException =>
      }
    }, context, "test task", warnIntervalMs = 100)
    val writerThread = startThread(writerTask.run())
    var stopper: Thread = null
    try {
      assert(inBuilder.await(1, TimeUnit.MINUTES))
      stopper = startThread(writerTask.stopAndAwaitExit())
      // Several warnings pass while the writer is in the builder.
      stopper.join(500)
      assert(stopper.isAlive)
    } finally {
      releaseBuilder.countDown()
    }
    stopper.join(TimeUnit.MINUTES.toMillis(1))
    assert(!stopper.isAlive)
    assert(interruptedInBuilder.get === Some(false))
    writerThread.join(TimeUnit.MINUTES.toMillis(1))
  }

  test("SPARK-60113: an interrupted channel write does not free the writer's buffer") {
    val bufferStream = new DirectByteBufferOutputStream(16)
    try {
      val channel =
        Channels.newChannel(new DataOutputStream(new CloseShieldOutputStream(bufferStream)))
      // An interrupt during the write closes the channel and its stream, like one from the
      // thread that stops the writer, which the JDK runs on that thread.
      Thread.currentThread().interrupt()
      try {
        intercept[ClosedByInterruptException] {
          channel.write(ByteBuffer.wrap(Array[Byte](1, 2)))
        }
      } finally {
        Thread.interrupted()
      }
      // The JDK closes the stream before it copies the bytes, and throws only after that copy,
      // so the copy and later writes must still find the buffer: freeing it is left to its
      // owner.
      bufferStream.write(3)
      assert(bufferStream.size() === 3)
    } finally {
      bufferStream.close()
    }
  }

  /**
   * Runs a real `PipelinedWriterRunnable` whose input is written by `writeNextInput`, against a
   * socket pair standing in for the Python worker, and calls `check` with the writer and the
   * Python side of the socket afterwards. `run` runs the runnable and may expect it to throw.
   */
  private def withPipelinedWriterRunnable(
      writeNextInput: DataOutputStream => Boolean)(
      run: Runnable => Unit)(
      check: (BasePythonRunner[Array[Byte], Array[Byte]]#Writer, SocketChannel) => Unit): Unit = {
    LocalSparkContext.withSpark(new SparkContext("local", "test")) { _ =>
      val func = SimplePythonFunction(
        Seq.empty, new JHashMap[String, String](), new JArrayList[String](), "python3", "3",
        new JArrayList[Broadcast[PythonBroadcast]](), null)
      val runner = new PythonRunner(Seq(ChainedPythonFunctions(Seq(func))), None)
      val server = ServerSocketChannel.open()
      try {
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        val channel = SocketChannel.open(server.getLocalAddress)
        val pythonSide = server.accept()
        try {
          val context = TaskContext.empty()
          val worker = PythonWorker(channel)
          val writer = new runner.Writer(SparkEnv.get, worker, Iterator.empty, 0, context) {
            override def open(dataOut: DataOutputStream): Unit = {}
            override protected def writeCommand(dataOut: DataOutputStream): Unit = {}
            override def writeNextInputToStream(dataOut: DataOutputStream): Boolean =
              writeNextInput(dataOut)
            override def close(dataOut: DataOutputStream): Unit = {}
          }
          run(new runner.PipelinedWriterRunnable(worker, writer, 1024, context))
          check(writer, pythonSide)
        } finally {
          pythonSide.close()
          channel.close()
        }
      } finally {
        server.close()
      }
    }
  }

  /** Reads one byte from the Python side of the socket, failing instead of blocking forever. */
  private def readFromPythonSide(pythonSide: SocketChannel): Int = {
    pythonSide.socket().setSoTimeout(TimeUnit.MINUTES.toMillis(1).toInt)
    pythonSide.socket().getInputStream.read()
  }

  test("SPARK-60113: a fatal error of the pipelined writer is passed to the reader") {
    withPipelinedWriterRunnable { _ =>
      // scalastyle:off throwerror
      throw new LinkageError("fatal writer error")
      // scalastyle:on throwerror
    } { runnable =>
      intercept[LinkageError] {
        runnable.run()
      }
    } { (writer, pythonSide) =>
      // The reader rethrows the error instead of waiting for the Python worker, which sees the
      // end of its input.
      assert(writer.exception.map(_.getMessage) === Some("fatal writer error"))
      assert(readFromPythonSide(pythonSide) === -1)
    }
  }

  test("SPARK-60113: an interrupted Arrow-style write leaves the writer's buffer usable") {
    withPipelinedWriterRunnable { dataOut =>
      // Like an Arrow writer, which writes through Channels.newChannel(dataOut), interrupted by
      // the thread that stops the writer.
      Thread.currentThread().interrupt()
      try {
        Channels.newChannel(dataOut).write(ByteBuffer.wrap(Array[Byte](1, 2)))
      } catch {
        case _: ClosedByInterruptException =>
      } finally {
        Thread.interrupted()
      }
      // Fails if the interrupt freed the buffer behind dataOut.
      dataOut.writeInt(42)
      false
    } { runnable =>
      runnable.run()
    } { (writer, pythonSide) =>
      assert(writer.exception.isEmpty)
      val input = {
        pythonSide.socket().setSoTimeout(TimeUnit.MINUTES.toMillis(1).toInt)
        new DataInputStream(pythonSide.socket().getInputStream)
      }
      // The bytes copied by the interrupted write, which the JDK copies after closing the
      // stream, and then the next write.
      assert(input.readByte() === 1)
      assert(input.readByte() === 2)
      assert(input.readInt() === 42)
    }
  }

  test("SPARK-58192: pyspark memory is split across the executor's concurrent task slots") {
    def workerMemoryMb(maxConcurrentTasks: Int): Option[Long] =
      BasePythonRunner.getWorkerMemoryMb(Some(4096L), maxConcurrentTasks)

    // One slot per concurrent task; each worker gets an equal share of the executor allocation.
    assert(workerMemoryMb(4) === Some(1024L))
    // A fractional spark.task.cpus (e.g. 4 cores / 0.5) admits more concurrent tasks than cores,
    // so each worker gets a smaller share and the aggregate stays within the allocation.
    assert(workerMemoryMb(8) === Some(512L))
    assert(workerMemoryMb(5) === Some(819L))
    // The concurrency is the limiting resource, not just cpu slots: a GPU that caps the executor
    // at a single concurrent task means the sole worker keeps the whole allocation, even though
    // the cpu-slot count (e.g. 64 cores / 0.1 = 640) is far higher.
    assert(workerMemoryMb(1) === Some(4096L))
    // Never split into less than one slot.
    assert(BasePythonRunner.getWorkerMemoryMb(Some(4096L), 0) === Some(4096L))
    // No pyspark memory configured means no per-worker limit.
    assert(BasePythonRunner.getWorkerMemoryMb(None, 8) === None)
  }

  test("SPARK-58192: fail fast when the per-slot pyspark memory share rounds to zero") {
    // 512 MiB across 640 genuinely concurrent tasks rounds down to 0, which the worker would
    // treat as "no limit". Fail fast rather than silently dropping the configured cap.
    val e = intercept[SparkException] {
      BasePythonRunner.getWorkerMemoryMb(Some(512L), 640)
    }
    assert(e.getMessage.contains("spark.executor.pyspark.memory"))
    assert(e.getMessage.contains("640"))
    // A share of exactly 1 MiB is still enforceable and must not fail.
    assert(BasePythonRunner.getWorkerMemoryMb(Some(640L), 640) === Some(1L))
    // An explicit spark.executor.pyspark.memory=0 means the limit is disabled (the worker
    // treats 0 as "no limit"), so it must pass through rather than fail fast, even when
    // concurrency is high enough that a positive budget would round to zero.
    assert(BasePythonRunner.getWorkerMemoryMb(Some(0L), 640) === Some(0L))
    assert(BasePythonRunner.getWorkerMemoryMb(Some(0L), 1) === Some(0L))
    // The smallest positive budget still fails fast when it cannot give every slot at least
    // 1 MiB; only an explicit budget of 0 disables the limit.
    intercept[SparkException] {
      BasePythonRunner.getWorkerMemoryMb(Some(1L), 2)
    }
  }

  private val metricsReport =
    "{\"bootTimestampMs\":1250,\"initTimestampMs\":2500," +
      "\"finishTimestampMs\":3750,\"pythonExecutionDurationMs\":42," +
      "\"memoryBytesSpilled\":7,\"diskBytesSpilled\":9}"
  private val expectedMetrics = BasePythonRunner.WorkerMetrics(1250L, 2500L, 3750L, 42L, 7L, 9L)

  private def framedMetricsStream(json: String, declaredLength: Option[Int] = None,
      endMarkers: Boolean = true): DataInputStream = {
    val bytes = json.getBytes(StandardCharsets.UTF_8)
    val buffer = new ByteArrayOutputStream()
    val output = new DataOutputStream(buffer)
    output.writeInt(declaredLength.getOrElse(bytes.length))
    output.write(bytes)
    if (endMarkers) {
      output.writeInt(SpecialLengths.END_OF_DATA_SECTION)
      output.writeInt(0) // No accumulator updates.
      output.writeInt(SpecialLengths.END_OF_STREAM)
    }
    new DataInputStream(new ByteArrayInputStream(buffer.toByteArray))
  }

  private def checkEndMarkers(stream: DataInputStream): Unit = {
    assert(stream.readInt() == SpecialLengths.END_OF_DATA_SECTION)
    assert(stream.readInt() == 0)
    assert(stream.readInt() == SpecialLengths.END_OF_STREAM)
    assert(stream.read() == -1)
  }

  test("SPARK-59773: read worker metrics and leave the following sections aligned") {
    val withExtra = metricsReport.dropRight(1) + ",\"futureMetric\":{\"value\":[1,2]}}"
    val stream = framedMetricsStream(withExtra)
    assert(BasePythonRunner.readWorkerMetrics(stream) == expectedMetrics)
    checkEndMarkers(stream)
  }

  test("SPARK-59773: worker metrics require all current fields") {
    val fields = Seq(
      "bootTimestampMs" -> "\"bootTimestampMs\":1250,",
      "initTimestampMs" -> "\"initTimestampMs\":2500,",
      "finishTimestampMs" -> "\"finishTimestampMs\":3750,",
      "pythonExecutionDurationMs" -> "\"pythonExecutionDurationMs\":42,",
      "memoryBytesSpilled" -> "\"memoryBytesSpilled\":7,",
      "diskBytesSpilled" -> ",\"diskBytesSpilled\":9")
    fields.foreach { case (name, field) =>
      val stream = framedMetricsStream(metricsReport.replace(field, ""))
      val error = intercept[SparkException] {
        BasePythonRunner.readWorkerMetrics(stream)
      }
      assert(error.getMessage.contains(name))
      checkEndMarkers(stream)
    }
  }

  test("SPARK-59773: worker metrics reject non-int64 values") {
    val invalidValues = Seq("null", "true", "\"42\"", "42.0", "[]", "{}",
      "9223372036854775808", "-9223372036854775809")
    val fields = Seq("pythonExecutionDurationMs" -> 42,
      "memoryBytesSpilled" -> 7, "diskBytesSpilled" -> 9)
    for {
      (name, originalValue) <- fields
      value <- invalidValues
    } {
      val field = "\"" + name + "\":"
      val json = metricsReport.replace(field + originalValue, field + value)
      val stream = framedMetricsStream(json)
      val error = intercept[SparkException] {
        BasePythonRunner.readWorkerMetrics(stream)
      }
      assert(error.getMessage.contains(name))
      checkEndMarkers(stream)
    }
  }

  test("SPARK-59773: worker metrics preserve int64 values exactly") {
    Seq(Long.MinValue, 9007199254740993L, Long.MaxValue).foreach { value =>
      val json = metricsReport.replace(
        "\"pythonExecutionDurationMs\":42", "\"pythonExecutionDurationMs\":" + value)
        .replace("\"memoryBytesSpilled\":7", "\"memoryBytesSpilled\":" + value)
        .replace("\"diskBytesSpilled\":9", "\"diskBytesSpilled\":" + value)
      val stream = framedMetricsStream(json)
      assert(BasePythonRunner.readWorkerMetrics(stream) ==
        expectedMetrics.copy(pythonExecutionDurationMs = value,
          memoryBytesSpilled = value, diskBytesSpilled = value))
      checkEndMarkers(stream)
    }
  }

  test("SPARK-59773: worker metrics reject malformed, duplicate, and non-object JSON") {
    val invalid = Seq("[]", "null", "42", metricsReport.dropRight(1), metricsReport + "{}",
      metricsReport.dropRight(1) + ",\"bootTimestampMs\":9}", "{\"future\":1,\"future\":2}")
    invalid.foreach { json =>
      val stream = framedMetricsStream(json)
      intercept[SparkException] {
        BasePythonRunner.readWorkerMetrics(stream)
      }
      checkEndMarkers(stream)
    }
  }

  test("SPARK-59773: worker metrics reject invalid lengths and truncated frames") {
    Seq(-1, 0).foreach { length =>
      intercept[SparkException] {
        BasePythonRunner.readWorkerMetrics(framedMetricsStream(metricsReport, Some(length)))
      }
    }

    val truncated = framedMetricsStream(metricsReport,
      Some(metricsReport.getBytes(StandardCharsets.UTF_8).length + 1), endMarkers = false)
    intercept[EOFException] {
      BasePythonRunner.readWorkerMetrics(truncated)
    }
  }

  test("SPARK-59773: worker metrics accept a larger report with an additional field") {
    val json = metricsReport.dropRight(1) + ",\"future\":\"" + ("x" * (70 * 1024)) + "\"}"
    val stream = framedMetricsStream(json)
    assert(BasePythonRunner.readWorkerMetrics(stream) == expectedMetrics)
    checkEndMarkers(stream)
  }
}

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

import java.io.{Closeable, DataInputStream, DataOutputStream}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ClosedByInterruptException, ServerSocketChannel, SocketChannel}
import java.util.Collections
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import com.google.common.util.concurrent.Uninterruptibles
import org.apache.logging.log4j.Level
import org.scalatest.concurrent.Eventually.{eventually, timeout}

import org.apache.spark.{SharedSparkContext, SparkEnv, SparkException, SparkFunSuite, TaskContext}
import org.apache.spark.util.TaskCompletionListener

class BasePythonRunnerSuite extends SparkFunSuite with SharedSparkContext {

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
  private def awaitIgnoringInterrupts(latch: CountDownLatch): Unit =
    Uninterruptibles.awaitUninterruptibly(latch)

  /** Blocks until this thread is interrupted, like a writer that exits on an interrupt. */
  private def awaitInterrupt(): Unit = {
    try {
      new CountDownLatch(1).await()
    } catch {
      case _: InterruptedException =>
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
    var completion: Thread = null
    try {
      BasePythonRunner.startPipelinedWriter(writer, context)
      assert(writer.started.await(1, TimeUnit.MINUTES))
      val reached = addReachedListener(context)

      completion = startThread(context.markTaskCompleted(None))
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

  test("SPARK-60113: a writer started after the task completed never runs") {
    // Like startPipelinedWriter called on a helper thread (e.g. PipedRDD's stdin writer) after
    // the task completed: the stop listener runs at once and keeps the writer from starting.
    val context = TaskContext.empty()
    context.markTaskCompleted(None)
    val ran = new CountDownLatch(1)
    BasePythonRunner.startPipelinedWriter(() => ran.countDown(), context)
    assert(!ran.await(500, TimeUnit.MILLISECONDS))
  }

  test("SPARK-60113: a writer started while another thread runs the listeners never runs") {
    // Like startPipelinedWriter called on a helper thread (e.g. PipedRDD's stdin writer) after
    // the task completed, while the completing thread runs the completion listeners: the stop
    // listener is then only queued, so the writer must not be submitted at all.
    val context = TaskContext.empty()
    val ran = new CountDownLatch(1)
    context.addTaskCompletionListener[Unit] { _ =>
      val helper =
        startThread(BasePythonRunner.startPipelinedWriter(() => ran.countDown(), context))
      helper.join(TimeUnit.MINUTES.toMillis(1))
      assert(!helper.isAlive)
    }
    context.markTaskCompleted(None)
    assert(!ran.await(500, TimeUnit.MILLISECONDS))
  }

  test("SPARK-60113: a failure to interrupt the writer does not end the wait") {
    val writer = new UninterruptibleWriter
    val writerTask = new BasePythonRunner.PipelinedWriterTask(
      writer, TaskContext.empty(), "test task", warnIntervalMs = 100)
    val interruptFailure = new IllegalStateException("cannot interrupt")
    // The first interrupt and the ones on every warning fail.
    val writerThread = new Thread(() => writerTask.run()) {
      override def interrupt(): Unit = throw interruptFailure
    }
    writerThread.setDaemon(true)
    writerThread.start()
    val stopFailure = new AtomicReference[Option[Throwable]](None)
    var stopper: Thread = null
    try {
      assert(writer.started.await(1, TimeUnit.MINUTES))
      stopper = startThread {
        try {
          writerTask.stopAndAwaitExit()
        } catch {
          case t: Throwable => stopFailure.set(Some(t))
        }
      }
      // Several warnings pass, and their interrupts fail, while the writer still runs.
      stopper.join(500)
      assert(stopper.isAlive)
      assert(stopFailure.get.isEmpty)
    } finally {
      writer.release.countDown()
    }
    stopper.join(TimeUnit.MINUTES.toMillis(1))
    assert(!stopper.isAlive)
    // Thrown once the writer has exited.
    assert(writer.exited)
    assert(stopFailure.get === Some(interruptFailure))
    writerThread.join(TimeUnit.MINUTES.toMillis(1))
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

  /**
   * Runs a writer that consumes the first interrupt and stops at the second one, on a thread made
   * by `newThread`, stops it, and returns the failure that stopping threw, if any. Fails if
   * stopping does not return.
   */
  private def stopWriterThatConsumesAnInterrupt(
      newThread: Runnable => Thread): (Int, Option[Throwable]) = {
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
    val writerThread = newThread(() => writerTask.run())
    writerThread.setDaemon(true)
    writerThread.start()
    eventually(timeout(1.minute)) {
      assert(writerThread.getState == Thread.State.WAITING)
    }

    val stopFailure = new AtomicReference[Option[Throwable]](None)
    val stopper = startThread {
      try {
        writerTask.stopAndAwaitExit()
      } catch {
        case t: Throwable => stopFailure.set(Some(t))
      }
    }
    stopper.join(TimeUnit.MINUTES.toMillis(1))
    assert(!stopper.isAlive)
    writerThread.join(TimeUnit.MINUTES.toMillis(1))
    (interrupts.get, stopFailure.get)
  }

  test("SPARK-60113: the writer is interrupted again if it consumed the interrupt") {
    val (interrupts, stopFailure) = stopWriterThatConsumesAnInterrupt(new Thread(_))
    assert(interrupts >= 2)
    assert(stopFailure.isEmpty)
  }

  test("SPARK-60113: a failing warning does not skip the repeated interrupt") {
    // The warning fails on every tick, since it takes the writer thread's stack trace.
    val warningFailure = new IllegalStateException("cannot take the stack trace")
    val (interrupts, stopFailure) = stopWriterThatConsumesAnInterrupt { runnable =>
      new Thread(runnable) {
        override def getStackTrace: Array[StackTraceElement] = throw warningFailure
      }
    }
    assert(interrupts >= 2)
    assert(stopFailure === Some(warningFailure))
  }

  test("SPARK-60113: an interrupt pending before stopping the writer is kept") {
    // The writer never started.
    val notStarted =
      new BasePythonRunner.PipelinedWriterTask(() => (), TaskContext.empty(), "test task")
    // The writer runs and exits once interrupted.
    val running = new BasePythonRunner.PipelinedWriterTask(() => {
      awaitInterrupt()
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
      awaitInterrupt()
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
      awaitInterrupt()
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

  /**
   * Runs a real `PipelinedWriterRunnable` whose input is written by `writeNextInput`, against a
   * socket pair standing in for the Python worker, using the suite's shared SparkContext. Reads
   * from the Python side of the socket through its input stream time out after a minute. Then
   * calls `check` with the writer and the Python side of the socket. `run` runs the runnable and
   * may expect it to throw.
   */
  private def withPipelinedWriterRunnable(
      writeNextInput: DataOutputStream => Boolean)(
      run: Runnable => Unit)(
      check: (BasePythonRunner[Array[Byte], Array[Byte]]#Writer, SocketChannel) => Unit): Unit = {
    val func = SimplePythonFunction(
      Seq.empty, Collections.emptyMap(), Collections.emptyList(), "python3", "3",
      Collections.emptyList(), null)
    val runner = new PythonRunner(Seq(ChainedPythonFunctions(Seq(func))), None)
    val server = ServerSocketChannel.open()
    try {
      server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      val channel = SocketChannel.open(server.getLocalAddress)
      val pythonSide = server.accept()
      try {
        // So that reads through the socket adaptor fail instead of blocking forever.
        pythonSide.socket().setSoTimeout(TimeUnit.MINUTES.toMillis(1).toInt)
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
      assert(pythonSide.socket().getInputStream.read() === -1)
    }
  }

  test("SPARK-60113: an interrupted Arrow-style write leaves the writer's buffer usable") {
    var calls = 0
    withPipelinedWriterRunnable { dataOut =>
      calls += 1
      if (calls == 1) {
        // Fills the first buffer, so that the interrupted write below goes through the stream
        // that the writer creates after flushing it.
        dataOut.write(new Array[Byte](2000))
        true
      } else {
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
      }
    } { runnable =>
      runnable.run()
    } { (writer, pythonSide) =>
      assert(writer.exception.isEmpty)
      val input = new DataInputStream(pythonSide.socket().getInputStream)
      input.readFully(new Array[Byte](2000))
      // The bytes copied by the interrupted write, which the JDK copies after closing the
      // stream, and then the next write.
      assert(input.readByte() === 1)
      assert(input.readByte() === 2)
      assert(input.readInt() === 42)
    }
  }

  test("SPARK-60343: pipelined writer uses a new DataOutputStream for every buffer") {
    val func = SimplePythonFunction(Seq.empty, Collections.emptyMap(), Collections.emptyList(),
      "python3", "3", Collections.emptyList(), null)
    val runner = new PythonRunner(Seq(ChainedPythonFunctions(Seq(func))), None)
    val server = ServerSocketChannel.open()
      .bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val channel = SocketChannel.open(server.getLocalAddress)
    val peer = server.accept()
    try {
      val worker = PythonWorker(channel)
      val context = TaskContext.empty()
      // The size of the stream at the start of each call. Each call writes 300 bytes.
      val startSizes = ArrayBuffer.empty[Int]
      val writer = new runner.Writer(SparkEnv.get, worker, Iterator.empty, 0, context) {
        override def open(dataOut: DataOutputStream): Unit = {}
        override protected def writeCommand(dataOut: DataOutputStream): Unit = {}
        override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
          startSizes += dataOut.size()
          dataOut.write(new Array[Byte](300))
          startSizes.length < 20
        }
      }
      new runner.PipelinedWriterRunnable(worker, writer, 1024, context).run()
      assert(writer.exception.isEmpty)
      // The runners measure pythonDataSent with DataOutputStream.size(), which saturates at
      // Int.MaxValue. Like ReaderInputStream, every buffer of about 1024 bytes gets a new stream.
      assert(startSizes.toSeq === Seq.fill(5)(Seq(0, 300, 600, 900)).flatten)

      // All the data and END_OF_STREAM still reach the worker.
      channel.shutdownOutput()
      val sent = ByteBuffer.wrap(Channels.newInputStream(peer).readAllBytes())
      assert(sent.limit() === 20 * 300 + 4)
      assert(sent.getInt(sent.limit() - 4) === SpecialLengths.END_OF_STREAM)
    } finally {
      peer.close()
      channel.close()
      server.close()
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
}

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

package org.apache.spark.util

import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkFunSuite, TaskContext, TestUtils}

class TaskInputThreadSuite extends SparkFunSuite {

  /** A thread that runs `body`, and tells once it runs. */
  private class TestThread(body: => Unit) extends Thread("task-input-thread-suite") {
    val running = new CountDownLatch(1)
    setDaemon(true)

    override def run(): Unit = {
      running.countDown()
      body
    }

    def awaitRunning(): Unit = assert(running.await(1, TimeUnit.MINUTES))
  }

  /** Waits for the latch, also when the thread is interrupted. */
  private def awaitUninterruptibly(latch: CountDownLatch): Unit = {
    var done = false
    while (!done) {
      try {
        latch.await()
        done = true
      } catch {
        case _: InterruptedException =>
      }
    }
  }

  test("stop a thread and wait for it before the completion listeners run") {
    val context = TaskContext.empty()
    val stopped = new CountDownLatch(1)
    val forces = new CopyOnWriteArrayList[Boolean]()
    val aliveAtCompletion = new CopyOnWriteArrayList[Boolean]()
    lazy val thread: TestThread = new TestThread({
      // A listener that the thread adds while it reads the input also runs after it exited.
      context.addTaskCompletionListener[Unit](_ => aliveAtCompletion.add(thread.isAlive))
      awaitUninterruptibly(stopped)
    })
    context.addTaskCompletionListener[Unit](_ => aliveAtCompletion.add(thread.isAlive))
    val started = TaskInputThread.start(context, thread) { force =>
      forces.add(force)
      stopped.countDown()
    }
    assert(started)
    thread.awaitRunning()
    context.markTaskCompleted(None)
    assert(aliveAtCompletion.asScala.toSeq === Seq(false, false))
    assert(forces.asScala.toSeq === Seq(false))
  }

  test("interrupt a thread that stop does not end") {
    val context = TaskContext.empty()
    val interrupted = new AtomicBoolean(false)
    val thread = new TestThread({
      try {
        new CountDownLatch(1).await()
      } catch {
        case _: InterruptedException => interrupted.set(true)
      }
    })
    assert(TaskInputThread.start(context, thread)(_ => ()))
    thread.awaitRunning()
    context.markTaskCompleted(None)
    assert(!thread.isAlive && interrupted.get())
  }

  test("stop a thread by force after the grace period") {
    val context = TaskContext.empty()
    val forced = new CountDownLatch(1)
    val forces = new CopyOnWriteArrayList[Boolean]()
    // Ends neither when it is stopped nor when it is interrupted, as a thread blocked in a write
    // to a process that does not read its input.
    val thread = new TestThread(awaitUninterruptibly(forced))
    val started = TaskInputThread.start(context, thread, gracePeriodMs = 200) { force =>
      forces.add(force)
      if (force) {
        forced.countDown()
      }
    }
    assert(started)
    thread.awaitRunning()
    val startNs = System.nanoTime()
    context.markTaskCompleted(None)
    assert(!thread.isAlive)
    assert(forces.asScala.toSeq === Seq(false, true))
    assert(System.nanoTime() - startNs >= TimeUnit.MILLISECONDS.toNanos(200))
  }

  test("warn while waiting for a thread") {
    val context = TaskContext.empty()
    val release = new CountDownLatch(1)
    val thread = new TestThread(awaitUninterruptibly(release))
    val started = TaskInputThread.start(
        context, thread, gracePeriodMs = TimeUnit.MINUTES.toMillis(10), warnIntervalMs = 50) { _ =>
    }
    assert(started)
    thread.awaitRunning()
    val logAppender = new LogAppender("the wait for a thread that reads the input of a task")
    val completing = new Thread(() => context.markTaskCompleted(None))
    withLogAppender(logAppender) {
      completing.start()
      // The first warning has the stack trace of the thread.
      eventually(timeout(1.minute), interval(10.milliseconds)) {
        val warnings = logAppender.loggingEvents.filter(
          _.getMessage.getFormattedMessage.contains("still reads the input of task"))
        assert(warnings.length >= 2)
        assert(warnings.head.getThrown != null && warnings(1).getThrown == null)
      }
      assert(completing.isAlive)
      release.countDown()
      completing.join(TimeUnit.MINUTES.toMillis(1))
      assert(!completing.isAlive && !thread.isAlive)
    }
  }

  test("do not start a thread once the task completed") {
    val context = TaskContext.empty()
    context.markTaskCompleted(None)
    val forces = new CopyOnWriteArrayList[Boolean]()
    val thread = new TestThread(())
    assert(!TaskInputThread.start(context, thread)(force => forces.add(force)))
    assert(thread.getState === Thread.State.NEW)
    // The listener ran right away, and released what the thread would have used.
    assert(forces.asScala.toSeq === Seq(false))
  }

  test("only wait until a thread no longer reads the input") {
    val context = TaskContext.empty()
    val inputRead = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val thread = new TestThread({
      inputRead.countDown()
      awaitUninterruptibly(release)
    })
    val started = TaskInputThread.start(
        context,
        thread,
        awaitDone = Some(nanos => inputRead.await(nanos, TimeUnit.NANOSECONDS)))(_ => ())
    assert(started)
    thread.awaitRunning()
    assert(inputRead.await(1, TimeUnit.MINUTES))
    try {
      context.markTaskCompleted(None)
      assert(thread.isAlive)
    } finally {
      release.countDown()
    }
    thread.join(TimeUnit.MINUTES.toMillis(1))
    assert(!thread.isAlive)
  }

  test("keep the interrupt status of the thread that completes the task") {
    val context = TaskContext.empty()
    val stopped = new CountDownLatch(1)
    val thread = new TestThread(awaitUninterruptibly(stopped))
    assert(TaskInputThread.start(context, thread)(_ => stopped.countDown()))
    thread.awaitRunning()
    Thread.currentThread().interrupt()
    try {
      context.markTaskCompleted(None)
      assert(!thread.isAlive)
      assert(Thread.currentThread().isInterrupted)
    } finally {
      Thread.interrupted()
    }
  }

  test("a failure to stop a thread does not end the wait for it") {
    val context = TaskContext.empty()
    val interrupted = new CountDownLatch(1)
    val thread = new TestThread({
      try {
        new CountDownLatch(1).await()
      } catch {
        case _: InterruptedException => interrupted.countDown()
      }
    })
    val started = TaskInputThread.start(context, thread) { _ =>
      throw new IllegalStateException("cannot stop")
    }
    assert(started)
    thread.awaitRunning()
    context.markTaskCompleted(None)
    assert(!thread.isAlive && interrupted.getCount == 0)
  }

  test("start a thread without a task") {
    val ran = new CountDownLatch(1)
    val thread = new TestThread(ran.countDown())
    assert(TaskInputThread.start(null, thread)(_ => fail("There is no task to complete.")))
    assert(ran.await(1, TimeUnit.MINUTES))
  }

  test("kill a process that ignores the signal, and the processes that it started") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    // The shell and the command that it runs ignore the signal.
    val process =
      new ProcessBuilder("/bin/bash", "-c", "trap '' TERM; sleep 600; echo done").start()
    try {
      val children = eventually(timeout(1.minute), interval(10.milliseconds)) {
        val children = process.toHandle.descendants().iterator().asScala.toSeq
        assert(children.nonEmpty)
        children
      }
      val stopper = new TaskInputThread.ProcessStopper(process)
      stopper.stop(force = false)
      assert(!process.waitFor(500, TimeUnit.MILLISECONDS))
      assert(children.forall(_.isAlive))
      stopper.stop(force = true)
      assert(process.waitFor(1, TimeUnit.MINUTES))
      eventually(timeout(1.minute), interval(10.milliseconds)) {
        assert(children.forall(!_.isAlive))
      }
    } finally {
      process.destroyForcibly()
    }
  }

  test("kill the processes that a process started before it ended") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    // The shell ends when it is signaled, but not the command that it started, which ignores the
    // signal, and which is then no longer a descendant of the shell.
    val process = new ProcessBuilder(
      "/bin/bash", "-c", "(trap '' TERM; exec sleep 600) & wait").start()
    try {
      val children = eventually(timeout(1.minute), interval(10.milliseconds)) {
        val children = process.toHandle.descendants().iterator().asScala.toSeq
        assert(children.nonEmpty)
        children
      }
      val stopper = new TaskInputThread.ProcessStopper(process)
      stopper.stop(force = false)
      assert(process.waitFor(1, TimeUnit.MINUTES))
      stopper.stop(force = true)
      eventually(timeout(1.minute), interval(10.milliseconds)) {
        assert(children.forall(!_.isAlive))
      }
    } finally {
      process.destroyForcibly()
    }
  }
}

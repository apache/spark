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

import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.TaskContext
import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys.{TASK_NAME, THREAD_NAME, TOTAL_TIME}

/**
 * Starts the threads that read the input of a task on their own, such as the thread that writes
 * the input of a task to the process of a script while the task reads the output of the process,
 * and stops them before the completion listeners of the task run.
 *
 * The completion listeners free what such a thread may still read, such as the buffers of the
 * batch that a columnar reader returned last, or the off-heap column vectors of a vectorized
 * reader. So the task stops the thread, and waits until it no longer reads the input, in a
 * pre-completion listener, which runs before all the completion listeners of the task, also the
 * ones that the thread itself adds while it reads the input. The writer of pipelined Python UDFs
 * does the same, see `BasePythonRunner.startPipelinedWriter` (SPARK-33277).
 */
private[spark] object TaskInputThread extends Logging {

  /** How long a thread gets to stop reading the input before it is stopped by force. */
  val DEFAULT_GRACE_PERIOD_MS = 10000L

  /** How often to log while the completion of a task waits for a thread. */
  val DEFAULT_WARN_INTERVAL_MS = 10000L

  /**
   * Starts `thread`, which reads the input of the task of `context`, unless the task already
   * completed, and makes the task stop it before its completion listeners run. Without a task,
   * it only starts the thread.
   *
   * Once the task completes, also when it failed or was killed, a pre-completion listener calls
   * `stop(false)`, interrupts the thread, unless the task is not interruptible then, and waits
   * until the thread no longer reads the input. `stop` must not block: it makes the thread stop
   * reading soon, for example with a flag that the thread checks between two rows, and by ending
   * what the thread may be blocked on, such as the process that it writes to. If the thread still
   * reads the input after `gracePeriodMs`, the listener calls `stop(true)` once, which stops it
   * by force, for example by killing that process. `stop` is also called for a thread that was
   * not started or that already ended, so that it can release what it stops.
   *
   * The wait has no time bound: giving up would let the completion listeners free what the
   * thread still reads. So a thread blocked in reading the input in a way that ignores
   * interrupts, such as a socket read without a timeout, holds the completion of the task until
   * that read returns, as for the writer of pipelined Python UDFs. If such a task is killed,
   * `spark.task.reaper.enabled` with a positive `spark.task.reaper.killTimeout` bounds the wait
   * by killing the executor, except in local mode. The listener logs a warning every
   * `warnIntervalMs` while it waits, the first one with the stack trace of the thread, and
   * interrupts the thread again, in case the code that it runs consumed the interrupt.
   *
   * @param awaitDone waits until the thread no longer reads the input of the task, for at most
   *                  the given number of nanoseconds, and returns whether it no longer does. By
   *                  default, it waits until the thread ended. A thread that goes on after it
   *                  read the input, for example to wait for the process that it wrote to,
   *                  passes a function that only waits for the reading.
   * @return whether the thread was started. It is not once the task completed: the listener
   *         would then run too late, behind completion listeners.
   */
  def start(
      context: TaskContext,
      thread: Thread,
      gracePeriodMs: Long = DEFAULT_GRACE_PERIOD_MS,
      warnIntervalMs: Long = DEFAULT_WARN_INTERVAL_MS,
      awaitDone: Option[Long => Boolean] = None)(stop: Boolean => Unit): Boolean = {
    if (context == null) {
      thread.start()
      true
    } else {
      val stopper = new Stopper(context, thread, gracePeriodMs, warnIntervalMs,
        awaitDone.getOrElse(awaitExit(thread)), stop)
      // Added before the thread starts, so that it runs before the completion listeners that the
      // thread adds. If the task already completed, the listener runs as a completion listener,
      // right away or behind the ones that another thread is running, and the thread is not
      // started.
      context.addTaskPreCompletionListener(stopper)
      stopper.start()
    }
  }

  /**
   * Ends the process that a thread writes the input of a task to, for the `stop` of [[start]].
   */
  final class ProcessStopper(process: Process) {
    // The processes that the process had started when it was signaled: once it is gone, they are
    // no longer its descendants. Only the thread that stops the process uses it.
    private var descendants: Seq[ProcessHandle] = Nil

    /**
     * Signals the process to end, as `Process.destroy()` does, or with `force`, kills it and the
     * processes that it started: when a shell is gone, the commands that it ran may still hold
     * its input open, which a thread blocked in a write to that input then waits for. Unlike
     * `Process.destroy()` and `Process.destroyForcibly()`, it does not close the streams of the
     * process: closing its input would wait for a thread that is blocked in a write to it, and so
     * for a process that ignores the signal.
     */
    def stop(force: Boolean): Unit = {
      val handle = process.toHandle
      val started = handle.descendants().iterator().asScala.toSeq
      if (force) {
        (handle +: (started ++ descendants)).distinct.foreach(_.destroyForcibly())
      } else {
        descendants = started
        handle.destroy()
      }
    }
  }

  private def awaitExit(thread: Thread)(nanos: Long): Boolean = {
    TimeUnit.NANOSECONDS.timedJoin(thread, nanos)
    !thread.isAlive
  }

  private class Stopper(
      context: TaskContext,
      thread: Thread,
      gracePeriodMs: Long,
      warnIntervalMs: Long,
      awaitDone: Long => Boolean,
      stop: Boolean => Unit) extends TaskCompletionListener {
    private val taskName = s"task ${context.partitionId()}.${context.attemptNumber()} in stage " +
      s"${context.stageId()} (TID ${context.taskAttemptId()})"

    // Protected by `this`: the thread is not started once the listener ran, which finds a thread
    // that is not started done.
    private var started = false
    private var stopped = false

    // The first failure to stop, log or interrupt the thread while the listener waits for it,
    // which is thrown once the wait ends. Only the thread that runs the listener uses it.
    private var failure: Option[Throwable] = None

    def start(): Boolean = synchronized {
      if (!stopped && !context.isCompleted()) {
        thread.start()
        started = true
      }
      started
    }

    /**
     * Stops the thread and waits until it no longer reads the input of the task. Leaves the
     * interrupt status of the calling thread as it was: an interrupt that arrives during the
     * wait, such as the kill of the task, which the task context already records, does not end
     * the wait, and is not left for the completion listeners that run next, whose cleanup it
     * would fail. Neither does a failure to stop, log or interrupt the thread, such as of an
     * appender that does not ignore exceptions: the completion listeners that run next would
     * free what the thread still reads. The first such failure is thrown once the wait ends,
     * except a non-fatal failure of `stop`, which is only logged.
     */
    override def onTaskCompletion(taskContext: TaskContext): Unit = {
      val wasStarted = synchronized {
        stopped = true
        started
      }
      val wasInterrupted = Thread.interrupted()
      try {
        tryStop(force = false)
        if (wasStarted) {
          await()
        }
        failure.foreach(e => throw e)
      } finally {
        Thread.interrupted()
        if (wasInterrupted) {
          Thread.currentThread().interrupt()
        }
      }
    }

    // Runs `f`, whose failure must not end the wait for the thread, and records it instead. An
    // interrupt of the calling thread is not a failure, see onTaskCompletion.
    private def bestEffort(f: => Unit): Unit = {
      try {
        f
      } catch {
        case _: InterruptedException => Thread.interrupted()
        case t: Throwable => failure = failure.orElse(Some(t))
      }
    }

    private def tryStop(force: Boolean): Unit = bestEffort {
      try {
        stop(force)
      } catch {
        case NonFatal(e) =>
          logWarning(log"Failed to stop the thread ${MDC(THREAD_NAME, thread.getName)} of " +
            log"${MDC(TASK_NAME, taskName)}.", e)
      }
    }

    // Not while the task is not interruptible, which the kill of a task respects too: a thread of
    // the task then creates a resource that an interrupt would leak.
    private def interruptThread(): Unit = bestEffort {
      TaskContext.synchronized {
        if (context.interruptible()) {
          thread.interrupt()
        }
      }
    }

    private def await(): Unit = {
      if (!isDone(0)) {
        interruptThread()
        val startNs = System.nanoTime()
        val graceNs = TimeUnit.MILLISECONDS.toNanos(gracePeriodMs)
        val warnIntervalNs = TimeUnit.MILLISECONDS.toNanos(warnIntervalMs)
        var lastWarningNs = startNs
        var forced = false
        var warned = false
        var done = false
        while (!done) {
          val now = System.nanoTime()
          // How long until the thread is stopped by force, and until the next warning.
          val untilForce = graceNs - (now - startNs)
          val untilWarning = warnIntervalNs - (now - lastWarningNs)
          if (!forced && untilForce <= 0) {
            forced = true
            bestEffort(logWarning(stillReading(startNs) + log" Stopping it by force."))
            tryStop(force = true)
            interruptThread()
          } else if (untilWarning <= 0) {
            bestEffort {
              if (warned) {
                logWarning(stillReading(startNs))
              } else {
                val stack = new Exception(s"Stack trace of thread ${thread.getName}")
                stack.setStackTrace(thread.getStackTrace)
                logWarning(stillReading(startNs), stack)
                warned = true
              }
            }
            interruptThread()
            lastWarningNs = System.nanoTime()
          } else {
            done = isDone(if (forced) untilWarning else math.min(untilWarning, untilForce))
          }
        }
      }
    }

    // An interrupt of the calling thread does not end the wait, see onTaskCompletion.
    private def isDone(nanos: Long): Boolean = {
      try {
        awaitDone(nanos)
      } catch {
        case _: InterruptedException =>
          Thread.interrupted()
          false
      }
    }

    private def stillReading(startNs: Long): MessageWithContext = {
      val waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs)
      log"The thread ${MDC(THREAD_NAME, thread.getName)} still reads the input of " +
        log"${MDC(TASK_NAME, taskName)} ${MDC(TOTAL_TIME, waitedMs)} ms after the task " +
        log"completed, which waits for it before its completion listeners run."
    }
  }
}

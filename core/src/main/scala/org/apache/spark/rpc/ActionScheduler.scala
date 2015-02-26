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

package org.apache.spark.rpc

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{SynchronousQueue, TimeUnit, ThreadPoolExecutor}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import io.netty.util.{Timeout, TimerTask, HashedWheelTimer}

import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf}

/**
 * It's very common that executing some actions in other threads to avoid blocking the event loop
 * in a RpcEndpoint. [[ActionScheduler]] is designed for such use cases.
 */
private[spark] trait ActionScheduler {

  /**
   * Run the action in the IO thread pool. The thread name will be `name` when running this action.
   */
  def executeIOAction(name: String)(action: => Unit): Unit

  /**
   * Run the action in the CPU thread pool. The thread name will be `name` when running this action.
   */
  def executeCPUAction(name: String)(action: => Unit): Unit

  /**
   * Run the action after `delay`. The thread name will be `name` when running this action.
   */
  def schedule(name: String, delay: FiniteDuration)(action: => Unit): Cancellable


  /**
   * Run the action every `interval`. The thread name will be `name` when running this action.
   */
  def schedulePeriodically(name: String, interval: FiniteDuration)(action: => Unit): Cancellable = {
    schedulePeriodically(name, interval, interval)(action)
  }

  /**
   * Run the action every `interval`. The thread name will be `name` when running this action.
   */
  def schedulePeriodically(
      name: String, delay: FiniteDuration, interval: FiniteDuration)(action: => Unit): Cancellable
}

private[spark] trait Cancellable {
  /**
   * Cancel the corresponding work. The caller may call this method multiple times and call it in
   * any thread.
   */
  def cancel(): Unit
}

private[rpc] class SettableCancellable extends Cancellable {

  @volatile private var underlying: Cancellable = SettableCancellable.NOP

  @volatile private var isCancelled = false

  // Should be called only once
  def set(c: Cancellable): Unit = {
    underlying = c
    if (isCancelled) {
      underlying.cancel()
    }
  }

  override def cancel(): Unit = {
    isCancelled = true
    underlying.cancel()
  }
}

private[rpc] object SettableCancellable {
  val NOP = new Cancellable {
    override def cancel(): Unit = {}
  }
}

private[spark] class ActionSchedulerImpl(conf: SparkConf) extends ActionScheduler with Logging {

  val maxIOThreadNumber = conf.getInt("spark.rpc.io.maxThreads", 1000)

  private val ioExecutor = new ThreadPoolExecutor(
    0,
    maxIOThreadNumber,
    60L,
    TimeUnit.SECONDS,
    new SynchronousQueue[Runnable](), Utils.namedThreadFactory("rpc-io"))

  private val cpuExecutor = ExecutionContext.fromExecutorService(null, e => {
    e match {
      case NonFatal(e) => logError(e.getMessage, e)
      case e =>
        Thread.getDefaultUncaughtExceptionHandler.uncaughtException(Thread.currentThread, e)
    }
  })

  private val timer = new HashedWheelTimer(Utils.namedThreadFactory("rpc-timer"))

  // Need a name to distinguish between different actions because they use the same thread pool
  override def executeIOAction(name: String)(action: => Unit): Unit = {
    ioExecutor.execute(new Runnable {

      override def run(): Unit = {
        val previousThreadName = Thread.currentThread().getName
        Thread.currentThread().setName(name)
        try {
          action
        } finally {
          Thread.currentThread().setName(previousThreadName)
        }
      }

    })
  }

  // Need a name to distinguish between different actions because they use the same thread pool
  override def executeCPUAction(name: String)(action: => Unit): Unit = {
    cpuExecutor.execute(new Runnable {

      override def run(): Unit = {
        val previousThreadName = Thread.currentThread().getName
        Thread.currentThread().setName(name)
        try {
          action
        } finally {
          Thread.currentThread().setName(previousThreadName)
        }
      }

    })
  }

  def schedule(name: String, delay: FiniteDuration)(action: => Unit): Cancellable = {
    val timeout = timer.newTimeout(new TimerTask {

      override def run(timeout: Timeout): Unit = {
        val previousThreadName = Thread.currentThread().getName
        Thread.currentThread().setName(name)
        try {
          action
        } finally {
          Thread.currentThread().setName(previousThreadName)
        }
      }

    }, delay.toNanos, TimeUnit.NANOSECONDS)
    new Cancellable {
      override def cancel(): Unit = timeout.cancel()
    }
  }

  override def schedulePeriodically(
      name: String, delay: FiniteDuration, interval: FiniteDuration)(action: => Unit):
    Cancellable = {
    val initial = new SettableCancellable
    val cancellable = new AtomicReference[SettableCancellable](initial)
    def actionOnce: Unit = {
      if (cancellable.get != null) {
        action
        val c = cancellable.get
        if (c != null) {
          val s = new SettableCancellable
          if (cancellable.compareAndSet(c, s)) {
            s.set(schedule(name, interval)(actionOnce))
          } else {
            // has been cancelled
            assert(cancellable.get == null)
          }
        }
      }
    }
    initial.set(schedule(name, delay)(actionOnce))
    new Cancellable {
      override def cancel(): Unit = {
        var c = cancellable.get
        while (c != null) {
          if (cancellable.compareAndSet(c, null)) {
            c.cancel()
            return
          } else {
            // Already schedule another action, retry to cancel it
            c = cancellable.get
          }
        }
      }
    }
  }
}

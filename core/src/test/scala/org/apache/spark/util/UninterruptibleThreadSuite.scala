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

import java.nio.channels.spi.AbstractInterruptibleChannel
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.util.Random

import com.google.common.util.concurrent.Uninterruptibles

import org.apache.spark.SparkFunSuite

class UninterruptibleThreadSuite extends SparkFunSuite {

  /* Sleep millis and return true if it's interrupted */
  private def sleep(millis: Long): Boolean = {
    try {
      Thread.sleep(millis)
      false
    } catch {
      case e: InterruptedException =>
        log.error("Thread interrupted during sleep", e)
        true
    }
  }

  /* Await latch and return true if it's interrupted */
  private def await(
      latch: CountDownLatch,
      timeout: Long = 10,
      timeUnit: TimeUnit = TimeUnit.SECONDS): Boolean = {
    try {
      if (!latch.await(timeout, timeUnit)) {
        log.error("timeout while waiting for the latch")
        fail("timeout while waiting for the latch")
      }
      false
    } catch {
      case e: InterruptedException =>
        log.error("Thread interrupted during await", e)
        true
    }
  }

  test("interrupt when runUninterruptibly is running") {
    val enterRunUninterruptibly = new CountDownLatch(1)
    val interruptLatch = new CountDownLatch(1)
    @volatile var hasInterruptedException = false
    @volatile var interruptStatusBeforeExit = false
    val t = new UninterruptibleThread("runUninterruptibly") {
      override def run(): Unit = {
        runUninterruptibly {
          enterRunUninterruptibly.countDown()
          hasInterruptedException = await(interruptLatch)
        }
        interruptStatusBeforeExit = Thread.interrupted()
      }
    }
    t.start()
    assert(!await(enterRunUninterruptibly), "await interrupted")
    t.interrupt()
    interruptLatch.countDown()
    t.join()
    assert(!hasInterruptedException, "runUninterruptibly should not be interrupted")
    assert(interruptStatusBeforeExit, "interrupt flag should be set")
  }

  test("interrupt before runUninterruptibly runs") {
    val interruptLatch = new CountDownLatch(1)
    @volatile var hasInterruptedException = false
    @volatile var interruptStatusBeforeExit = false
    val t = new UninterruptibleThread("runUninterruptibly") {
      override def run(): Unit = {
        assert(Uninterruptibles.awaitUninterruptibly(interruptLatch, 10, TimeUnit.SECONDS))
        assert(isInterrupted, "interrupt flag should be set")
        runUninterruptibly {
          hasInterruptedException = sleep(0)
        }
        interruptStatusBeforeExit = Thread.interrupted()
      }
    }
    t.start()
    t.interrupt()
    interruptLatch.countDown()
    t.join()
    assert(!hasInterruptedException, "runUninterruptibly should not be interrupted")
    assert(interruptStatusBeforeExit, "interrupt flag should be set")
  }

  test("nested runUninterruptibly") {
    val enterRunUninterruptibly = new CountDownLatch(1)
    val interruptLatch = new CountDownLatch(1)
    @volatile var hasInterruptedException = false
    @volatile var interruptStatusBeforeExit = false
    val t = new UninterruptibleThread("runUninterruptibly") {
      override def run(): Unit = {
        runUninterruptibly {
          enterRunUninterruptibly.countDown()
          hasInterruptedException = await(interruptLatch)
          if (!hasInterruptedException) {
            runUninterruptibly {
              hasInterruptedException = sleep(0)
            }
            hasInterruptedException |= sleep(0)
          }
        }
        interruptStatusBeforeExit = Thread.interrupted()
      }
    }
    t.start()
    assert(!await(enterRunUninterruptibly), "await interrupted")
    t.interrupt()
    interruptLatch.countDown()
    assert(!sleep(0), "sleep should not be interrupted")
    t.interrupt()
    assert(!sleep(0), "sleep should not be interrupted")
    t.interrupt()
    t.join()
    assert(!hasInterruptedException, "runUninterruptibly should not be interrupted")
    assert(interruptStatusBeforeExit, "interrupt flag should be set")
  }

  test("no runUninterruptibly") {
    @volatile var hasInterruptedException = false
    @volatile var interruptStatusBeforeExit = false
    val t = new UninterruptibleThread("run") {
      override def run(): Unit = {
        hasInterruptedException = sleep(0)
        interruptStatusBeforeExit = Thread.interrupted()
      }
    }
    t.interrupt()
    t.start()
    t.join()
    assert(hasInterruptedException, "run should be interrupted")
    assert(!interruptStatusBeforeExit, "interrupt flag should not be set")
  }

  test("SPARK-51821 uninterruptibleLock deadlock") {
    val interruptLatch = new CountDownLatch(1)
    val t = new UninterruptibleThread("run") {
      override def run(): Unit = {
        val channel = new AbstractInterruptibleChannel() {
          override def implCloseChannel(): Unit = {
            begin()
            interruptLatch.countDown()
            try {
              Thread.sleep(Long.MaxValue)
            } catch {
              case e: InterruptedException =>
                log.info("sleep interrupted", e)
                Thread.currentThread().interrupt()
            }
          }
        }
        channel.close()
      }
    }
    t.start()
    assert(!await(interruptLatch), "await interrupted")
    t.interrupt()
    t.join()
  }

  test("stress test") {
    for (i <- 0 until 20) {
      stressTest(i)
    }
  }

  def stressTest(i: Int): Unit = {
    @volatile var hasInterruptedException = false
    val t = new UninterruptibleThread(s"stress test $i") {
      override def run(): Unit = {
        for (i <- 0 until 100 if !hasInterruptedException) {
          try {
            runUninterruptibly {
              hasInterruptedException = sleep(Random.nextInt(10))
              runUninterruptibly {
                hasInterruptedException |= sleep(Random.nextInt(10))
              }
              hasInterruptedException |= sleep(Random.nextInt(10))
            }
            Uninterruptibles.sleepUninterruptibly(Random.nextInt(10), TimeUnit.MILLISECONDS)
            // 50% chance to clear the interrupted status
            if (Random.nextBoolean()) {
              Thread.interrupted()
            }
          } catch {
            case _: InterruptedException => hasInterruptedException = true
          }
        }
      }
    }
    val threads = Array.fill(Runtime.getRuntime.availableProcessors)(
      new Thread() {
        override def run(): Unit = {
          for (i <- 0 until 400 if !hasInterruptedException) {
            Thread.sleep(Random.nextInt(10))
            t.interrupt()
          }
        }
      })
    t.start()
    threads.foreach(t => t.start())
    threads.foreach(t => t.join())
    t.join()
    assert(!hasInterruptedException, "runUninterruptibly should not be interrupted")
  }
}

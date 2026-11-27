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
package org.apache.spark.sql.connect.service

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, ThreadLocalRandom, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.util.Random

import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkFunSuite

/** Unit tests for [[ExecutionSlots]]. */
class ExecutionSlotsSuite extends SparkFunSuite with Eventually {

  private class Recorder {
    val permit = new AtomicReference[ExecutionPermit]()
    val timedOut = new CountDownLatch(1)
    val granted = new CountDownLatch(1)
    def onGranted(p: ExecutionPermit): Unit = {
      assert(permit.compareAndSet(null, p), "Granted twice")
      granted.countDown()
    }
    def onTimeout(): Unit = timedOut.countDown()
    def isGranted: Boolean = permit.get() != null
  }

  private def request(
      slots: ExecutionSlots,
      timeoutMs: Long = 0): (ExecutionSlotRequest, Recorder) = {
    val r = new Recorder
    (slots.request(timeoutMs, r.onGranted, () => r.onTimeout()), r)
  }

  private def withSlots(limit: Int)(f: ExecutionSlots => Unit): Unit = {
    val slots = new ExecutionSlots(limit)
    try f(slots)
    finally slots.stop()
  }

  test("slots are granted synchronously while available") {
    withSlots(2) { slots =>
      val (req1, r1) = request(slots)
      val (_, r2) = request(slots)
      assert(r1.isGranted && r2.isGranted)
      assert(!req1.isWaiting)
      assert(slots.availableSlots == 0)
      r1.permit.get().release()
      r1.permit.get().release()
      assert(slots.availableSlots == 1)
      r2.permit.get().release()
      assert(slots.availableSlots == 2)
    }
  }

  test("queued requests are granted in FIFO order on release") {
    withSlots(1) { slots =>
      val (_, first) = request(slots)
      val (reqA, a) = request(slots)
      val (reqB, b) = request(slots)
      assert(reqA.isWaiting && reqB.isWaiting)
      assert(slots.queueLength == 2)

      first.permit.get().release()
      assert(a.isGranted && !b.isGranted)
      assert(!reqA.isWaiting && reqB.isWaiting)

      a.permit.get().release()
      assert(b.isGranted)
      b.permit.get().release()
      assert(slots.availableSlots == 1)
      assert(slots.queueLength == 0)
    }
  }

  test("a new request does not overtake queued requests") {
    withSlots(1) { slots =>
      val (_, first) = request(slots)
      val (_, a) = request(slots)
      first.permit.get().release()
      assert(a.isGranted)
      val (reqB, b) = request(slots)
      assert(reqB.isWaiting && !b.isGranted)
      a.permit.get().release()
      assert(b.isGranted)
      b.permit.get().release()
    }
  }

  test("an abandoned request leaves the queue and is never granted") {
    withSlots(1) { slots =>
      val (_, first) = request(slots)
      val (reqA, a) = request(slots)
      val (_, b) = request(slots)
      assert(reqA.abandon())
      assert(!reqA.abandon())
      assert(slots.queueLength == 1)

      first.permit.get().release()
      assert(!a.isGranted)
      assert(b.isGranted)
      b.permit.get().release()
      assert(slots.availableSlots == 1)
    }
  }

  test("abandoning a granted request is a no-op") {
    withSlots(1) { slots =>
      val (req, r) = request(slots)
      assert(r.isGranted)
      assert(!req.abandon())
      assert(slots.availableSlots == 0)
      r.permit.get().release()
      assert(slots.availableSlots == 1)
    }
  }

  test("a request that times out leaves the queue and does not consume a slot") {
    withSlots(1) { slots =>
      val (_, first) = request(slots)
      val (req, r) = request(slots, timeoutMs = 50)
      assert(r.timedOut.await(10, TimeUnit.SECONDS))
      assert(!req.isWaiting)
      assert(slots.queueLength == 0)
      first.permit.get().release()
      assert(!r.isGranted)
      assert(slots.availableSlots == 1)
    }
  }

  test("a request granted before its timeout does not time out") {
    withSlots(1) { slots =>
      val (_, first) = request(slots)
      val (_, r) = request(slots, timeoutMs = 200)
      first.permit.get().release()
      assert(r.isGranted)
      assert(!r.timedOut.await(400, TimeUnit.MILLISECONDS))
      r.permit.get().release()
    }
  }

  test("a failing grant callback returns the slot") {
    withSlots(1) { slots =>
      val (_, first) = request(slots)
      slots.request(0, _ => throw new RuntimeException("boom"), () => ())
      val (_, b) = request(slots)
      first.permit.get().release()
      // The slot went through the failing request, back to the pool, and then to b.
      assert(b.isGranted)
      b.permit.get().release()
      assert(slots.availableSlots == 1)
    }
  }

  test("slots are not leaked when releases race with abandons and timeouts") {
    val limit = 3
    withSlots(limit) { slots =>
      val held = new ConcurrentLinkedQueue[ExecutionPermit]()
      val granted = new AtomicInteger(0)
      val threads = (0 until 16).map { i =>
        val t = new Thread(() => {
          val random = new Random(i)
          (0 until 300).foreach { _ =>
            val req = slots.request(
              random.nextInt(3),
              p => {
                granted.incrementAndGet()
                if (ThreadLocalRandom.current().nextBoolean()) p.release() else held.add(p)
              },
              () => ())
            if (random.nextInt(3) == 0) req.abandon()
            val p = held.poll()
            if (p != null) p.release()
          }
        })
        t.setDaemon(true)
        t.start()
        t
      }
      threads.foreach { t =>
        t.join(60000L)
        assert(!t.isAlive)
      }
      // Drain: requests may still be queued (no timeout) and get granted as permits are released.
      eventually(timeout(10.seconds), interval(10.millis)) {
        var p = held.poll()
        while (p != null) {
          p.release()
          p = held.poll()
        }
        assert(slots.queueLength == 0)
        assert(slots.availableSlots == limit)
      }
      assert(granted.get() > 0)
    }
  }
}

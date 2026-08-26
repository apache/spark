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

package org.apache.spark

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

/**
 * Unit tests for the TTL sweep itself. `BlockTTLIntegrationSuite` covers the wiring end to end
 * against a real cluster; this drives `BlockTTLCleaner` directly with a plain map, veto and reap,
 * so the parts that are awkward to provoke through a SparkContext -- a vetoed id, an atime that
 * moves mid-sweep, a reap that throws -- are cheap to pin down.
 */
class BlockTTLCleanerSuite extends SparkFunSuite with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  // Short, so an expiry happens promptly; the sweep's own floor is max(ttl/10, 100)ms.
  private val ttlMillis = 200L

  private def withCleaner(
      accessTimes: ConcurrentHashMap[Int, Long],
      shouldReap: Int => Boolean = _ => true)(
      reap: Int => Unit)(body: => Unit): Unit = {
    val cleaner = new BlockTTLCleaner("test", ttlMillis, accessTimes, shouldReap, reap)
    cleaner.start()
    try {
      body
    } finally {
      cleaner.stop()
    }
  }

  /** An atime far enough in the past to be expired on the first sweep. */
  private def expiredAtime: Long = System.currentTimeMillis() - (ttlMillis * 10)

  test("an idle id is reaped and left untracked") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(7, expiredAtime)
    val reaped = new java.util.concurrent.LinkedBlockingQueue[Int]
    withCleaner(accessTimes)(reaped.add(_)) {
      assert(reaped.poll(10, TimeUnit.SECONDS) === 7)
      eventually {
        assert(accessTimes.isEmpty, "a reaped id should not stay tracked")
      }
    }
  }

  test("a vetoed id is never reaped but stays tracked for a later sweep") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(1, expiredAtime)
    accessTimes.put(2, expiredAtime)
    val reaped = new java.util.concurrent.LinkedBlockingQueue[Int]
    // 1 is vetoed (e.g. a locally-checkpointed RDD, or a shuffle with no output yet), 2 is not.
    withCleaner(accessTimes, shouldReap = _ != 1)(reaped.add(_)) {
      assert(reaped.poll(10, TimeUnit.SECONDS) === 2)
      // Several sweeps' worth, to show the veto holds rather than merely being slow.
      Thread.sleep(ttlMillis * 5)
      assert(!reaped.contains(1), "a vetoed id must not be reaped")
      assert(accessTimes.containsKey(1),
        "a vetoed id must stay tracked, or it could never be reaped once the veto lifts")
    }
  }

  test("an id that comes back into use between the scan and the reap is not reaped") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(9, expiredAtime)
    val reaped = new AtomicInteger(0)
    // shouldReap is the sweep's last look before it commits, so refreshing the atime here
    // reproduces an access landing in exactly that window. The compare-and-remove must then fail.
    withCleaner(accessTimes, shouldReap = { _ =>
      accessTimes.put(9, System.currentTimeMillis())
      true
    })(_ => reaped.incrementAndGet()) {
      Thread.sleep(ttlMillis * 5)
      assert(reaped.get() === 0,
        "the atime moved after the scan read it, so the compare-and-remove should have failed")
      assert(accessTimes.containsKey(9))
    }
  }

  test("a failed reap leaves the id tracked so a later sweep retries it") {
    // Regression test: dropping the atime before the reap meant a reap that threw part-way (a
    // blocking shuffle removal timing out, say) left the id untracked forever, so whatever it still
    // held on disk leaked for the life of the driver.
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(4, expiredAtime)
    val attempts = new AtomicInteger(0)
    val secondAttempt = new CountDownLatch(2)
    withCleaner(accessTimes)({ _ =>
      attempts.incrementAndGet()
      secondAttempt.countDown()
      throw new IllegalStateException("boom")
    }) {
      // Reaching a second attempt at all is the real assertion: the sweep removes the atime before
      // it reaps, so a retry can only happen if the failed reap put it back.
      assert(secondAttempt.await(10, TimeUnit.SECONDS),
        "a reap that threw should be retried on a later sweep")
      assert(attempts.get() >= 2)
      // eventually, because the latch is counted down on entry to the reap -- the id is untracked
      // for the length of the attempt and only restored once it has failed.
      eventually {
        assert(accessTimes.containsKey(4), "a failed reap must leave the id tracked")
      }
    }
  }

  test("a live id is left alone") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(3, System.currentTimeMillis())
    accessTimes.put(5, expiredAtime)
    val reaped = new java.util.concurrent.LinkedBlockingQueue[Int]
    withCleaner(accessTimes)(reaped.add(_)) {
      assert(reaped.poll(10, TimeUnit.SECONDS) === 5)
      assert(accessTimes.containsKey(3), "an id inside its TTL must not be reaped")
      assert(!reaped.contains(3))
    }
  }

  test("stop() ends the sweep") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    val cleaner = new BlockTTLCleaner("test", ttlMillis, accessTimes, _ => true, _ => ())
    cleaner.start()
    // Long enough to be mid-sleep rather than mid-scan.
    Thread.sleep(ttlMillis)
    cleaner.stop()
    accessTimes.put(1, expiredAtime)
    Thread.sleep(ttlMillis * 5)
    assert(accessTimes.asScala.keySet === Set(1), "a stopped cleaner must not reap anything")
  }
}

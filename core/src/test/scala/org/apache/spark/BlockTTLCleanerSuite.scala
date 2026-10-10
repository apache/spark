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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.internal.LogKeys.RDD_ID

/**
 * Unit tests for the TTL sweep itself. `BlockTTLIntegrationSuite` covers the wiring end to end
 * against a real cluster; this drives `BlockTTLCleaner` directly with a plain map, veto and reap,
 * so the parts that are awkward to provoke through a SparkContext -- a vetoed id, an access time
 * that moves mid-sweep, a reap that throws -- are cheap to pin down.
 *
 * Most tests call `sweep()` rather than starting the thread, so there is nothing to wait for.
 */
class BlockTTLCleanerSuite extends SparkFunSuite with Eventually {

  private val ttlMillis = 10000L

  private def cleaner(
      accessTimes: ConcurrentHashMap[Int, Long],
      shouldReap: Int => Boolean = _ => true)(
      reap: Int => Unit): BlockTTLCleaner =
    new BlockTTLCleaner("test", RDD_ID, ttlMillis, accessTimes, shouldReap, reap)

  /** An access time far enough in the past that a sweep sees it as expired. */
  private def expired: Long = System.currentTimeMillis() - (ttlMillis * 10)

  test("an idle id is reaped and left untracked") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(7, expired)
    val reaped = ArrayBuffer.empty[Int]
    cleaner(accessTimes)(reaped += _).sweep()
    assert(reaped.toSeq === Seq(7))
    assert(accessTimes.isEmpty, "a reaped id should not stay tracked")
  }

  test("a live id is left alone") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(3, System.currentTimeMillis())
    accessTimes.put(5, expired)
    val reaped = ArrayBuffer.empty[Int]
    cleaner(accessTimes)(reaped += _).sweep()
    assert(reaped.toSeq === Seq(5))
    assert(accessTimes.containsKey(3), "an id inside its TTL must not be reaped")
  }

  test("a vetoed id is never reaped but stays tracked for a later sweep") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(1, expired)
    accessTimes.put(2, expired)
    val reaped = ArrayBuffer.empty[Int]
    // 1 is vetoed (a locally-checkpointed RDD, or a shuffle with no output yet), 2 is not.
    val c = cleaner(accessTimes, shouldReap = _ != 1)(reaped += _)
    c.sweep()
    c.sweep()
    assert(reaped.toSeq === Seq(2), "a vetoed id must not be reaped, on this sweep or a later one")
    assert(accessTimes.containsKey(1),
      "a vetoed id must stay tracked, or it could never be reaped once the veto lifts")
  }

  test("an id that comes back into use between the scan and the reap is not reaped") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(9, expired)
    val reaped = new AtomicInteger(0)
    // shouldReap is the sweep's last look before it commits, so refreshing the access time here
    // reproduces a use landing in exactly that window. The compare-and-remove must then fail.
    cleaner(accessTimes, shouldReap = { _ =>
      accessTimes.put(9, System.currentTimeMillis())
      true
    })(_ => reaped.incrementAndGet()).sweep()
    assert(reaped.get() === 0,
      "the access time moved after the scan read it, so the compare-and-remove should have failed")
    assert(accessTimes.containsKey(9))
  }

  test("a failed reap leaves the id tracked so a later sweep retries it") {
    // Regression test: dropping the access time before the reap meant a reap that threw part-way (a
    // blocking shuffle removal timing out, say) left the id untracked forever, so whatever it still
    // held on disk leaked for the life of the driver.
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(4, expired)
    val attempts = new AtomicInteger(0)
    val c = cleaner(accessTimes) { _ =>
      attempts.incrementAndGet()
      throw new IllegalStateException("boom")
    }
    c.sweep()
    assert(attempts.get() === 1)
    assert(accessTimes.containsKey(4), "a failed reap must leave the id tracked")
    // The retry is one TTL out, not the very next sweep, so a permanently failing reap costs one
    // warning per TTL rather than one per sweep.
    c.sweep()
    assert(attempts.get() === 1, "the restored access time should be fresh, not still expired")
    accessTimes.put(4, expired)
    c.sweep()
    assert(attempts.get() === 2, "once expired again, the id should be retried")
  }

  test("the sweep asks to be woken before the oldest live entry expires") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    // Half a TTL old, so it expires in roughly half a TTL from now.
    accessTimes.put(1, System.currentTimeMillis() - (ttlMillis / 2))
    val wait = cleaner(accessTimes)(_ => ()).sweep()
    assert(wait <= ttlMillis / 2 + 1000, s"should wake near the entry's expiry, not in $wait ms")
    assert(wait >= ttlMillis / 10, "but never sooner than the floor, or the sweep would spin")
  }

  test("an empty map waits a whole TTL rather than spinning") {
    val wait = cleaner(new ConcurrentHashMap[Int, Long])(_ => ()).sweep()
    assert(wait >= ttlMillis - 1000 && wait <= ttlMillis)
  }

  test("start and stop run and end the sweep thread") {
    val accessTimes = new ConcurrentHashMap[Int, Long]
    accessTimes.put(1, expired)
    val reaped = new AtomicInteger(0)
    val c = cleaner(accessTimes)(_ => reaped.incrementAndGet())
    c.start()
    // The first sweep happens immediately, so this needs no TTL-length wait.
    eventually(timeout(Span(10, Seconds))) {
      assert(reaped.get() === 1)
    }
    c.stop()
    accessTimes.put(2, expired)
    // Well over the floor (ttl/10 = 1s), so a still-running sweep would have taken this.
    TimeUnit.SECONDS.sleep(3)
    assert(accessTimes.containsKey(2), "a stopped cleaner must not reap anything")
  }
}

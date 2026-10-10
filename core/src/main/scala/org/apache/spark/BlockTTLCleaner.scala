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

import scala.util.control.NonFatal

import org.apache.spark.internal.{Logging, LogKey}
import org.apache.spark.internal.LogKeys.{COMPONENT, TIMEOUT}
import org.apache.spark.util.ThreadUtils

/**
 * Periodic TTL sweep over an id -> last-access-time map, on a daemon thread of its own: reaps every
 * id idle for longer than `ttlMillis`, then sleeps until the next one can expire. Shared by the
 * RDD-cache TTL cleaner (`BlockManagerMasterEndpoint`) and the shuffle TTL cleaner
 * (`MapOutputTrackerMaster`), which differ only in the map, the `shouldReap` veto and the `reap`
 * action.
 *
 * `accessTimes` must be concurrent: this thread iterates and removes from it while others put.
 * `shouldReap` returning false leaves the id tracked for the next pass (e.g. a shuffle that has
 * not produced output yet has nothing to reclaim).
 */
private[spark] class BlockTTLCleaner(
    name: String,
    idKey: LogKey,
    ttlMillis: Long,
    accessTimes: ConcurrentHashMap[Int, Long],
    shouldReap: Int => Boolean,
    reap: Int => Unit) extends Runnable with Logging {

  import BlockTTLCleaner.StopTimeoutSeconds

  private val pool = ThreadUtils.newDaemonSingleThreadExecutor(s"$name-ttl-cleaner")

  // Not just the interrupt from shutdownNow: a reap can swallow the InterruptedException on its way
  // out (ContextCleaner.doCleanupRDD catches Exception), which also clears the interrupt flag, and
  // then nothing would ever stop the sweep.
  @volatile private var stopped = false

  def start(): Unit = pool.execute(this)

  def stop(): Unit = {
    stopped = true
    pool.shutdownNow()
    // Wait for a reap already in flight, briefly. SparkContext.stop stops these cleaners before the
    // ContextCleaner, the listener bus and the shuffle driver components precisely so a reap cannot
    // use them half torn down; without joining, that ordering only narrows the window instead of
    // closing it. Bounded because a reap can be blocked on an RPC to a dead executor: shutdownNow
    // has already interrupted it, and a daemon thread outliving this call is better than a
    // shutdown that hangs.
    if (!pool.awaitTermination(StopTimeoutSeconds, TimeUnit.SECONDS)) {
      logWarning(log"The ${MDC(COMPONENT, name)} TTL cleaner did not stop within " +
        log"${MDC(TIMEOUT, StopTimeoutSeconds)}s; continuing shutdown with a reap still in flight")
    }
  }

  /**
   * One pass over `accessTimes`: reap whatever has expired, and return how long to wait before the
   * next pass. Separate from `run` so a test can drive a single sweep rather than wait out a TTL --
   * the configured minimum is 10 minutes, so waiting is not an option for a test.
   */
  private[spark] def sweep(): Long = {
    val now = System.currentTimeMillis()
    // Oldest live atime, so we can sleep until the next possible expiry.
    var oldestLive = now
    val expired = List.newBuilder[(Int, Long)]
    accessTimes.forEach { (id, atime) =>
      if (atime >= now - ttlMillis) oldestLive = math.min(oldestLive, atime)
      else expired += ((id, atime))
    }
    // Reap outside the iteration: a reap blocks on an RPC, and comparing the rest of the map
    // against a `now` that went stale meanwhile would defer them a whole sweep.
    expired.result().foreach { case (id, atime) =>
      // remove(k, v) fails if the atime moved since we read it: the id is back in use.
      if (shouldReap(id) && accessTimes.remove(id, atime)) {
        var reaped = false
        try {
          reap(id)
          reaped = true
        } catch {
          // Warn, not debug: reclaiming space is this loop's whole job.
          case NonFatal(e) =>
            logWarning(log"Error reaping ${MDC(idKey, id)} in the " +
              log"${MDC(COMPONENT, name)} TTL cleaner", e)
        } finally {
          if (!reaped) {
            // Start the id's clock again rather than leaving it untracked. A reap can fail
            // part-way -- a blocking shuffle removal times out waiting on an executor, say --
            // and dropping the atime here would mean nothing ever revisits the id, so whatever
            // it still holds on disk would leak for the life of the driver. A fresh timestamp
            // retries one TTL from now instead of every sweep, so a reap that always fails
            // costs one warning per TTL. putIfAbsent, because the id may have come back into
            // use while the reap ran, and that atime is the more recent truth.
            accessTimes.putIfAbsent(id, System.currentTimeMillis())
          }
        }
      }
    }
    // Wait until the oldest live entry can expire, but never spin: with a live entry always near
    // expiry the floor is what stops this from rescanning the whole map continuously.
    val floor = math.max(ttlMillis / 10, 100)
    math.max(oldestLive + ttlMillis - System.currentTimeMillis(), floor)
  }

  override def run(): Unit = {
    try {
      while (!stopped && !Thread.currentThread().isInterrupted) {
        val sleepMillis = sweep()
        // Re-check `stopped`: a reap that swallowed the interrupt cleared the flag too, and
        // sleeping out a whole TTL would keep the stopped SparkContext alive via the reap closure.
        if (!stopped) {
          Thread.sleep(sleepMillis)
        }
      }
    } catch {
      case _: InterruptedException => // Shutdown; fall through and exit.
      // Nothing restarts this thread, so say so rather than stopping reaping in silence.
      case NonFatal(e) =>
        logError(log"The ${MDC(COMPONENT, name)} TTL cleaner died; nothing more will be reaped", e)
    }
    logInfo(log"${MDC(COMPONENT, name)} TTL cleaner exiting.")
  }
}

private[spark] object BlockTTLCleaner {
  // How long stop() waits for an in-flight reap before giving up on it.
  val StopTimeoutSeconds = 2L
}

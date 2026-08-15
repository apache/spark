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

import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
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
private[spark] class BlockTtlCleaner(
    name: String,
    ttlMillis: Long,
    accessTimes: ConcurrentHashMap[Int, Long],
    shouldReap: Int => Boolean,
    reap: Int => Unit) extends Runnable with Logging {

  private val pool = ThreadUtils.newDaemonSingleThreadExecutor(s"$name-ttl-cleaner")

  // Not just the interrupt from shutdownNow: a reap can swallow the InterruptedException on its way
  // out (ContextCleaner.doCleanupRDD catches Exception), which also clears the interrupt flag, and
  // then nothing would ever stop the sweep.
  @volatile private var stopped = false

  def start(): Unit = pool.execute(this)

  def stop(): Unit = {
    stopped = true
    pool.shutdownNow()
  }

  override def run(): Unit = {
    try {
      while (!stopped && !Thread.currentThread().isInterrupted) {
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
          try {
            // remove(k, v) fails if the atime moved since we read it: the id is back in use.
            if (shouldReap(id) && accessTimes.remove(id, atime)) {
              reap(id)
            }
          } catch {
            // Warn, not debug: reclaiming space is this loop's whole job, and the id is already
            // untracked so the reap is never retried.
            case NonFatal(e) => logWarning(s"Error reaping $id in the $name TTL cleaner", e)
          }
        }
        // Sleep until the oldest live entry can expire, but never spin: with a live entry always
        // near expiry the floor is what stops this from rescanning the whole map continuously.
        val floor = math.max(ttlMillis / 10, 100)
        Thread.sleep(math.max(oldestLive + ttlMillis - System.currentTimeMillis(), floor))
      }
    } catch {
      case _: InterruptedException => // Shutdown; fall through and exit.
      // Nothing restarts this thread, so say so rather than stopping reaping in silence.
      case NonFatal(e) => logError(s"The $name TTL cleaner died; nothing more will be reaped", e)
    }
    logInfo(s"$name TTL cleaner exiting.")
  }
}

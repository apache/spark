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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * Shared periodic TTL sweep for a block/shuffle access-time map. Finds ids whose recorded access
 * time is older than `ttlMillis`, reaps them, and sleeps until the next possible expiry. Used by
 * both the RDD-cache TTL cleaner (in `BlockManagerMasterEndpoint`) and the shuffle TTL cleaner (in
 * `MapOutputTrackerMaster`); they differ only in the map, the `shouldReap` gate, and the `reap`
 * action, so the loop lives here once.
 *
 * The runnable loops until interrupted (its owner interrupts it via `shutdownNow` on stop).
 *
 * @param name        label used in log messages (e.g. "RDD" / "shuffle")
 * @param ttlMillis   the TTL; only constructed when the corresponding config is set
 * @param accessTimes id -> last-access-time (millis). Must be a `ConcurrentHashMap`: this sweep
 *                    iterates it (weakly consistent) while other threads `put` to it, and plain
 *                    HashMap structural mutation from multiple threads can corrupt the map.
 * @param shouldReap  gate checked before removal; return false to leave an id tracked this pass
 *                    (e.g. a shuffle that has not produced output yet has nothing to reclaim, and
 *                    removing its state would break a later registration).
 * @param reap        performs the actual removal for an id whose atime was still stale.
 */
private[spark] class BlockTtlCleaner(
    name: String,
    ttlMillis: Long,
    accessTimes: ConcurrentHashMap[Int, Long],
    shouldReap: Int => Boolean,
    reap: Int => Unit) extends Runnable with Logging {

  override def run(): Unit = {
    try {
      while (!Thread.currentThread().isInterrupted) {
        val maxAge = System.currentTimeMillis() - ttlMillis
        // Track the oldest still-live atime so we can sleep until the next possible expiry.
        var oldest = System.currentTimeMillis()
        val toBeRemoved = accessTimes.asScala.toList.flatMap { case (id, atime) =>
          if (atime < maxAge) {
            Some((id, atime))
          } else {
            if (atime < oldest) {
              oldest = atime
            }
            None
          }
        }
        toBeRemoved.foreach { case (id, atime) =>
          try {
            // `shouldReap` is checked before the removal so a skipped id stays tracked (its atime
            // is unchanged). `remove(key, value)` only succeeds if the atime is unchanged since the
            // snapshot, so a concurrent access in the window leaves the entry: it is back in use.
            if (shouldReap(id) && accessTimes.remove(id, atime)) {
              reap(id)
            }
          } catch {
            // Warn, not debug: this loop's whole value is reclaiming space, so a reap that always
            // fails (e.g. an unwired remover, or an RPC failure) must not be invisible at the
            // default log level. The id has already been dropped from `accessTimes`, so it is not
            // retried -- a persistent failure means that id is simply never reclaimed.
            case NonFatal(e) =>
              logWarning(s"Error reaping $id in the $name TTL cleaner", e)
          }
        }
        // Wait until the next possible element to be removed.
        val delay = math.max((oldest + ttlMillis) - System.currentTimeMillis(), 100)
        Thread.sleep(delay)
      }
      logInfo(s"$name TTL cleaner thread interrupted, exiting.")
    } catch {
      case _: InterruptedException =>
        logInfo(s"$name TTL cleaner thread interrupted, exiting.")
    }
  }
}

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
 * Periodic TTL sweep over an id -> last-access-time map: reaps every id idle for longer than
 * `ttlMillis`, then sleeps until the next one can expire. Shared by the RDD-cache TTL cleaner
 * (`BlockManagerMasterEndpoint`) and the shuffle TTL cleaner (`MapOutputTrackerMaster`), which
 * differ only in the map, the `shouldReap` veto and the `reap` action. Runs until its owner
 * interrupts it via `shutdownNow`.
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

  override def run(): Unit = {
    try {
      while (!Thread.currentThread().isInterrupted) {
        val now = System.currentTimeMillis()
        // Oldest live atime, so we can sleep until the next possible expiry.
        var oldestLive = now
        accessTimes.asScala.foreach { case (id, atime) =>
          if (atime >= now - ttlMillis) {
            oldestLive = math.min(oldestLive, atime)
          } else {
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
        }
        Thread.sleep(math.max(oldestLive + ttlMillis - System.currentTimeMillis(), 100))
      }
    } catch {
      case _: InterruptedException => // Shutdown; fall through and exit.
    }
    logInfo(s"$name TTL cleaner exiting.")
  }
}

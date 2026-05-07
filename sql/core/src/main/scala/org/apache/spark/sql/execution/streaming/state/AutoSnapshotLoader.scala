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

package org.apache.spark.sql.execution.streaming.state

import scala.collection.immutable.ArraySeq
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{Path, PathFilter}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{NUM_RETRIES, NUM_RETRY, UUID, VERSION_NUM}
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager

/**
 * [[AutoSnapshotLoader]] is used to handle loading state store snapshot version from the
 * checkpoint directory. It supports Auto snapshot repair, which will automatically handle
 * corrupt snapshots and skip them, by using another snapshot version before the corrupt one.
 * If no snapshot exists before the corrupt one, then it will use the 0 version snapshot
 * (represents initial/empty snapshot).
 *
 * @param autoSnapshotRepairEnabled If true, it will handle corrupt snapshot
 * @param numFailuresBeforeActivating If auto snapshot repair is enabled,
 *                                    number of failures before activating it
 * @param maxChangeFileReplay If auto snapshot repair is enabled, maximum difference between
 *                            the requested snapshot version and the selected snapshot version
 * @param loggingId To append to log messages
 * */
abstract class AutoSnapshotLoader(
    autoSnapshotRepairEnabled: Boolean,
    numFailuresBeforeActivating: Int,
    maxChangeFileReplay: Int,
    loggingId: String = "") extends Logging {

  override protected def logName: String = s"${super.logName} $loggingId"

  /** Called before loading a snapshot from the checkpoint directory */
  protected def beforeLoad(): Unit

  /**
   * Attempt to load the specified snapshot version from the checkpoint directory.
   * Should throw an exception if the snapshot is corrupt.
   * @param snapshotVersion The snapshot version to load
   * @param uniqueId The unique checkpoint ID for V2, or None for V1
   * @note Must support loading version 0
   * */
  protected def loadSnapshotFromCheckpoint(
      snapshotVersion: Long, uniqueId: Option[String]): Unit

  /** Called when load fails, to do any necessary cleanup/variable reset */
  protected def onLoadSnapshotFromCheckpointFailure(): Unit

  /**
   * Get a list of eligible snapshot (version, uniqueId) pairs in the checkpoint directory
   * that can be loaded. For V1, uniqueId is None.
   */
  protected def getEligibleSnapshots(
      versionToLoad: Long): Seq[(Long, Option[String])]

  /**
   * Returns additional eligible snapshots discovered during auto-repair.
   * Called once when the first snapshot load fails and auto-repair begins.
   *
   * V2 overrides this to enrich a sparse lineage (via getFullLineage) and return
   * newly-discovered snapshots. The results are merged with the initial eligible
   * snapshots (duplicates are removed by version).
   *
   * @param versionToLoad The version being loaded
   * @return Additional eligible snapshot (version, uniqueId) pairs for repair
   */
  protected def getAdditionalEligibleSnapshots(
      versionToLoad: Long): Seq[(Long, Option[String])] = Seq.empty

  /**
   * Load the latest snapshot for the specified version from the checkpoint directory.
   * If Auto snapshot repair is enabled, the snapshot version loaded may be lower than
   * the latest snapshot version, if the latest is corrupt.
   *
   * @param versionToLoad The version to load latest snapshot for
   * @return The actual loaded snapshot version and if it was due to auto repair
   * */
  def loadSnapshot(versionToLoad: Long): (Long, Boolean) = {
    val eligibleSnapshots =
      (getEligibleSnapshots(versionToLoad) :+ (0L, Option.empty[String]))
      .distinctBy(_._1) // Ensure no duplicate version numbers
      .sortBy(_._1)(Ordering[Long].reverse)

    // Start with the latest snapshot
    val firstEligibleSnapshot = eligibleSnapshots.head

    // no retry if auto snapshot repair is not enabled
    val maxNumFailures = if (autoSnapshotRepairEnabled) numFailuresBeforeActivating else 1
    var numFailuresForFirstSnapshot = 0
    var lastException: Throwable = null
    var loadedSnapshot: Option[Long] = None
    while (loadedSnapshot.isEmpty && numFailuresForFirstSnapshot < maxNumFailures) {
      beforeLoad() // if this fails, then we should fail
      try {
        // try to load the first eligible snapshot
        loadSnapshotFromCheckpoint(firstEligibleSnapshot._1, firstEligibleSnapshot._2)
        loadedSnapshot = Some(firstEligibleSnapshot._1)
      } catch {
        // Swallow only if auto snapshot repair is enabled
        // If auto snapshot repair is not enabled, we should fail immediately
        case NonFatal(e) if autoSnapshotRepairEnabled =>
          onLoadSnapshotFromCheckpointFailure()
          numFailuresForFirstSnapshot += 1
          logError(log"Failed to load snapshot version " +
            log"${MDC(VERSION_NUM, firstEligibleSnapshot._1)} " +
            log"(uniqueId: ${MDC(UUID, firstEligibleSnapshot._2.getOrElse(""))}), " +
            log"attempt ${MDC(NUM_RETRY, numFailuresForFirstSnapshot)} out of " +
            log"${MDC(NUM_RETRIES, maxNumFailures)} attempts", e)
          lastException = e
        case e: Throwable =>
          onLoadSnapshotFromCheckpointFailure()
          throw e
      }
    }

    var autoRepairCompleted = false
    if (loadedSnapshot.isEmpty) {
      // we would only get here if auto snapshot repair is enabled
      assert(autoSnapshotRepairEnabled)

      // Merge initial eligible snapshots with any additional ones discovered during
      // repair (e.g., V2 enriches a sparse lineage to discover more snapshots).
      // Deduplicate by version and filter out the first snapshot we already tried.
      val remainingEligibleSnapshots =
        (eligibleSnapshots ++ getAdditionalEligibleSnapshots(versionToLoad)
          :+ (0L, Option.empty[String]))
        .distinctBy(_._1)
        .sortBy(_._1)(Ordering[Long].reverse)
        .filter(_._1 != firstEligibleSnapshot._1)

      // select remaining snapshots that are within the maxChangeFileReplay limit
      val selectedRemainingSnapshots = remainingEligibleSnapshots.filter(
        s => versionToLoad - s._1 <= maxChangeFileReplay)

      logInfo(log"Attempting to auto repair snapshot by skipping " +
        log"snapshot version ${MDC(VERSION_NUM, firstEligibleSnapshot._1)} " +
        log"and trying to load with one of the selected snapshots " +
        log"${MDC(VERSION_NUM, selectedRemainingSnapshots.map(_._1))}, " +
        log"out of eligible snapshots " +
        log"${MDC(VERSION_NUM, remainingEligibleSnapshots.map(_._1))}. " +
        log"maxChangeFileReplay: ${MDC(VERSION_NUM, maxChangeFileReplay)}")

      // Now try to load using any of the selected snapshots,
      // remember they are sorted in descending order
      for (snapshot <- selectedRemainingSnapshots if loadedSnapshot.isEmpty) {
        beforeLoad() // if this fails, then we should fail
        try {
          loadSnapshotFromCheckpoint(snapshot._1, snapshot._2)
          loadedSnapshot = Some(snapshot._1)
          logInfo(log"Successfully loaded snapshot version " +
            log"${MDC(VERSION_NUM, snapshot._1)} " +
            log"(uniqueId: ${MDC(UUID, snapshot._2.getOrElse(""))}). Repair complete.")
        } catch {
          case NonFatal(e) =>
            logError(log"Failed to load snapshot version " +
              log"${MDC(VERSION_NUM, snapshot._1)} " +
              log"(uniqueId: ${MDC(UUID, snapshot._2.getOrElse(""))}), will retry repair " +
              log"with the next eligible snapshot version", e)
            onLoadSnapshotFromCheckpointFailure()
            lastException = e
        }
      }

      if (loadedSnapshot.isEmpty) {
        // we tried all eligible snapshots and failed to load any of them
        logError(log"Auto snapshot repair failed to load any snapshot:" +
          log" latestSnapshotVersion: ${MDC(VERSION_NUM, firstEligibleSnapshot._1)}, " +
          log"attemptedSnapshots: ${MDC(VERSION_NUM, selectedRemainingSnapshots.map(_._1))}, " +
          log"eligibleSnapshots:  ${MDC(VERSION_NUM, remainingEligibleSnapshots.map(_._1))}, " +
          log"maxChangeFileReplay: ${MDC(VERSION_NUM, maxChangeFileReplay)}", lastException)
        throw StateStoreErrors.autoSnapshotRepairFailed(
          loggingId, firstEligibleSnapshot._1,
          selectedRemainingSnapshots.map(_._1),
          remainingEligibleSnapshots.map(_._1),
          lastException)
      } else {
        autoRepairCompleted = true
      }
    }

    // we would only get here if we successfully loaded a snapshot
    (loadedSnapshot.get, autoRepairCompleted)
  }
}

object SnapshotLoaderHelper {
  /** Get all the snapshot versions that can be used to load this version */
  def getEligibleSnapshotsForVersion(
      version: Long,
      fm: CheckpointFileManager,
      dfsPath: Path,
      pathFilter: PathFilter,
      fileSuffix: String): Seq[Long] = {
    if (fm.exists(dfsPath)) {
      ArraySeq.unsafeWrapArray(
        fm.list(dfsPath, pathFilter)
          .map(_.getPath.getName.stripSuffix(fileSuffix))
          .map(_.toLong)
      ).filter(_ <= version)
    } else {
      Seq(0L)
    }
  }
}

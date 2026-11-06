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

package org.apache.spark.shuffle.checksum

import scala.util.control.NonFatal

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}

/**
 * A class for computing checksum for input (key, value) pairs. The checksum is independent of
 * the order of the input (key, value) pairs. It is done by computing a checksum for each row
 * first, then computing the XOR and SUM for all the row checksums and mixing these two values
 * as the final checksum.
 *
 * [[failOnInvalidRow]] controls what happens when [[validateRow]] flags a row whose backing
 * memory is unsafe to read: true (default) logs the row's context and fails the task with a
 * descriptive error; false logs and disables this checksum so the query proceeds (a safety valve
 * for a validator false positive). In neither case is the invalid pointer dereferenced, so a
 * corrupt row no longer crashes the JVM with a SIGSEGV.
 */
abstract class RowBasedChecksum() extends Serializable with Logging {
  private val ROTATE_POSITIONS = 27
  private var hasError: Boolean = false
  private var checksumXor: Long = 0
  private var checksumSum: Long = 0
  // Rows passed to `update` so far (1-based for the current row); reported with an invalid row to
  // locate it within its partition. Not part of the checksum.
  private var rowOrdinal: Long = 0

  /**
   * When true (the default), a row flagged by [[validateRow]] fails the task; when false, it only
   * disables this checksum. Overridden by concrete subclasses (typically from a constructor arg).
   */
  protected def failOnInvalidRow: Boolean = true

  /**
   * Returns the checksum value. It returns the default checksum value (0) if there
   * are any errors encountered during the checksum computation.
   */
  def getValue: Long = {
    if (!hasError) {
      // Here we rotate the `checksumSum` to transforms these two values into a single, strong
      // composite checksum by ensuring their bit patterns are thoroughly mixed.
      checksumXor ^ rotateLeft(checksumSum)
    } else {
      0
    }
  }

  /** Updates the row-based checksum with the given (key, value) pair. Not thread safe. */
  def update(key: Any, value: Any): Unit = {
    if (!hasError) {
      rowOrdinal += 1
      // Guard the row before calculateRowChecksum touches its memory: a checksum that reads raw
      // row bytes would otherwise SIGSEGV on a corrupt pointer instead of reporting it.
      validateRow(value) match {
        case Some(problem) => reportInvalidRow(problem)
        case None =>
          try {
            val rowChecksumValue = calculateRowChecksum(key, value)
            checksumXor = checksumXor ^ rowChecksumValue
            checksumSum += rowChecksumValue
          } catch {
            case NonFatal(e) =>
              logError(log"Checksum computation encountered error", e)
              hasError = true
          }
      }
    }
  }

  /** Computes and returns the checksum value for the given (key, value) pair */
  protected def calculateRowChecksum(key: Any, value: Any): Long

  /**
   * Validates that `value`'s backing memory is safe to read, called before [[calculateRowChecksum]]
   * dereferences it. Returns Some(description) if the row must not be read (the description is
   * logged and, in fail mode, becomes the error message), or None if it looks well-formed. The
   * default accepts every row; subclasses that read raw row memory override this.
   */
  protected def validateRow(value: Any): Option[String] = None

  // Handles a row flagged by validateRow. Always logs the full context; then either fails the task
  // (failOnInvalidRow) or disables this checksum and lets the query proceed. Runs before the row is
  // dereferenced, turning what would have been a JVM crash into a loggable, attributable event.
  private def reportInvalidRow(problem: String): Unit = {
    val tc = TaskContext.get()
    val stage = if (tc != null) tc.stageId() else -1
    val partition = if (tc != null) tc.partitionId() else -1
    val taskAttempt = if (tc != null) tc.taskAttemptId() else -1L
    val context =
      log"${MDC(LogKeys.REASON, problem)} (stage=${MDC(LogKeys.STAGE_ID, stage)} " +
        log"partition=${MDC(LogKeys.PARTITION_ID, partition)} " +
        log"taskAttempt=${MDC(LogKeys.TASK_ATTEMPT_ID, taskAttempt)} " +
        log"rowOrdinal=${MDC(LogKeys.ROW_INDEX, rowOrdinal)})"
    if (failOnInvalidRow) {
      logError(log"Invalid row in shuffle row-based checksum: " + context)
      throw SparkException.internalError(
        s"Invalid row in shuffle row-based checksum: $problem (stage=$stage " +
          s"partition=$partition taskAttempt=$taskAttempt rowOrdinal=$rowOrdinal)")
    } else {
      logError(
        log"Invalid row in shuffle row-based checksum, disabling this partition's checksum: " +
          context)
      hasError = true
    }
  }

  // Rotate the value by shifting the bits by `ROTATE_POSITIONS` positions to the left.
  private def rotateLeft(value: Long): Long = {
    (value << ROTATE_POSITIONS) | (value >>> (64 - ROTATE_POSITIONS))
  }
}

object RowBasedChecksum {
  def getAggregatedChecksumValue(rowBasedChecksums: Array[RowBasedChecksum]): Long = {
    Option(rowBasedChecksums)
      .map(_.foldLeft(0L)((acc, c) => acc * 31L + c.getValue))
      .getOrElse(0L)
  }
}

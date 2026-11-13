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

package org.apache.spark.scheduler

import java.io.{BufferedOutputStream, DataOutputStream, OutputStream}
import java.util.Locale
import java.util.zip.{Adler32, Checksum, CRC32}

import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{SHUFFLE_MAP_STATUS_CHECKSUM_ALGORITHM, SHUFFLE_MAP_STATUS_CHECKSUM_ENABLED}
import org.apache.spark.shuffle.MetadataFetchFailedException

/**
 * Integrity checksums over the shuffle metadata a [[MapStatus]] carries from the mapper that
 * produced it to the reducers that consume it. Corruption on that path would otherwise let
 * reducers act on wrong block sizes, and a non-empty partition flipped to size zero is
 * silently skipped rather than fetched.
 *
 * The mapper stamps a status in its own constructor and the value then crosses the wire
 * verbatim; a reducer recomputes it once per shuffle. A mismatch raises a
 * [[MetadataFetchFailedException]], which Spark's fetch-failure path turns into a stage retry.
 */
private[spark] object MapStatusChecksum extends Logging {

  val ADLER32 = "ADLER32"
  val CRC32_ALG = "CRC32"

  /** Return a fresh Checksum instance for the given algorithm name. */
  def newChecksum(algorithm: String): Checksum = algorithm.toUpperCase(Locale.ROOT) match {
    case ADLER32 => new Adler32
    case CRC32_ALG => new CRC32
    case other =>
      throw new IllegalArgumentException(s"Unsupported MapStatus checksum algorithm: $other")
  }

  /** The mapper's stamp, called from a status's constructor. Returns 0 when the feature is off. */
  def compute(status: MapStatus): Int = {
    val env = SparkEnv.get
    if (env != null && env.conf.get(SHUFFLE_MAP_STATUS_CHECKSUM_ENABLED)) {
      compute(status, env.conf.get(SHUFFLE_MAP_STATUS_CHECKSUM_ALGORITHM))
    } else {
      0
    }
  }

  def compute(status: MapStatus, algorithm: String): Int = {
    val checksum = newChecksum(algorithm)
    val out = new DataOutputStream(new BufferedOutputStream(new OutputStream {
      override def write(b: Int): Unit = checksum.update(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = checksum.update(b, off, len)
    }, 256))
    status.writeForChecksum(out)
    out.flush()
    checksum.getValue.toInt
  }

  /**
   * Verify a freshly deserialized array of map statuses, once per executor per shuffle. Null
   * entries are mappers whose output is not registered yet; `convertMapStatuses` still fails
   * those for any reduce task that needs them.
   */
  def verify(shuffleId: Int, statuses: Array[MapStatus], conf: SparkConf): Unit = {
    if (conf.get(SHUFFLE_MAP_STATUS_CHECKSUM_ENABLED)) {
      val algorithm = conf.get(SHUFFLE_MAP_STATUS_CHECKSUM_ALGORITHM)
      var mapIndex = 0
      while (mapIndex < statuses.length) {
        val status = statuses(mapIndex)
        if (status != null) {
          val actual = try {
            compute(status, algorithm)
          } catch {
            case NonFatal(e) =>
              // Metadata too inconsistent for `writeForChecksum` to encode at all.
              val errorMessage = log"MapStatus checksum verification failed: invalid metadata" +
                log" for shuffleId=${MDC(SHUFFLE_ID, shuffleId)}" +
                log" mapId=${MDC(MAP_ID, status.mapId)}: ${MDC(ERROR, e.getMessage)}"
              logError(errorMessage)
              throw new MetadataFetchFailedException(shuffleId, -1, errorMessage.message)
          }
          // An unstamped status carries a zero, and fails here as a corrupted value would: a
          // MapStatus never crosses an application boundary, so every status was stamped by a
          // mapper running with this same configuration.
          if (actual != status.metadataChecksum) {
            val errorMessage = log"MapStatus checksum verification failed:" +
              log" shuffleId=${MDC(SHUFFLE_ID, shuffleId)}" +
              log" mapIndex=${MDC(INDEX, mapIndex)}" +
              log" mapId=${MDC(MAP_ID, status.mapId)}" +
              log" mapper=${MDC(BLOCK_MANAGER_ID, status.location)}" +
              log" type=${MDC(CLASS_NAME, status.getClass.getSimpleName)}." +
              log" This indicates MapStatus was corrupted between the mapper and this reducer."
            logError(errorMessage)
            throw new MetadataFetchFailedException(shuffleId, -1, errorMessage.message)
          }
        }
        mapIndex += 1
      }
    }
  }
}

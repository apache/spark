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
package org.apache.spark.shuffle

import java.io.{File, FileInputStream, FileOutputStream, IOException}

import com.google.common.annotations.VisibleForTesting

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.ShuffleMapStatusBlockId
import org.apache.spark.util.Utils

/**
 * Ensures that on each executor, there are no conflicting writes to the same shuffle files.  It
 * implements "first write wins", by atomically moving all shuffle files into their final location,
 * only if the files did not already exist. See SPARK-8029
 */
private[spark] object ShuffleOutputCoordinator extends Logging {

  /**
   * If any of the destination files do not exist, then move all of the temporary files to their
   * destinations, and return (true, the given MapStatus).  If all destination files exist, then
   * delete all temporary files, and return (false, the MapStatus from previously committed shuffle
   * output).
   *
   * Note that this will write to all destination files.  If the tmp file is missing, then a
   * zero-length destination file will be created.  This is so the ShuffleOutputCoordinator can work
   * even when there is non-determinstic data, where the output exists in one attempt, but is
   * empty in another attempt.
   *
   * @param tmpToDest  Seq of (temporary, destination) file pairs
   * @param mapStatus the [[MapStatus]] for the output already written to the the temporary files
   * @return pair of: (1) true iff the set of temporary files was moved to the destination and (2)
   *         the MapStatus of the committed attempt.
   */
  def commitOutputs(
      shuffleId: Int,
      partitionId: Int,
      tmpToDest: Seq[TmpDestShuffleFile],
      mapStatus: MapStatus,
      sparkEnv: SparkEnv): (Boolean, MapStatus) = synchronized {
    val mapStatusFile = sparkEnv.blockManager.diskBlockManager.getFile(
      ShuffleMapStatusBlockId(shuffleId, partitionId))
    val ser = sparkEnv.serializer.newInstance()
    commitOutputs(shuffleId, partitionId, tmpToDest, mapStatus, mapStatusFile, ser)
  }

  /** Exposed for testing. */
  def commitOutputs(
      shuffleId: Int,
      partitionId: Int,
      tmpToDest: Seq[TmpDestShuffleFile],
      mapStatus: MapStatus,
      mapStatusFile: File,
      serializer: SerializerInstance): (Boolean, MapStatus) = synchronized {
    // due to SPARK-4085, we only consider the previous attempt "committed" if all its output
    // files are present
    val destAlreadyExists = tmpToDest.forall(_.dstFile.exists()) && mapStatusFile.exists()
    if (!destAlreadyExists) {
      tmpToDest.foreach { case TmpDestShuffleFile(tmp, dest) =>
        // If *some* of the destination files exist, but not all of them, then it's not clear
        // what to do.  There could be a task already reading from this dest file when we delete
        // it -- but then again, something in that taskset would be doomed to fail in any case when
        // it got to the missing files.  Better to just put consistent output into place.
        // Note that for this to work with non-determinstic data, it is *critical* that each
        // attempt always produces the exact same set of destination files (even if they are empty).
        if (dest.exists()) {
          dest.delete()
        }
        if (tmp.exists()) {
          tmp.renameTo(dest)
        } else {
          // we always create the destination files, so this works correctly even when the
          // input data is non-deterministic (potentially empty in one iteration, and non-empty
          // in another)
          if (!dest.createNewFile()) {
            throw new IOException("could not create file: $file")
          }
        }
      }
      val out = serializer.serializeStream(new FileOutputStream(mapStatusFile))
      out.writeObject(mapStatus)
      out.close()
      (true, mapStatus)
    } else {
      logInfo(s"shuffle output for shuffle $shuffleId, partition $partitionId already exists, " +
        s"not overwriting.  Another task must have created this shuffle output.")
      tmpToDest.foreach { tmpAndDest => tmpAndDest.tmpFile.delete() }
      val in = serializer.deserializeStream(new FileInputStream(mapStatusFile))
      Utils.tryWithSafeFinally {
        (false, in.readObject[MapStatus]())
      } {
        in.close()
      }
    }
  }
}

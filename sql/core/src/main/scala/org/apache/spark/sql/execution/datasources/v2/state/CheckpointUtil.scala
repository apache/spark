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

package org.apache.spark.sql.execution.datasources.v2.state

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog, OffsetSeqMetadata}

/**
 * Providing features to deal with checkpoint, like creating savepoint.
 */
object CheckpointUtil {

  /**
   * Create savepoint from existing checkpoint.
   * OffsetLog and CommitLog will be purged based on newLastBatchId.
   * Use `additionalMetadataConf` to modify metadata configuration: you may want to modify it
   * when rescaling state, or migrate state format version.
   * e.g. when rescaling, pass Map(SQLConf.SHUFFLE_PARTITIONS.key -> newShufflePartitions.toString)
   *
   * @param sparkSession spark session
   * @param checkpointRoot the root path of existing checkpoint
   * @param newCheckpointRoot the root path of new savepoint - target directory should be empty
   * @param newLastBatchId the new last batch ID - it needs to be one of committed batch ID
   * @param additionalMetadataConf the configuration to add to existing metadata configuration
   * @param excludeState whether to exclude state directory
   */
  def createSavePoint(
      sparkSession: SparkSession,
      checkpointRoot: String,
      newCheckpointRoot: String,
      newLastBatchId: Long,
      additionalMetadataConf: Map[String, String],
      excludeState: Boolean = false): Unit = {
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val src = new Path(resolve(hadoopConf, checkpointRoot))
    val srcFs = src.getFileSystem(hadoopConf)
    val dst = new Path(resolve(hadoopConf, newCheckpointRoot))
    val dstFs = dst.getFileSystem(hadoopConf)

    if (dstFs.listFiles(dst, false).hasNext) {
      throw new IllegalArgumentException("Destination directory should be empty.")
    }

    dstFs.mkdirs(dst)

    // copy content of src directory to dst directory
    srcFs.listStatus(src).foreach { fs =>
      val path = fs.getPath
      val fileName = path.getName
      if (fileName == "state" && excludeState) {
        // pass
      } else {
        FileUtil.copy(srcFs, path, dstFs, new Path(dst, fileName),
          false, false, hadoopConf)
      }
    }

    val offsetLog = new OffsetSeqLog(sparkSession, new Path(dst, "offsets").toString)
    val logForBatch = offsetLog.get(newLastBatchId) match {
      case Some(log) => log
      case None => throw new IllegalStateException("offset log for batch should be exist")
    }

    val newMetadata = logForBatch.metadata match {
      case Some(md) =>
        val newMap = md.conf ++ additionalMetadataConf
        Some(md.copy(conf = newMap))
      case None =>
        Some(OffsetSeqMetadata(conf = additionalMetadataConf))
    }

    val newLogForBatch = logForBatch.copy(metadata = newMetadata)

    // we will restart from last batch + 1: overwrite the last batch with new configuration
    offsetLog.purgeAfter(newLastBatchId - 1)
    offsetLog.add(newLastBatchId, newLogForBatch)

    val commitLog = new CommitLog(sparkSession, new Path(dst, "commits").toString)
    commitLog.purgeAfter(newLastBatchId)

    // state doesn't expose purge mechanism as its interface
    // assuming state would work with overwriting batch files when it replays previous batch
  }

  private def resolve(hadoopConf: Configuration, cpLocation: String): String = {
    val checkpointPath = new Path(cpLocation)
    val fs = checkpointPath.getFileSystem(hadoopConf)
    checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
  }
}

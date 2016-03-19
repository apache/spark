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

package org.apache.spark.sql.execution.streaming

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.collection.OpenHashSet

/**
 * A very simple source that reads text files from the given directory as they appear.
 *
 * TODO Clean up the metadata files periodically
 */
class FileStreamSource(
    sqlContext: SQLContext,
    metadataPath: String,
    path: String,
    dataSchema: Option[StructType],
    providerName: String,
    dataFrameBuilder: Array[String] => DataFrame) extends Source with Logging {

  private val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
  private val metadataLog = new HDFSMetadataLog[Seq[String]](sqlContext, metadataPath)
  private var maxBatchId = metadataLog.getLatest().map(_._1).getOrElse(-1L)

  private val seenFiles = new OpenHashSet[String]
  metadataLog.get(None, maxBatchId).foreach { case (batchId, files) =>
    files.foreach(seenFiles.add)
  }

  /** Returns the schema of the data from this source */
  override lazy val schema: StructType = {
    dataSchema.getOrElse {
      val filesPresent = fetchAllFiles()
      if (filesPresent.isEmpty) {
        if (providerName == "text") {
          // Add a default schema for "text"
          new StructType().add("value", StringType)
        } else {
          throw new IllegalArgumentException("No schema specified")
        }
      } else {
        // There are some existing files. Use them to infer the schema.
        dataFrameBuilder(filesPresent.toArray).schema
      }
    }
  }

  /**
   * Returns the maximum offset that can be retrieved from the source.
   *
   * `synchronized` on this method is for solving race conditions in tests. In the normal usage,
   * there is no race here, so the cost of `synchronized` should be rare.
   */
  private def fetchMaxOffset(): LongOffset = synchronized {
    val filesPresent = fetchAllFiles()
    val newFiles = new ArrayBuffer[String]()
    filesPresent.foreach { file =>
      if (!seenFiles.contains(file)) {
        logDebug(s"new file: $file")
        newFiles.append(file)
        seenFiles.add(file)
      } else {
        logDebug(s"old file: $file")
      }
    }

    if (newFiles.nonEmpty) {
      maxBatchId += 1
      metadataLog.add(maxBatchId, newFiles)
    }

    new LongOffset(maxBatchId)
  }

  /**
   * For test only. Run `func` with the internal lock to make sure when `func` is running,
   * the current offset won't be changed and no new batch will be emitted.
   */
  def withBatchingLocked[T](func: => T): T = synchronized {
    func
  }

  /** Return the latest offset in the source */
  def currentOffset: LongOffset = synchronized {
    new LongOffset(maxBatchId)
  }

  /**
   * Returns the next batch of data that is available after `start`, if any is available.
   */
  override def getNextBatch(start: Option[Offset]): Option[Batch] = {
    val startId = start.map(_.asInstanceOf[LongOffset].offset).getOrElse(-1L)
    val end = fetchMaxOffset()
    val endId = end.offset

    if (startId + 1 <= endId) {
      val files = metadataLog.get(Some(startId + 1), endId).map(_._2).flatten
      logDebug(s"Return files from batches ${startId + 1}:$endId")
      logDebug(s"Streaming ${files.mkString(", ")}")
      Some(new Batch(end, dataFrameBuilder(files)))
    }
    else {
      None
    }
  }

  private def fetchAllFiles(): Seq[String] = {
    fs.listStatus(new Path(path))
      .filterNot(_.getPath.getName.startsWith("_"))
      .map(_.getPath.toUri.toString)
  }
}

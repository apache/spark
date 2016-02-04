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

import java.io.{BufferedWriter, OutputStreamWriter}

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.collection.OpenHashSet

/**
 * A very simple source that reads text files from the given directory as they appear.
 */
class FileStreamSource(
    sqlContext: SQLContext,
    metadataPath: String,
    path: String,
    dataSchema: Option[StructType],
    dataFrameBuilder: Array[String] => DataFrame) extends Source with Logging {

  import sqlContext.implicits._

  /** Returns the schema of the data from this source */
  override lazy val schema: StructType = {
    dataSchema.getOrElse {
      val filesPresent = fetchAllFiles()
      if (filesPresent.isEmpty) {
        new StructType().add("value", StringType)
      } else {
        // There are some existing files. Use them to infer the schema
        dataFrameBuilder(filesPresent.toArray).schema
      }
    }
  }

  /** Returns the maximum offset that can be retrieved from the source. */
  def fetchMaxOffset(): LongOffset = synchronized {
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
      writeBatch(maxBatchId, newFiles)
    }

    new LongOffset(maxBatchId)
  }

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

    val batchFiles = (startId + 1 to endId).filter(_ >= 0).map(i => s"$metadataPath/$i")
    if (batchFiles.nonEmpty) {
      logDebug(s"Producing files from batches ${startId + 1}:$endId")
      logDebug(s"Batch files: $batchFiles")

      // Probably does not need to be a spark job...
      val files = sqlContext
          .read
          .text(batchFiles: _*)
          .as[String]
          .collect()
      logDebug(s"Streaming ${files.mkString(", ")}")
      Some(new Batch(end, dataFrameBuilder(files)))
    } else {
      None
    }
  }

  private def sparkContext = sqlContext.sparkContext

  private val fs = FileSystem.get(sparkContext.hadoopConfiguration)
  private val existingBatchFiles = fetchAllBatchFiles()
  private val existingBatchIds = existingBatchFiles.map(_.getPath.getName.toInt)
  private var maxBatchId = if (existingBatchIds.isEmpty) -1 else existingBatchIds.max
  private val seenFiles = new OpenHashSet[String]

  if (existingBatchFiles.nonEmpty) {
    sqlContext.read
        .text(existingBatchFiles.map(_.getPath.toString): _*)
        .as[String]
        .collect()
        .foreach { file =>
          seenFiles.add(file)
        }
  }

  private def fetchAllBatchFiles(): Seq[FileStatus] = {
    try fs.listStatus(new Path(metadataPath)) catch {
      case _: java.io.FileNotFoundException =>
        fs.mkdirs(new Path(metadataPath))
        Seq.empty
    }
  }

  private def fetchAllFiles(): Seq[String] = {
    fs.listStatus(new Path(path))
      .filterNot(_.getPath.getName.startsWith("_"))
      .map(_.getPath.toUri.toString)
  }

  private def writeBatch(id: Int, files: Seq[String]): Unit = {
    val path = new Path(metadataPath + "/" + id)
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)))
    files.foreach { file =>
      writer.write(file)
      writer.write("\n")
    }
    writer.close()
    logDebug(s"Wrote batch file $path")
  }
}

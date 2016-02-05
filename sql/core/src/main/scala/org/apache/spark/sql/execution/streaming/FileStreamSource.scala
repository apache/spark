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

import java.io._

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Codec

import com.google.common.base.Charsets.UTF_8
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.Logging
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
    dataFrameBuilder: Array[String] => DataFrame) extends Source with Logging {

  private val version = sqlContext.sparkContext.version
  private val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
  private var maxBatchId = -1
  private val seenFiles = new OpenHashSet[String]

  /** Cache files for each batch. The content of this map is also stored in the disk.  */
  private val batchToMetadata = new HashMap[Long, Seq[String]]

  {
    // Restore statues from the metadata files
    val existingBatchFiles = fetchAllBatchFiles()
    if (existingBatchFiles.nonEmpty) {
      val existingBatchIds = existingBatchFiles.map(_.getPath.getName.toInt)
      maxBatchId = existingBatchIds.max
      // Recover "batchToMetadata" and "seenFiles" from existing metadata files.
      existingBatchIds.sorted.foreach { batchId =>
        val files = readBatch(batchId)
        if (files.isEmpty) {
          // Assert that the corrupted file must be the latest metadata file.
          require(batchId == maxBatchId, "Invalid metadata files")
          maxBatchId = maxBatchId - 1
        } else {
          batchToMetadata(batchId) = files
          files.foreach(seenFiles.add)
        }
      }
    }
  }

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

    if (startId + 1 <= endId) {
      val files = (startId + 1 to endId).filter(_ >= 0).flatMap { batchId =>
          batchToMetadata.getOrElse(batchId, Nil)
        }.toArray
      logDebug(s"Return files from batches ${startId + 1}:$endId")
      logDebug(s"Streaming ${files.mkString(", ")}")
      Some(new Batch(end, dataFrameBuilder(files)))
    }
    else {
      None
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

  /**
   * Write the metadata of a batch to disk. The file format is as follows:
   *
   * {{{
   *   SPARK_VERSION
   *   START
   *   -/a/b/c
   *   -/d/e/f
   *   ...
   *   END
   * }}}
   *
   * Note: every file path starts with "-" so that we can know if a line is a file path easily.
   */
  private def writeBatch(id: Int, files: Seq[String]): Unit = {
    assert(files.nonEmpty, "create a new batch without any file")
    val output = fs.create(new Path(metadataPath + "/" + id), true)
    val writer = new PrintWriter(new OutputStreamWriter(output, UTF_8))
    try {
      // scalastyle:off println
      writer.println(version)
      writer.println(FileStreamSource.START_TAG)
      files.foreach(file => writer.println("-" + file))
      writer.println(FileStreamSource.END_TAG)
      // scalastyle:on println
    } finally {
      writer.close()
    }
    batchToMetadata(id) = files
  }

  /** Read the file names of the specified batch id from the metadata file */
  private def readBatch(id: Int): Seq[String] = {
    val input = fs.open(new Path(metadataPath + "/" + id))
    try {
      FileStreamSource.readBatch(input)
    } finally {
      input.close()
    }
  }
}

object FileStreamSource {

  private val START_TAG = "START"
  private val END_TAG = "END"

  /**
   * Parse a metadata file and return the content. If the metadata file is corrupted, it will return
   * an empty `Seq`.
   */
  def readBatch(input: InputStream): Seq[String] = {
    val lines = scala.io.Source.fromInputStream(input)(Codec.UTF8).getLines().toArray
      .drop(1) // The first line is version, just drop it
    if (lines.isEmpty) {
      return Nil
    }
    if (lines.head != "START") {
      return Nil
    }
    if (lines.last != END_TAG) {
      return Nil
    }
    lines.slice(1, lines.length - 1).map(_.drop(1)) // Drop character "-"
  }
}

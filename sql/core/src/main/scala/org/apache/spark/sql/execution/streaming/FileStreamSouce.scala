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

import java.io.{OutputStreamWriter, BufferedWriter}

import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable.ArrayBuffer

class FileStreamSouce(
    val sqlContext: SQLContext,
    val metadataPath: String,
    val path: String) extends Source with Logging {

  import sqlContext.implicits._

  /** Returns the schema of the data from this source */
  override def schema: StructType =
    StructType(Nil).add("value", StringType)

  /** Returns the maximum offset that can be retrieved from the source. */
  def fetchMaxOffset(): LongOffset = synchronized {
    val filesPresent = fetchAllFiles()
    val newFiles = new ArrayBuffer[String]()
    filesPresent.foreach { file =>
      if (!seenFiles.contains(file)) {
        logWarning(s"new file: $file")
        newFiles.append(file)
        seenFiles.add(file)
      } else {
        logDebug(s"old file: $file")
      }
    }

    if (newFiles.nonEmpty) {
      maxBatchFile += 1
      writeBatch(maxBatchFile, newFiles)
    }

    new LongOffset(maxBatchFile)
  }

  /**
   * Returns the next batch of data that is available after `start`, if any is available.
   */
  override def getNextBatch(start: Option[Offset]): Option[Batch] = {
    val startId = start.map(_.asInstanceOf[LongOffset].offset).getOrElse(0L)
    val end = fetchMaxOffset()
    val endId = end.offset

    val batchFiles = (startId to endId).filter(_ >= 0).map(i => s"$metadataPath/$i")
    if (!(batchFiles.isEmpty || start == Some(end))) {
      log.warn(s"Producing files from batches $start:$endId")
      log.warn(s"Batch files: $batchFiles")

      // Probably does not need to be a spark job...
      val files = sqlContext
          .read
          .text(batchFiles: _*)
          .as[String]
          .collect()
      log.warn(s"Streaming ${files.mkString(", ")}")
      Some(new Batch(end, sqlContext.read.text(files: _*)))
    } else {
      None
    }
  }

  def restart(): FileStreamSouce = {
    new FileStreamSouce(sqlContext, metadataPath, path)
  }

  private def sparkContext = sqlContext.sparkContext

  private val fs = FileSystem.get(sparkContext.hadoopConfiguration)
  private val existingBatchFiles = fetchAllBatchFiles()
  private val existingBatchIds = existingBatchFiles.map(_.getPath.getName.toInt)
  private var maxBatchFile = if (existingBatchIds.isEmpty) -1 else existingBatchIds.max
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
    logWarning(s"Wrote batch file $path")
  }
}

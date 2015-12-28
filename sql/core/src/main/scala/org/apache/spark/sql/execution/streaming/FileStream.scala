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
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable.ArrayBuffer

class FileStream(
    val sqlContext: SQLContext,
    val metadataPath: String,
    val path: String) extends Source with Logging {

  import sqlContext.implicits._

  /** Returns the schema of the data from this source */
  override def schema: StructType =
    StructType(Nil).add("value", StringType)

  /** Returns the maximum offset that can be retrieved from the source. */
  override def offset: Offset = synchronized {
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
   * Returns the data between the `start` and `end` offsets.  This function must always return
   * the same set of data for any given pair of offsets.
   */
  override def getSlice(
      sqlContext: SQLContext,
      start: Option[Offset],
      end: Offset): RDD[InternalRow] = {
    val startId = start.map(_.asInstanceOf[LongOffset].offset).getOrElse(0L)
    val endId = end.asInstanceOf[LongOffset].offset

    log.warn(s"Producing files from batches $start:$end")
    if (endId == -1) {
      sparkContext.emptyRDD
    } else {
      val batchFiles = ((startId + 1) to endId).map(i => s"$metadataPath$i")
      log.warn(s"Batch files: $batchFiles")

      val files = sqlContext
        .read
        .text(batchFiles: _*)
        .as[String]
        .collect()
      log.warn(s"Streaming ${files.mkString(", ")}")
      sqlContext.read.text(files: _*).queryExecution.executedPlan.execute()
    }

  }

  def restart(): FileStream = {
    new FileStream(sqlContext, metadataPath, path)
  }

  private def sparkContext = sqlContext.sparkContext

  val fs = FileSystem.get(sparkContext.hadoopConfiguration)
  val existingBatchFiles = fetchAllBatchFiles()
  val existingBatchIds = existingBatchFiles.map(_.getPath.getName.toInt)
  var maxBatchFile = if (existingBatchIds.isEmpty) -1 else existingBatchIds.max
  val seenFiles = new OpenHashSet[String]

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
    fs.listStatus(new Path(metadataPath))
  }

  private def fetchAllFiles(): Seq[String] = {
    fs.listStatus(new Path(path)).map(_.getPath.toUri.toString)
  }

  private def writeBatch(id: Int, files: Seq[String]): Unit = {
    val path = new Path(metadataPath + id)
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
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

package org.apache.spark.sql.sqs

import java.net.URI

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.FileStreamSource._
import org.apache.spark.sql.types.StructType


class SqsSource(sparkSession: SparkSession,
                metadataPath: String,
                options: Map[String, String],
                override val schema: StructType) extends Source with Logging {

  private val sourceOptions = new SqsSourceOptions(options)

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  private val metadataLog =
    new FileStreamSourceLog(FileStreamSourceLog.VERSION, sparkSession, metadataPath)
  private var metadataLogCurrentOffset = metadataLog.getLatest().map(_._1).getOrElse(-1L)

  private val maxFilesPerTrigger = sourceOptions.maxFilesPerTrigger

  private val maxFileAgeMs: Long = sourceOptions.maxFileAgeMs

  private val fileFormatClassName = sourceOptions.fileFormatClassName

  private val shouldSortFiles = sourceOptions.shouldSortFiles

  private val sqsClient = new SqsClient(sourceOptions, hadoopConf)

  metadataLog.allFiles().foreach { entry =>
    sqsClient.sqsFileCache.add(entry.path, MessageDescription(entry.timestamp, true, ""))
  }
  sqsClient.sqsFileCache.purge()

  logInfo(s"maxFilesPerBatch = $maxFilesPerTrigger, maxFileAgeMs = $maxFileAgeMs")

  /**
    * Returns the data that is between the offsets (`start`, `end`].
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOffset = start.map(FileStreamSourceOffset(_).logOffset).getOrElse(-1L)
    val endOffset = FileStreamSourceOffset(end).logOffset

    assert(startOffset <= endOffset)
    val files = metadataLog.get(Some(startOffset + 1), Some(endOffset)).flatMap(_._2)
    logInfo(s"Processing ${files.length} files from ${startOffset + 1}:$endOffset")
    logTrace(s"Files are:\n\t" + files.mkString("\n\t"))
    val newDataSource =
      DataSource(
        sparkSession,
        paths = files.map(f => new Path(new URI(f.path)).toString),
        userSpecifiedSchema = Some(schema),
        className = fileFormatClassName,
        options = options)
    Dataset.ofRows(sparkSession, LogicalRelation(newDataSource.resolveRelation(
      checkFilesExist = false), isStreaming = true))
  }

  private def fetchMaxOffset(): FileStreamSourceOffset = synchronized {

    sqsClient.assertSqsIsWorking()
    /* All the new files found - ignore aged files and files that we have seen.
     *  Obey user's setting to limit the number of files in this batch trigger.
     */
    val batchFiles = sqsClient.sqsFileCache.getUncommittedFiles(maxFilesPerTrigger, shouldSortFiles)

    if (batchFiles.nonEmpty) {
      metadataLogCurrentOffset += 1
      metadataLog.add(metadataLogCurrentOffset, batchFiles.map {
        case (path, timestamp, receiptHandle) =>
          FileEntry(path = path, timestamp = timestamp, batchId = metadataLogCurrentOffset)
      }.toArray)
      logInfo(s"Log offset set to $metadataLogCurrentOffset with ${batchFiles.size} new files")
      val messageReceiptHandles = batchFiles.map {
        case (path, timestamp, receiptHandle) =>
          sqsClient.sqsFileCache.markCommitted(path)
          logDebug(s"New file: $path")
          receiptHandle
      }.toList
      sqsClient.addToDeleteMessageQueue(messageReceiptHandles)
    }

    val numPurged = sqsClient.sqsFileCache.purge()

    if (!sqsClient.deleteMessageQueue.isEmpty) {
      sqsClient.deleteMessagesFromQueue()
    }

    logTrace(
      s"""
         |Number of files selected for batch = ${batchFiles.size}
         |Number of files purged from tracking map = $numPurged
       """.stripMargin)

    FileStreamSourceOffset(metadataLogCurrentOffset)
  }

  override def getOffset: Option[Offset] = Some(fetchMaxOffset()).filterNot(_.logOffset == -1)

  override def commit(end: Offset): Unit = {
    // No-op for now; SqsSource currently garbage-collects files based on timestamp
    // and the value of the maxFileAge parameter.
  }

  override def stop(): Unit = {
    if (!sqsClient.sqsScheduler.isTerminated) {
      sqsClient.sqsScheduler.shutdownNow()
    }
  }

  override def toString: String = s"SqsSource[${sqsClient.sqsUrl}]"

}


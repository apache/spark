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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging

  /**
    * A custom hash map used to track the list of files seen. This map is thread-safe.
    *
    * To prevent the hash map from growing indefinitely, a purge function is available to
    * remove files "maxAgeMs" older than the latest file.
    */

class SqsFileCache(maxAgeMs: Long, fileNameOnly: Boolean) extends Logging {
  require(maxAgeMs >= 0)
  if (fileNameOnly) {
    logWarning("'fileNameOnly' is enabled. Make sure your file names are unique (e.g. using " +
      "UUID), otherwise, files with the same name but under different paths will be considered " +
      "the same and causes data lost.")
  }

  /** Mapping from file path to its message description. */
  private val sqsMap = new ConcurrentHashMap[String, MessageDescription]

  /** Timestamp for the last purge operation. */
  private var lastPurgeTimestamp: Long = 0L

  /** Timestamp of the latest file. */
  private var latestTimestamp: Long = 0L

  @inline private def stripPathIfNecessary(path: String) = {
    if (fileNameOnly) new Path(new URI(path)).getName else path
  }

  /**
    * Returns true if we should consider this file a new file. The file is only considered "new"
    * if it is new enough that we are still tracking, and we have not seen it before.
    */
  def isNewFile(path: String, timestamp: Long): Boolean = {
    timestamp >= lastPurgeTimestamp && !sqsMap.containsKey(stripPathIfNecessary(path))
  }

  /** Add a new file to the map. */
  def add(path: String, fileStatus: MessageDescription): Unit = {
    sqsMap.put(stripPathIfNecessary(path), fileStatus)
    if (fileStatus.timestamp > latestTimestamp) {
      latestTimestamp = fileStatus.timestamp
    }
  }

  /**
    * Returns all the new files found - ignore aged files and files that we have already seen.
    * Sorts the files by timestamp.
    */
  def getUncommittedFiles(maxFilesPerTrigger: Option[Int],
                             shouldSortFiles: Boolean): Seq[(String, Long, String)] = {
    if (shouldSortFiles) {
      val uncommittedFiles = filterAllUncommittedFiles()
      val sortedFiles = reportTimeTaken("Sorting Files") {
         uncommittedFiles.sortWith(_._2 < _._2)
      }
      if (maxFilesPerTrigger.nonEmpty) sortedFiles.take(maxFilesPerTrigger.get) else sortedFiles
    } else {
      if (maxFilesPerTrigger.isEmpty) {
        filterAllUncommittedFiles()
      } else {
        filterTopUncommittedFiles(maxFilesPerTrigger.get)
      }
    }
  }
    private def filterTopUncommittedFiles(maxFilesPerTrigger: Int): List[(String, Long, String)] = {
      val iterator = sqsMap.asScala.iterator
      val uncommittedFiles = ListBuffer[(String, Long, String)]()
      while (uncommittedFiles.length < maxFilesPerTrigger && iterator.hasNext) {
        val file = iterator.next()
        if (file._2.isCommitted && file._2.timestamp >= lastPurgeTimestamp) {
          uncommittedFiles += ((file._1, file._2.timestamp, file._2.messageReceiptHandle))
        }
      }
      uncommittedFiles.toList
    }

    private def reportTimeTaken[T](operation: String)(body: => T): T = {
      val startTime = System.currentTimeMillis()
      val result = body
      val endTime = System.currentTimeMillis()
      val timeTaken = math.max(endTime - startTime, 0)

      logDebug(s"$operation took $timeTaken ms")
      result
    }

    private def filterAllUncommittedFiles(): List[(String, Long, String)] = {
      sqsMap.asScala.foldLeft(List[(String, Long, String)]()) {
        (list, file) =>
          if (!file._2.isCommitted && file._2.timestamp >= lastPurgeTimestamp) {
            list :+ ((file._1, file._2.timestamp, file._2.messageReceiptHandle))
          } else {
            list
          }
      }
    }

  /** Removes aged entries and returns the number of files removed. */
  def purge(): Int = {
    lastPurgeTimestamp = latestTimestamp - maxAgeMs
    var count = 0
    sqsMap.asScala.foreach { fileEntry =>
      if (fileEntry._2.timestamp < lastPurgeTimestamp) {
        sqsMap.remove(fileEntry._1)
        count += 1
      }
    }
    count
  }

  /** Mark file entry as committed or already processed */
  def markCommitted(path: String): Unit = {
    sqsMap.replace(path, MessageDescription(
      sqsMap.get(path).timestamp, true, sqsMap.get(path).messageReceiptHandle))
  }

  def size: Int = sqsMap.size()

}

  /**
    * A case class to store file metadata. Metadata includes file timestamp, file status -
    * committed or not committed and message reciept handle used for deleting message from
    * Amazon SQS
    */
case class MessageDescription(timestamp: Long,
                              isCommitted: Boolean = false,
                              messageReceiptHandle: String)
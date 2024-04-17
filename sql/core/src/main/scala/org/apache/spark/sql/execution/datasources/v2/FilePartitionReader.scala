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
package org.apache.spark.sql.execution.datasources.v2

import java.io.{FileNotFoundException, IOException}

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.PARTITIONED_FILE_READER
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile

class FilePartitionReader[T](
    files: Iterator[PartitionedFile],
    buildReader: PartitionedFile => PartitionReader[T],
    options: FileSourceOptions)
  extends PartitionReader[T] with Logging {
  private var currentReader: PartitionedFileReader[T] = null

  private def ignoreMissingFiles = options.ignoreMissingFiles
  private def ignoreCorruptFiles = options.ignoreCorruptFiles

  override def next(): Boolean = {
    if (currentReader == null) {
      if (files.hasNext) {
        val file = files.next()
        logInfo(s"Reading file $file")
        // Sets InputFileBlockHolder for the file block's information
        InputFileBlockHolder.set(file.urlEncodedPath, file.start, file.length)
        try {
          currentReader = PartitionedFileReader(file, buildReader(file))
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(s"Skipped missing file.", e)
            currentReader = null
          case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
            logWarning(
              s"Skipped the rest of the content in the corrupted file.", e)
            currentReader = null
          case e: Throwable => throw FileDataSourceV2.attachFilePath(file.urlEncodedPath, e)
        }
      } else {
        return false
      }
    }

    // In PartitionReader.next(), the current reader proceeds to next record.
    // It might throw RuntimeException/IOException and Spark should handle these exceptions.
    val hasNext = try {
      currentReader != null && currentReader.next()
    } catch {
      case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
        logWarning(log"Skipped the rest of the content in the corrupted file: " +
          log"${MDC(PARTITIONED_FILE_READER, currentReader)}", e)
        false
      case e: Throwable =>
        throw FileDataSourceV2.attachFilePath(currentReader.file.urlEncodedPath, e)
    }
    if (hasNext) {
      true
    } else {
      close()
      currentReader = null
      next()
    }
  }

  override def get(): T = currentReader.get()

  override def close(): Unit = {
    if (currentReader != null) {
      currentReader.close()
    }
    InputFileBlockHolder.unset()
  }
}

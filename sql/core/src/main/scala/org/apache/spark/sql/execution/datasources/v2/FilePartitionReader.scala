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

import scala.util.control.NonFatal

import org.apache.spark.SparkUpgradeException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.internal.SQLConf

class FilePartitionReader[T](readers: Iterator[PartitionedFileReader[T]])
  extends PartitionReader[T] with Logging {
  private var currentReader: PartitionedFileReader[T] = null

  private val sqlConf = SQLConf.get
  private def ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private def ignoreCorruptFiles = sqlConf.ignoreCorruptFiles

  override def next(): Boolean = {
    if (currentReader == null) {
      if (readers.hasNext) {
        try {
          currentReader = getNextReader()
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(s"Skipped missing file.", e)
            currentReader = null
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles =>
            throw QueryExecutionErrors.fileNotFoundError(e)
          case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
            logWarning(
              s"Skipped the rest of the content in the corrupted file.", e)
            currentReader = null
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
      case e: SchemaColumnConvertNotSupportedException =>
        throw QueryExecutionErrors.unsupportedSchemaColumnConvertError(
          currentReader.file.filePath, e.getColumn, e.getLogicalType, e.getPhysicalType, e)
      case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
        logWarning(
          s"Skipped the rest of the content in the corrupted file: $currentReader", e)
        false
      case sue: SparkUpgradeException => throw sue
      case NonFatal(e) =>
        e.getCause match {
          case sue: SparkUpgradeException => throw sue
          case _ => throw QueryExecutionErrors.cannotReadFilesError(e, currentReader.file.filePath)
        }
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

  private def getNextReader(): PartitionedFileReader[T] = {
    val reader = readers.next()
    logInfo(s"Reading file $reader")
    // Sets InputFileBlockHolder for the file block's information
    val file = reader.file
    InputFileBlockHolder.set(file.filePath, file.start, file.length)
    reader
  }
}

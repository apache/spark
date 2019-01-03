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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.reader.PartitionReader

class FilePartitionReader[T](
    readers: Iterator[PartitionReader[T]]) extends PartitionReader[T] with Logging {
  private var currentFile: PartitionReader[T] = null

  private val sqlConf = SQLConf.get
  private val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles

  override def next(): Boolean = {
    if (currentFile == null) {
      if (readers.hasNext) {
        if (ignoreMissingFiles || ignoreCorruptFiles) {
          try {
            currentFile = readers.next()
          } catch {
            case e: FileNotFoundException if ignoreMissingFiles =>
              logWarning(s"Skipped missing file: $currentFile", e)
              currentFile = null
              return false
            // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
            case e: FileNotFoundException if !ignoreMissingFiles => throw e
            case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
              logWarning(
                s"Skipped the rest of the content in the corrupted file: $currentFile", e)
              currentFile = null
              return false
          }
        } else {
          currentFile = readers.next()
        }
      } else {
        return false
      }
    }
    if (currentFile.next()) {
      return true
    } else {
      close()
      currentFile = null
    }
    next()
  }

  override def get(): T = currentFile.get()

  override def close(): Unit = {
    if (currentFile != null) {
      currentFile.close()
    }
  }
}

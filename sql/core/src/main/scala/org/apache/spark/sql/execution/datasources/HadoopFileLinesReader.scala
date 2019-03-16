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

package org.apache.spark.sql.execution.datasources

import java.io.Closeable
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, LineRecordReader}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

/**
 * An adaptor from a [[PartitionedFile]] to an [[Iterator]] of [[Text]], which are all of the lines
 * in that file.
 *
 * @param file A part (i.e. "block") of a single file that should be read line by line.
 * @param lineSeparator A line separator that should be used for each line. If the value is `None`,
 *                      it covers `\r`, `\r\n` and `\n`.
 * @param conf Hadoop configuration
 *
 * @note The behavior when `lineSeparator` is `None` (covering `\r`, `\r\n` and `\n`) is defined
 * by [[LineRecordReader]], not within Spark.
 */
class HadoopFileLinesReader(
    file: PartitionedFile,
    lineSeparator: Option[Array[Byte]],
    conf: Configuration) extends Iterator[Text] with Closeable {

  def this(file: PartitionedFile, conf: Configuration) = this(file, None, conf)

  private val iterator = {
    val fileSplit = new FileSplit(
      new Path(new URI(file.filePath)),
      file.start,
      file.length,
      // The locality is decided by `getPreferredLocations` in `FileScanRDD`.
      Array.empty)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    val reader = lineSeparator match {
      case Some(sep) => new LineRecordReader(sep)
      // If the line separator is `None`, it covers `\r`, `\r\n` and `\n`.
      case _ => new LineRecordReader()
    }

    reader.initialize(fileSplit, hadoopAttemptContext)
    new RecordReaderIterator(reader)
  }

  override def hasNext: Boolean = iterator.hasNext

  override def next(): Text = iterator.next()

  override def close(): Unit = iterator.close()
}

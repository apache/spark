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

package org.apache.spark.input

import com.google.common.io.{ByteStreams, Closeables}

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext

/**
 * A [[org.apache.hadoop.mapreduce.RecordReader RecordReader]] for reading a single whole text file
 * out in a key-value pair, where the key is the file path and the value is the entire content of
 * the file.
 */
private[spark] class WholeTextFileRecordReader(
    split: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
  extends RecordReader[String, String] {

  private val path = split.getPath(index)
  private val fs = path.getFileSystem(context.getConfiguration)

  // True means the current file has been processed, then skip it.
  private var processed = false

  private val key = path.toString
  private var value: String = null

  override def initialize(split: InputSplit, context: TaskAttemptContext) = {}

  override def close() = {}

  override def getProgress = if (processed) 1.0f else 0.0f

  override def getCurrentKey = key

  override def getCurrentValue = value

  override def nextKeyValue = {
    if (!processed) {
      val fileIn = fs.open(path)
      val innerBuffer = ByteStreams.toByteArray(fileIn)

      value = new Text(innerBuffer).toString
      Closeables.close(fileIn, false)

      processed = true
      true
    } else {
      false
    }
  }
}

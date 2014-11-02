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

import java.io.IOException

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

/**
 * FixedLengthBinaryRecordReader is returned by FixedLengthBinaryInputFormat.
 * It uses the record length set in FixedLengthBinaryInputFormat to
 * read one record at a time from the given InputSplit.
 *
 * Each call to nextKeyValue() updates the LongWritable key and BytesWritable value.
 *
 * key = record index (Long)
 * value = the record itself (BytesWritable)
 */
private[spark] class FixedLengthBinaryRecordReader
  extends RecordReader[LongWritable, BytesWritable] {

  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L
  private var currentPosition: Long = 0L
  private var recordLength: Int = 0
  private var fileInputStream: FSDataInputStream = null
  private var recordKey: LongWritable = null
  private var recordValue: BytesWritable = null

  override def close() {
    if (fileInputStream != null) {
      fileInputStream.close()
    }
  }

  override def getCurrentKey: LongWritable = {
    recordKey
  }

  override def getCurrentValue: BytesWritable = {
    recordValue
  }

  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(
        ((currentPosition - splitStart) / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {
    // the file input
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // the byte position this fileSplit starts at
    splitStart = fileSplit.getStart

    // splitEnd byte marker that the fileSplit ends at
    splitEnd = splitStart + fileSplit.getLength

    // the actual file we will be reading from
    val file = fileSplit.getPath
    // job configuration
    val job = context.getConfiguration
    // check compression
    val codec = new CompressionCodecFactory(job).getCodec(file)
    if (codec != null) {
      throw new IOException("FixedLengthRecordReader does not support reading compressed files")
    }
    // get the record length
    recordLength = FixedLengthBinaryInputFormat.getRecordLength(context)
    // get the filesystem
    val fs = file.getFileSystem(job)
    // open the File
    fileInputStream = fs.open(file)
    // seek to the splitStart position
    fileInputStream.seek(splitStart)
    // set our current position
    currentPosition = splitStart
  }

  override def nextKeyValue(): Boolean = {
    if (recordKey == null) {
      recordKey = new LongWritable()
    }
    // the key is a linear index of the record, given by the
    // position the record starts divided by the record length
    recordKey.set(currentPosition / recordLength)
    // the recordValue to place the bytes into
    if (recordValue == null) {
      recordValue = new BytesWritable(new Array[Byte](recordLength))
    }
    // read a record if the currentPosition is less than the split end
    if (currentPosition < splitEnd) {
      // setup a buffer to store the record
      val buffer = recordValue.getBytes
      fileInputStream.read(buffer, 0, recordLength)
      // update our current position
      currentPosition = currentPosition + recordLength
      // return true
      return true
    }
    false
  }
}

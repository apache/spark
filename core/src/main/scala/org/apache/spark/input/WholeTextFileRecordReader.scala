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

import org.apache.hadoop.conf.{Configuration, Configurable}
import com.google.common.io.{ByteStreams, Closeables}

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, CombineFileRecordReader}
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * A [[org.apache.hadoop.mapreduce.RecordReader RecordReader]] for reading a single whole text file
 * out in a key-value pair, where the key is the file path and the value is the entire content of
 * the file.
 */
private[spark] class WholeTextFileRecordReader(
    split: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
  extends RecordReader[String, String] with Configurable {

  private var conf: Configuration = _
  def setConf(c: Configuration) {
    conf = c
  }
  def getConf: Configuration = conf

  private[this] val path = split.getPath(index)
  private[this] val fs = path.getFileSystem(
    SparkHadoopUtil.get.getConfigurationFromJobContext(context))

  // True means the current file has been processed, then skip it.
  private[this] var processed = false

  private[this] val key = path.toString
  private[this] var value: String = null

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

  override def close(): Unit = {}

  override def getProgress: Float = if (processed) 1.0f else 0.0f

  override def getCurrentKey: String = key

  override def getCurrentValue: String = value

  override def nextKeyValue(): Boolean = {
    if (!processed) {
      val conf = new Configuration
      val factory = new CompressionCodecFactory(conf)
      val codec = factory.getCodec(path)  // infers from file ext.
      val fileIn = fs.open(path)
      val innerBuffer = if (codec != null) {
        ByteStreams.toByteArray(codec.createInputStream(fileIn))
      } else {
        ByteStreams.toByteArray(fileIn)
      }

      value = new Text(innerBuffer).toString
      Closeables.close(fileIn, false)
      processed = true
      true
    } else {
      false
    }
  }
}


/**
 * A [[org.apache.hadoop.mapreduce.RecordReader RecordReader]] for reading a single whole text file
 * out in a key-value pair, where the key is the file path and the value is the entire content of
 * the file.
 */
private[spark] class WholeCombineFileRecordReader(
    split: InputSplit,
    context: TaskAttemptContext)
  extends CombineFileRecordReader[String, String](
    split.asInstanceOf[CombineFileSplit],
    context,
    classOf[WholeTextFileRecordReader]
  ) with Configurable {

  private var conf: Configuration = _
  def setConf(c: Configuration) {
    conf = c
  }
  def getConf: Configuration = conf

  override def initNextRecordReader(): Boolean = {
    val r = super.initNextRecordReader()
    if (r) {
      this.curReader.asInstanceOf[WholeTextFileRecordReader].setConf(conf)
    }
    r
  }
}

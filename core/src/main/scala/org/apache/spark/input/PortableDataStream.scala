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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.JavaConverters._

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{CombineFileInputFormat, CombineFileRecordReader, CombineFileSplit}

import org.apache.spark.internal.config
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since

/**
 * A general format for reading whole files in as streams, byte arrays,
 * or other functions to be added
 */
private[spark] abstract class StreamFileInputFormat[T]
  extends CombineFileInputFormat[String, T]
{
  override protected def isSplitable(context: JobContext, file: Path): Boolean = false

  /**
   * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API
   * which is set through setMaxSplitSize
   */
  def setMinPartitions(sc: SparkContext, context: JobContext, minPartitions: Int) {
    val defaultMaxSplitBytes = sc.getConf.get(config.FILES_MAX_PARTITION_BYTES)
    val openCostInBytes = sc.getConf.get(config.FILES_OPEN_COST_IN_BYTES)
    val defaultParallelism = sc.defaultParallelism
    val files = listStatus(context).asScala
    val totalBytes = files.filterNot(_.isDirectory).map(_.getLen + openCostInBytes).sum
    val bytesPerCore = totalBytes / defaultParallelism
    val maxSplitSize = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
    super.setMaxSplitSize(maxSplitSize)
  }

  def createRecordReader(split: InputSplit, taContext: TaskAttemptContext): RecordReader[String, T]

}

/**
 * An abstract class of [[org.apache.hadoop.mapreduce.RecordReader RecordReader]]
 * to reading files out as streams
 */
private[spark] abstract class StreamBasedRecordReader[T](
    split: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
  extends RecordReader[String, T] {

  // True means the current file has been processed, then skip it.
  private var processed = false

  private var key = ""
  private var value: T = null.asInstanceOf[T]

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}
  override def close(): Unit = {}

  override def getProgress: Float = if (processed) 1.0f else 0.0f

  override def getCurrentKey: String = key

  override def getCurrentValue: T = value

  override def nextKeyValue: Boolean = {
    if (!processed) {
      val fileIn = new PortableDataStream(split, context, index)
      value = parseStream(fileIn)
      key = fileIn.getPath
      processed = true
      true
    } else {
      false
    }
  }

  /**
   * Parse the stream (and close it afterwards) and return the value as in type T
   * @param inStream the stream to be read in
   * @return the data formatted as
   */
  def parseStream(inStream: PortableDataStream): T
}

/**
 * Reads the record in directly as a stream for other objects to manipulate and handle
 */
private[spark] class StreamRecordReader(
    split: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
  extends StreamBasedRecordReader[PortableDataStream](split, context, index) {

  def parseStream(inStream: PortableDataStream): PortableDataStream = inStream
}

/**
 * The format for the PortableDataStream files
 */
private[spark] class StreamInputFormat extends StreamFileInputFormat[PortableDataStream] {
  override def createRecordReader(split: InputSplit, taContext: TaskAttemptContext)
    : CombineFileRecordReader[String, PortableDataStream] = {
    new CombineFileRecordReader[String, PortableDataStream](
      split.asInstanceOf[CombineFileSplit], taContext, classOf[StreamRecordReader])
  }
}

/**
 * A class that allows DataStreams to be serialized and moved around by not creating them
 * until they need to be read
 * @note TaskAttemptContext is not serializable resulting in the confBytes construct
 * @note CombineFileSplit is not serializable resulting in the splitBytes construct
 */
class PortableDataStream(
    isplit: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
  extends Serializable {

  private val confBytes = {
    val baos = new ByteArrayOutputStream()
    context.getConfiguration.write(new DataOutputStream(baos))
    baos.toByteArray
  }

  private val splitBytes = {
    val baos = new ByteArrayOutputStream()
    isplit.write(new DataOutputStream(baos))
    baos.toByteArray
  }

  @transient private lazy val split = {
    val bais = new ByteArrayInputStream(splitBytes)
    val nsplit = new CombineFileSplit()
    nsplit.readFields(new DataInputStream(bais))
    nsplit
  }

  @transient private lazy val conf = {
    val bais = new ByteArrayInputStream(confBytes)
    val nconf = new Configuration()
    nconf.readFields(new DataInputStream(bais))
    nconf
  }
  /**
   * Calculate the path name independently of opening the file
   */
  @transient private lazy val path = {
    val pathp = split.getPath(index)
    pathp.toString
  }

  /**
   * Create a new DataInputStream from the split and context. The user of this method is responsible
   * for closing the stream after usage.
   */
  @Since("1.2.0")
  def open(): DataInputStream = {
    val pathp = split.getPath(index)
    val fs = pathp.getFileSystem(conf)
    fs.open(pathp)
  }

  /**
   * Read the file as a byte array
   */
  @Since("1.2.0")
  def toArray(): Array[Byte] = {
    val stream = open()
    try {
      ByteStreams.toByteArray(stream)
    } finally {
      Closeables.close(stream, true)
    }
  }

  @Since("1.2.0")
  def getPath(): String = path

  @Since("2.2.0")
  def getConfiguration: Configuration = conf
}


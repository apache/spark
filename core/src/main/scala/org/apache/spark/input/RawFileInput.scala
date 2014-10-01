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

import scala.collection.JavaConversions._
import com.google.common.io.{ ByteStreams, Closeables }
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.fs.{ FSDataInputStream, Path }
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader
import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream, DataInputStream }

/**
 *  A general format for reading whole files in as streams, byte arrays,
 *  or other functions to be added
 */
abstract class StreamFileInputFormat[T]
  extends CombineFileInputFormat[String, T] {
  override protected def isSplitable(context: JobContext, file: Path): Boolean = false
  /**
   * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API.
   */
  def setMaxSplitSize(context: JobContext, minPartitions: Int) {
    val files = listStatus(context)
    val totalLen = files.map { file =>
      if (file.isDir) 0L else file.getLen
    }.sum

    val maxSplitSize = Math.ceil(totalLen * 1.0 / files.length).toLong
    super.setMaxSplitSize(maxSplitSize)
  }

  def createRecordReader(split: InputSplit, taContext: TaskAttemptContext): RecordReader[String, T]

}

/**
 * A class that allows DataStreams to be serialized and moved around by not creating them
 * until they need to be read
 * @note TaskAttemptContext is not serializable resulting in the confBytes construct
 * @note CombineFileSplit is not serializable resulting in the splitBytes construct
 */
class PortableDataStream(@transient isplit: CombineFileSplit,
  @transient context: TaskAttemptContext, index: Integer)
  extends Serializable {
  // transient forces file to be reopened after being serialization
  // it is also used for non-serializable classes

  @transient
  private var fileIn: DataInputStream = null.asInstanceOf[DataInputStream]
  @transient
  private var isOpen = false

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

  @transient
  private lazy val split = {
    val bais = new ByteArrayInputStream(splitBytes)
    val nsplit = new CombineFileSplit()
    nsplit.readFields(new DataInputStream(bais))
    nsplit
  }

  @transient
  private lazy val conf = {
    val bais = new ByteArrayInputStream(confBytes)
    val nconf = new Configuration()
    nconf.readFields(new DataInputStream(bais))
    nconf
  }
  /**
   * Calculate the path name independently of opening the file
   */
  @transient
  private lazy val path = {
    val pathp = split.getPath(index)
    pathp.toString
  }

  /**
   * create a new DataInputStream from the split and context
   */
  def open(): DataInputStream = {
    if (!isOpen) {
      val pathp = split.getPath(index)
      val fs = pathp.getFileSystem(conf)
      fileIn = fs.open(pathp)
      isOpen = true
    }
    fileIn
  }

  /**
   * Read the file as a byte array
   */
  def toArray(): Array[Byte] = {
    open()
    val innerBuffer = ByteStreams.toByteArray(fileIn)
    close()
    innerBuffer
  }

  /**
   * close the file (if it is already open)
   */
  def close() = {
    if (isOpen) {
      try {
        fileIn.close()
        isOpen = false
      } catch {
        case ioe: java.io.IOException => // do nothing
      }
    }
  }
  def getPath(): String = path
}

/**
 * An abstract class of [[org.apache.hadoop.mapreduce.RecordReader RecordReader]]
 * to reading files out as streams
 */
abstract class StreamBasedRecordReader[T](
  split: CombineFileSplit,
  context: TaskAttemptContext,
  index: Integer)
  extends RecordReader[String, T] {

  // True means the current file has been processed, then skip it.
  private var processed = false

  private var key = ""
  private var value: T = null.asInstanceOf[T]

  override def initialize(split: InputSplit, context: TaskAttemptContext) = {}
  override def close() = {}

  override def getProgress = if (processed) 1.0f else 0.0f

  override def getCurrentKey = key

  override def getCurrentValue = value

  override def nextKeyValue = {
    if (!processed) {
      val fileIn = new PortableDataStream(split, context, index)
      value = parseStream(fileIn)
      fileIn.close() // if it has not been open yet, close does nothing
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
 * A class for extracting the information from the file using the
 * BinaryRecordReader (as Byte array)
 */
private[spark] class StreamInputFormat extends StreamFileInputFormat[PortableDataStream] {
  override def createRecordReader(split: InputSplit, taContext: TaskAttemptContext) =
    {
      new CombineFileRecordReader[String, PortableDataStream](
        split.asInstanceOf[CombineFileSplit], taContext, classOf[StreamRecordReader])
    }
}

/**
 * A [[org.apache.hadoop.mapreduce.RecordReader RecordReader]] for reading a single binary file
 * out in a key-value pair, where the key is the file path and the value is the entire content of
 * the file as a byte array
 */
abstract class BinaryRecordReader[T](
  split: CombineFileSplit,
  context: TaskAttemptContext,
  index: Integer)
  extends StreamBasedRecordReader[T](split, context, index) {

  def parseStream(inpStream: PortableDataStream): T = {
    val inStream = inpStream.open()
    val innerBuffer = ByteStreams.toByteArray(inStream)
    Closeables.close(inStream, false)
    parseByteArray(innerBuffer)
  }
  def parseByteArray(inArray: Array[Byte]): T
}

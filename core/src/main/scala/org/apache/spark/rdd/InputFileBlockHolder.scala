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

package org.apache.spark.rdd

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.unsafe.types.UTF8String

/**
 * This holds file names of the current Spark task. This is used in HadoopRDD,
 * FileScanRDD, NewHadoopRDD and InputFileName function in Spark SQL.
 */
private[spark] object InputFileBlockHolder {
  /**
   * A wrapper around some input file information.
   *
   * @param filePath path of the file read, or empty string if not available.
   * @param startOffset starting offset, in bytes, or -1 if not available.
   * @param length size of the block, in bytes, or -1 if not available.
   */
  private class FileBlock(val filePath: UTF8String, val startOffset: Long, val length: Long) {
    def this() = {
      this(UTF8String.fromString(""), -1, -1)
    }
  }

  /**
   * The thread variable for the name of the current file being read. This is used by
   * the InputFileName function in Spark SQL.
   *
   * @note `inputBlock` works somewhat complicatedly. It guarantees that `initialValue`
   * is called at the start of a task. Therefore, one atomic reference is created in the task
   * thread. After that, read and write happen to the same atomic reference across the parent and
   * children threads. This is in order to support a case where write happens in a child thread
   * but read happens at its parent thread, for instance, Python UDF execution. See SPARK-28153.
   */
  private[this] val inputBlock: InheritableThreadLocal[AtomicReference[FileBlock]] =
    new InheritableThreadLocal[AtomicReference[FileBlock]] {
      override protected def initialValue(): AtomicReference[FileBlock] =
        new AtomicReference(new FileBlock)
    }

  /**
   * Returns the holding file name or empty string if it is unknown.
   */
  def getInputFilePath: UTF8String = inputBlock.get().get().filePath

  /**
   * Returns the starting offset of the block currently being read, or -1 if it is unknown.
   */
  def getStartOffset: Long = inputBlock.get().get().startOffset

  /**
   * Returns the length of the block being read, or -1 if it is unknown.
   */
  def getLength: Long = inputBlock.get().get().length

  /**
   * Sets the thread-local input block.
   */
  def set(filePath: String, startOffset: Long, length: Long): Unit = {
    require(filePath != null, "filePath cannot be null")
    require(startOffset >= 0, s"startOffset ($startOffset) cannot be negative")
    require(length >= -1, s"length ($length) cannot be smaller than -1")
    inputBlock.get().set(new FileBlock(UTF8String.fromString(filePath), startOffset, length))
  }

  /**
   * Clears the input file block to default value.
   */
  def unset(): Unit = inputBlock.remove()

  /**
   * Initializes thread local by explicitly getting the value. It triggers ThreadLocal's
   * initialValue in the parent thread.
   */
  def initialize(): Unit = inputBlock.get()
}

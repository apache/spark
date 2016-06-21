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

import org.apache.spark.unsafe.types.UTF8String

/**
 * This holds file names of the current Spark task. This is used in HadoopRDD,
 * FileScanRDD, NewHadoopRDD and InputFileName function in Spark SQL.
 */
private[spark] object InputFileNameHolder {
  /**
   * The thread variable for the name of the current file being read. This is used by
   * the InputFileName function in Spark SQL.
   */
  private[this] val inputFileName: ThreadLocal[UTF8String] = new ThreadLocal[UTF8String] {
    override protected def initialValue(): UTF8String = UTF8String.fromString("")
  }

  def getInputFileName(): UTF8String = inputFileName.get()

  private[spark] def setInputFileName(file: String) = inputFileName.set(UTF8String.fromString(file))

  private[spark] def unsetInputFileName(): Unit = inputFileName.remove()

}

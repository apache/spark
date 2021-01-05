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

package org.apache.spark.sql.util

import org.apache.spark.SparkException
import org.apache.spark.util.SizeEstimator


object CollectExecutionMemoryUsage {

  private val MEM_USAGE = new ThreadLocal[CollectExecutionMemoryUsage]

  def init(maxAllowedSize: Long) : Unit = {
    MEM_USAGE.set(new CollectExecutionMemoryUsage(maxAllowedSize))
  }

  def current: CollectExecutionMemoryUsage = {
    MEM_USAGE.get()
  }

  def clear(): Unit = {
    MEM_USAGE.remove()
  }
}

class CollectExecutionMemoryUsage(maxAllowedSize: Long) {

  private var estimateAccumulatedSize = 0L

  private var rowCount = 0L

  def estimatedMemoryUsage() : Long = estimateAccumulatedSize

  private[spark] def setRowCount(cnt: Long) : Unit = {
    rowCount = cnt
  }

  private[spark] def addSizeOfArray[A <: Any](arr: Array[A]) : Unit = {
    if (!arr.isEmpty) {
      setRowCount(arr.length)
      addSizeOfArraySizeEstimatedByFistElement(arr(0))
    }
  }

  private[spark] def addSizeOfArraySizeEstimatedByFistElement(obj: Any) : Unit = {
    estimateAccumulatedSize += SizeEstimator.estimate(obj.asInstanceOf[AnyRef]) * rowCount
    if (estimateAccumulatedSize > maxAllowedSize) {
      throw new SparkException(s"estimate memory usage ${estimateAccumulatedSize} " +
        s"> allowd maximum memory usage ${maxAllowedSize}")
    }
  }
}

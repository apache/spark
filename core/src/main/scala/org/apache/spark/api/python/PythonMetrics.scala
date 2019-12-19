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

package org.apache.spark.api.python

import java.util.concurrent.atomic.AtomicLong

private[spark] object PythonMetrics {

  // Instrument with general metrics on serialization/deserialization JVM-to-Python
  private val toWorkerWriteTime = new AtomicLong(0L)
  private val toWorkerBatchCount = new AtomicLong(0L)
  private val toWorkerBytesWritten = new AtomicLong(0L)
  private val fromWorkerReadTime = new AtomicLong(0L)
  private val fromWorkerBatchCount = new AtomicLong(0L)
  private val fromWorkerBytesRead = new AtomicLong(0L)

  // Instrument Pandas_UDF
  private val pandasUDFReadRowCount = new AtomicLong(0L)
  private val pandasUDFWriteRowCount = new AtomicLong(0L)

  def incToWorkerWriteTime(delta: Long): Unit = {
    toWorkerWriteTime.getAndAdd(delta)
  }

  def getToWorkerWriteTime: Long = {
    toWorkerWriteTime.get
  }

  def incToWorkerBytesWritten(delta: Long): Unit = {
    toWorkerBytesWritten.getAndAdd(delta)
  }

  def getToWorkerBytesWritten: Long = {
    toWorkerBytesWritten.get
  }

  def incToWorkerBatchCount(delta: Long): Unit = {
    toWorkerBatchCount.getAndAdd(delta)
  }

  def getToWorkerBatchCount: Long = {
    toWorkerBatchCount.get
  }

  def incFromWorkerReadTime(delta: Long): Unit = {
    fromWorkerReadTime.getAndAdd(delta)
  }

  def getFromWorkerReadTime: Long = {
    fromWorkerReadTime.get
  }

  def incFromWorkerBatchCount(delta: Long): Unit = {
    fromWorkerBatchCount.getAndAdd(delta)
  }

  def getFromWorkerBatchCount: Long = {
    fromWorkerBatchCount.get
  }

  def incFromWorkerBytesRead(delta: Long): Unit = {
    fromWorkerBytesRead.getAndAdd(delta)
  }

  def getFromWorkerBytesRead: Long = {
    fromWorkerBytesRead.get
  }

  // Pandas_UDF
  def incPandasUDFReadRowCount(step: Long): Unit = {
    pandasUDFReadRowCount.getAndAdd(step)
  }

  def getPandasUDFReadRowCount: Long = {
    pandasUDFReadRowCount.get
  }

  def incPandasUDFWriteRowCount(step: Long): Unit = {
    pandasUDFWriteRowCount.getAndAdd(step)
  }

  def getPandasUDFWriteRowCount: Long = {
    pandasUDFWriteRowCount.get
  }

}

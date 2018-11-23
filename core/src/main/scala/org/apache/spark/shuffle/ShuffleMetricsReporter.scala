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

package org.apache.spark.shuffle

/**
 * An interface for reporting shuffle information, for each shuffle. This interface assumes
 * all the methods are called on a single-threaded, i.e. concrete implementations would not need
 * to synchronize anything.
 */
private[spark] trait ShuffleMetricsReporter {
  def incRemoteBlocksFetched(v: Long): Unit
  def incLocalBlocksFetched(v: Long): Unit
  def incRemoteBytesRead(v: Long): Unit
  def incRemoteBytesReadToDisk(v: Long): Unit
  def incLocalBytesRead(v: Long): Unit
  def incFetchWaitTime(v: Long): Unit
  def incRecordsRead(v: Long): Unit
}

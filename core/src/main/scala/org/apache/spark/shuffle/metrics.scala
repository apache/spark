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
 * An interface for reporting shuffle read metrics, for each shuffle. This interface assumes
 * all the methods are called on a single-threaded, i.e. concrete implementations would not need
 * to synchronize.
 *
 * All methods have additional Spark visibility modifier to allow public, concrete implementations
 * that still have these methods marked as private[spark].
 */
private[spark] trait ShuffleReadMetricsReporter {
  private[spark] def incRemoteBlocksFetched(v: Long): Unit
  private[spark] def incLocalBlocksFetched(v: Long): Unit
  private[spark] def incRemoteBytesRead(v: Long): Unit
  private[spark] def incRemoteBytesReadToDisk(v: Long): Unit
  private[spark] def incLocalBytesRead(v: Long): Unit
  private[spark] def incFetchWaitTime(v: Long): Unit
  private[spark] def incRecordsRead(v: Long): Unit
}


/**
 * An interface for reporting shuffle write metrics. This interface assumes all the methods are
 * called on a single-threaded, i.e. concrete implementations would not need to synchronize.
 *
 * All methods have additional Spark visibility modifier to allow public, concrete implementations
 * that still have these methods marked as private[spark].
 */
private[spark] trait ShuffleWriteMetricsReporter {
  private[spark] def incBytesWritten(v: Long): Unit
  private[spark] def incRecordsWritten(v: Long): Unit
  private[spark] def incWriteTime(v: Long): Unit
  private[spark] def decBytesWritten(v: Long): Unit
  private[spark] def decRecordsWritten(v: Long): Unit
}

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

package org.apache.spark.streaming.dstream

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/**
 * A common trait for all DStream which would like to make data persistence
 *
 * @tparam K Class of data key
 * @tparam S Class of data value
 */
abstract class DumpableDStream[K: ClassTag, S: ClassTag](ssc: StreamingContext)
  extends DStream[(K, S)](ssc) {
  // A reference to dumpFormat. default value is None, so nothing will be dumpped. User need to
  // specify DumpFormat instance, if he/she want to dump data during process shutdown.
  var _dumpFormat: Option[DSteamDumpFormat[K, S]] = None

  def setDumpFormat(df: DSteamDumpFormat[K, S]): Unit = {
    _dumpFormat = Some(df)
  }

  // dump data on given time to given path
  def dump(time: Time, path: String): Unit = _dumpFormat match {
    case None =>
      log.debug("DumpFormat is not set, nothing will be dumped for this DStream")
    case Some(df) =>
      log.info(s"DumpFormat is set as ${df.getClass}, start dumping ${df.identity}")

      // "identity" in DumpFormat will be used as file name
      val dumpPath = path + "/" + df.identity
      val validTime = lastValidTime(time)
      log.info(s"convent ${time} to valid time ${validTime}")

      getOrCompute(validTime) match {
        case Some(rdd) =>
          df.dump(rdd, dumpPath) // delegate to DumpFormat.dump
          log.info(s"Finish dumping ${df.identity} into path ${dumpPath}")
        case None =>
          log.info(s"There is no RDD on ${validTime}, nothing will be dumped for this DStream")
      }
  }
}

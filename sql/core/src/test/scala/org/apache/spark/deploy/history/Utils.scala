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

package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.History.HISTORY_LOG_DIR
import org.apache.spark.util.ManualClock

object Utils {
  def withFsHistoryProvider(logDir: String)(fn: FsHistoryProvider => Unit): Unit = {
    var provider: FsHistoryProvider = null
    try {
      val clock = new ManualClock()
      val conf = new SparkConf().set(HISTORY_LOG_DIR, logDir)
      val provider = new FsHistoryProvider(conf, clock)
      provider.checkForLogs()
      fn(provider)
    } finally {
      if (provider != null) {
        provider.stop()
        provider = null
      }
    }
  }
}

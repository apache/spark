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

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogTestHelper.{TestEventFilter1, TestEventFilter2}
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerBlockManagerAdded, SparkListenerEvent, SparkListenerExecutorAdded, SparkListenerJobStart, SparkListenerNodeBlacklisted, SparkListenerNodeUnblacklisted, SparkListenerTaskStart, SparkListenerUnpersistRDD}
import org.apache.spark.storage.BlockManagerId

class EventLogFilterRateCalculatorSuite extends SparkFunSuite {
  private val sparkConf = new SparkConf()
  private val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)

  test("calculate filter-in rate") {
    withTempDir { dir =>
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)

      val events = new mutable.ArrayBuffer[SparkListenerEvent]

      // filterApplicationEnd: Some(true) & Some(true) => filter in
      events += SparkListenerApplicationEnd(0)

      // filterBlockManagerAdded: Some(true) & Some(false) => filter out
      events += SparkListenerBlockManagerAdded(0, BlockManagerId("1", "host1", 1), 10)

      // filterApplicationStart: Some(false) & Some(false) => filter out
      events += SparkListenerApplicationStart("app", None, 0, "user", None)

      // filterNodeBlacklisted: None & Some(true) => filter in
      events += SparkListenerNodeBlacklisted(0, "host1", 1)

      // filterNodeUnblacklisted: None & Some(false) => filter out
      events += SparkListenerNodeUnblacklisted(0, "host1")

      // other events: None & None => filter in
      events += SparkListenerUnpersistRDD(0)

      val logPath = EventLogTestHelper.writeEventLogFile(sparkConf, hadoopConf, dir, 1, events)
      val logPath2 = EventLogTestHelper.writeEventLogFile(sparkConf, hadoopConf, dir, 2, events)
      val logs = Seq(logPath, logPath2).map(new Path(_))

      val filters = Seq(new TestEventFilter1, new TestEventFilter2)
      val calculator = new EventLogFilterRateCalculator(fs)
      // 6 filtered in, 6 filtered out
      assert(0.5d === calculator.doCalculate(logs, filters))
    }
  }
}

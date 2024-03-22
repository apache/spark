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

package org.apache.spark.sql.streaming

import java.time.Duration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

class ValueStateTTLProcessor
  extends StatefulProcessor[String, String, (String, Long)]
  with Logging {

  @transient private var _countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeoutMode: TimeoutMode): Unit = {
    _countState = getHandle.getValueState("countState", Encoders.scalaLong)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, Long)] = {

    val currValueOption = _countState.getOption()

    var totalLogins: Long = inputRows.size
    if (currValueOption.isDefined) {
      totalLogins = totalLogins + currValueOption.get
    }

    _countState.update(totalLogins, Duration.ofMinutes(1))

    Iterator.single((key, totalLogins))
  }
}

class TransformWithStateTTLSuite
  extends StreamTest {
  import testImplicits._

  test("validate state is evicted at ttl expiry") {

    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputStream = MemoryStream[String]
      val result = inputStream.toDS()
        .groupByKey(x => x)
        .transformWithState(
          new ValueStateTTLProcessor(),
          TimeoutMode.NoTimeouts(),
          TTLMode.ProcessingTimeTTL())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream, "k1"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("k1", 1L)),
        // advance clock so that state expires
        AdvanceManualClock(60 * 1000),
        AddData(inputStream, "k1"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("k1", 1))
      )
    }
  }
}

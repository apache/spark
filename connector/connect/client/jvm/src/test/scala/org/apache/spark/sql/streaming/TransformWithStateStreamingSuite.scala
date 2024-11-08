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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.test.{QueryTest, RemoteSparkSession}

class RunningCountStatefulProcessor
  extends StatefulProcessor[String, (String, String), (String, String)]
  with Logging {
  @transient protected var _countState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong)
  }

  override def handleInputRows(
    key: String,
    inputRows: Iterator[(String, String)],
    timerValues: TimerValues): Iterator[(String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + 1
    _countState.update(count)
    Iterator((key, count.toString))
  }
}

class TransformWithStateStreamingSuite extends QueryTest with RemoteSparkSession {
  test("transformWithState - streaming") {
    withSQLConf("spark.sql.streaming.stateStore.providerClass" ->
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
      "spark.sql.shuffle.partitions" -> "5") {
      val session: SparkSession = spark
      import session.implicits._

      spark.sql("DROP TABLE IF EXISTS my_sink")

      withTempPath { dir =>
        val q = spark.readStream
          .format("rate")
          .option("rowsPerSecond", "10")
          .option("numPartitions", "5")
          .load()
          .withColumn("id", unix_timestamp(col("timestamp")).cast("string"))
          .withColumn("value", col("value").cast("string"))
          .select("id", "value")
          .as[(String, String)]
          .groupByKey(x => x._1)
          .transformWithState(
            new RunningCountStatefulProcessor(),
            TimeMode.None(), OutputMode.Update())
          .writeStream
          .format("memory")
          .queryName("my_sink")
          .start()

        try {
          q.processAllAvailable()

        } finally {
          q.stop()
          spark.sql("DROP TABLE IF EXISTS my_sink")
        }
      }
    }
  }
}

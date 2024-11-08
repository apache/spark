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

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class NotTheSameRunningCountStatefulProcessor
  extends StatefulProcessor[String, String, (String, String)]
  with Logging {
  @transient protected var _countState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong)
  }

  override def handleInputRows(
    key: String,
    inputRows: Iterator[String],
    timerValues: TimerValues): Iterator[(String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + inputRows.toSeq.length
    _countState.update(count)
    Iterator((key, count.toString))
  }
}

class TransformWithStateStreamingSuite extends QueryTest with RemoteSparkSession {
  val testData: Seq[String] = Seq("a", "b", "a")

  test("transformWithState - streaming") {
    withSQLConf("spark.sql.streaming.stateStore.providerClass" ->
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
      "spark.sql.shuffle.partitions" -> "5") {
      val session: SparkSession = spark
      import session.implicits._

      spark.sql("DROP TABLE IF EXISTS my_sink")

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        testData.toDS().toDF("value").repartition(3).write.parquet(path)

        val testSchema = StructType(Array(StructField("value", StringType)))

        val q = spark.readStream
          .schema(testSchema)
          .option("maxFilesPerTrigger", 1)
          .parquet(path)
          .as[String]
          .groupByKey(x => x)
          .transformWithState(
            new NotTheSameRunningCountStatefulProcessor(),
            TimeMode.None(), OutputMode.Update())
          .writeStream
          .format("memory")
          .queryName("my_sink")
          .start()

        try {
          q.processAllAvailable()
          eventually(timeout(30.seconds)) {
            checkDataset(
              spark.table("my_sink").toDF().as[(String, String)],
              ("a", "1"), ("b", "1"), ("a", "2")
            )
          }
        } finally {
          q.stop()
          spark.sql("DROP TABLE IF EXISTS my_sink")
        }
      }
    }
  }
}

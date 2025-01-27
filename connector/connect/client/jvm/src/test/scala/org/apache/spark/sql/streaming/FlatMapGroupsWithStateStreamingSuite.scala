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

import java.sql.Timestamp

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Append
import org.apache.spark.sql.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

case class ClickEvent(id: String, timestamp: Timestamp)

case class ClickState(id: String, count: Int)

class FlatMapGroupsWithStateStreamingSuite extends QueryTest with RemoteSparkSession {

  val flatMapGroupsWithStateSchema: StructType = StructType(
    Array(StructField("id", StringType), StructField("timestamp", TimestampType)))

  val flatMapGroupsWithStateData: Seq[ClickEvent] = Seq(
    ClickEvent("a", new Timestamp(12345)),
    ClickEvent("a", new Timestamp(12346)),
    ClickEvent("a", new Timestamp(12347)),
    ClickEvent("b", new Timestamp(12348)),
    ClickEvent("b", new Timestamp(12349)),
    ClickEvent("c", new Timestamp(12350)))

  val flatMapGroupsWithStateInitialStateData: Seq[ClickState] =
    Seq(ClickState("a", 2), ClickState("b", 1))

  test("flatMapGroupsWithState - streaming") {
    val session: SparkSession = spark
    import session.implicits._

    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        if (state.exists) throw new IllegalArgumentException("state.exists should be false")
        val newState = ClickState(key, values.size)
        state.update(newState)
        Iterator(newState)
      }
    spark.sql("DROP TABLE IF EXISTS my_sink")

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      flatMapGroupsWithStateData.toDS().write.parquet(path)
      val q = spark.readStream
        .schema(flatMapGroupsWithStateSchema)
        .parquet(path)
        .as[ClickEvent]
        .groupByKey(_.id)
        .flatMapGroupsWithState(Append, GroupStateTimeout.NoTimeout)(stateFunc)
        .writeStream
        .format("memory")
        .queryName("my_sink")
        .start()

      try {
        q.processAllAvailable()
        eventually(timeout(30.seconds)) {
          checkDataset(
            spark.table("my_sink").toDF().as[ClickState],
            ClickState("c", 1),
            ClickState("b", 2),
            ClickState("a", 3))
        }
      } finally {
        q.stop()
        spark.sql("DROP TABLE IF EXISTS my_sink")
      }
    }
  }

  test("flatMapGroupsWithState - streaming - with initial state") {
    val session: SparkSession = spark
    import session.implicits._

    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        val currState = state.getOption.getOrElse(ClickState(key, 0))
        val newState = ClickState(key, currState.count + values.size)
        state.update(newState)
        Iterator(newState)
      }
    val initialState = flatMapGroupsWithStateInitialStateData
      .toDS()
      .groupByKey(_.id)
      .mapValues(x => x)
    spark.sql("DROP TABLE IF EXISTS my_sink")

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      flatMapGroupsWithStateData.toDS().write.parquet(path)
      val q = spark.readStream
        .schema(flatMapGroupsWithStateSchema)
        .parquet(path)
        .as[ClickEvent]
        .groupByKey(_.id)
        .flatMapGroupsWithState(Append, GroupStateTimeout.NoTimeout, initialState)(stateFunc)
        .writeStream
        .format("memory")
        .queryName("my_sink")
        .start()

      try {
        q.processAllAvailable()
        eventually(timeout(30.seconds)) {
          checkDataset(
            spark.table("my_sink").toDF().as[ClickState],
            ClickState("c", 1),
            ClickState("b", 3),
            ClickState("a", 5))
        }
      } finally {
        q.stop()
        spark.sql("DROP TABLE IF EXISTS my_sink")
      }
    }
  }

  test("mapGroupsWithState - streaming") {
    val session: SparkSession = spark
    import session.implicits._

    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        if (state.exists) throw new IllegalArgumentException("state.exists should be false")
        val newState = ClickState(key, values.size)
        state.update(newState)
        newState
      }
    spark.sql("DROP TABLE IF EXISTS my_sink")

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      flatMapGroupsWithStateData.toDS().write.parquet(path)
      val q = spark.readStream
        .schema(flatMapGroupsWithStateSchema)
        .parquet(path)
        .as[ClickEvent]
        .groupByKey(_.id)
        .mapGroupsWithState(GroupStateTimeout.NoTimeout)(stateFunc)
        .writeStream
        .format("memory")
        .queryName("my_sink")
        .outputMode("update")
        .start()

      try {
        q.processAllAvailable()
        eventually(timeout(30.seconds)) {
          checkDataset(
            spark.table("my_sink").toDF().as[ClickState],
            ClickState("c", 1),
            ClickState("b", 2),
            ClickState("a", 3))
        }
      } finally {
        q.stop()
        spark.sql("DROP TABLE IF EXISTS my_sink")
      }
    }
  }

  test("mapGroupsWithState - streaming - with initial state") {
    val session: SparkSession = spark
    import session.implicits._

    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        val currState = state.getOption.getOrElse(ClickState(key, 0))
        val newState = ClickState(key, currState.count + values.size)
        state.update(newState)
        newState
      }
    val initialState = flatMapGroupsWithStateInitialStateData
      .toDS()
      .groupByKey(_.id)
      .mapValues(x => x)
    spark.sql("DROP TABLE IF EXISTS my_sink")

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      flatMapGroupsWithStateData.toDS().write.parquet(path)
      val q = spark.readStream
        .schema(flatMapGroupsWithStateSchema)
        .parquet(path)
        .as[ClickEvent]
        .groupByKey(_.id)
        .mapGroupsWithState(GroupStateTimeout.NoTimeout, initialState)(stateFunc)
        .writeStream
        .format("memory")
        .queryName("my_sink")
        .outputMode("update")
        .start()

      try {
        q.processAllAvailable()
        eventually(timeout(30.seconds)) {
          checkDataset(
            spark.table("my_sink").toDF().as[ClickState],
            ClickState("c", 1),
            ClickState("b", 3),
            ClickState("a", 5))
        }
      } finally {
        q.stop()
        spark.sql("DROP TABLE IF EXISTS my_sink")
      }
    }
  }
}

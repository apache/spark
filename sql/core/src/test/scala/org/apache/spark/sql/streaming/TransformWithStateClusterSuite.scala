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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf

case class FruitState(
    name: String,
    count: Long,
    family: String
)

class FruitCountStatefulProcessor(useImplicits: Boolean)
  extends StatefulProcessor[String, String, (String, Long, String)] {
  import implicits._

  @transient protected var _fruitState: ValueState[FruitState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    if (useImplicits) {
      _fruitState = getHandle.getValueState[FruitState]("fruitState", TTLConfig.NONE)
    } else {
      _fruitState = getHandle.getValueState("fruitState", Encoders.product[FruitState],
        TTLConfig.NONE)
    }
  }

  private def getFamily(fruitName: String): String = {
    if (fruitName == "orange" || fruitName == "lemon" || fruitName == "lime") {
      "citrus"
    } else {
      "non-citrus"
    }
  }

  override def handleInputRows(key: String, inputRows: Iterator[String], timerValues: TimerValues):
    Iterator[(String, Long, String)] = {
    val new_cnt = _fruitState.getOption().map(x => x.count).getOrElse(0L) + inputRows.size
    val family = getFamily(key)
    _fruitState.update(FruitState(key, new_cnt, family))
    Iterator.single((key, new_cnt, family))
  }
}

class FruitCountStatefulProcessorWithInitialState(useImplicits: Boolean)
  extends StatefulProcessorWithInitialState[String, String, (String, Long, String), String] {
  import implicits._

  @transient protected var _fruitState: ValueState[FruitState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    if (useImplicits) {
      _fruitState = getHandle.getValueState[FruitState]("fruitState", TTLConfig.NONE)
    } else {
      _fruitState = getHandle.getValueState("fruitState", Encoders.product[FruitState],
        TTLConfig.NONE)
    }
  }

  private def getFamily(fruitName: String): String = {
    if (fruitName == "orange" || fruitName == "lemon" || fruitName == "lime") {
      "citrus"
    } else {
      "non-citrus"
    }
  }

  override def handleInitialState(key: String, initialState: String,
    timerValues: TimerValues): Unit = {
    val new_cnt = _fruitState.getOption().map(x => x.count).getOrElse(0L) + 1
    val family = getFamily(key)
    _fruitState.update(FruitState(key, new_cnt, family))
  }

  override def handleInputRows(key: String, inputRows: Iterator[String], timerValues: TimerValues):
    Iterator[(String, Long, String)] = {
    val new_cnt = _fruitState.getOption().map(x => x.count).getOrElse(0L) + inputRows.size
    val family = getFamily(key)
    _fruitState.update(FruitState(key, new_cnt, family))
    Iterator.single((key, new_cnt, family))
  }
}

trait TransformWithStateClusterSuiteBase extends SparkFunSuite {
  def getSparkConf(): SparkConf = {
    val conf = new SparkConf()
      .setMaster("local-cluster[2, 2, 1024]")
      .set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
        classOf[RocksDBStateStoreProvider].getCanonicalName)
      .set(SQLConf.SHUFFLE_PARTITIONS.key,
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString)
      .set(SQLConf.STREAMING_STOP_TIMEOUT, 5000L)
    conf
  }

  // Start a new test with cluster containing two executors and streaming stop timeout set to 5s
  val testSparkConf = getSparkConf()

  protected def testWithAndWithoutImplicitEncoders(name: String)
    (func: (SparkSession, Boolean) => Any): Unit = {
    Seq(false, true).foreach { useImplicits =>
      test(s"$name - useImplicits = $useImplicits") {
        withSparkSession(SparkSession.builder().config(testSparkConf).getOrCreate()) { spark =>
          func(spark, useImplicits)
        }
      }
    }
  }
}

/**
 * Test suite spawning local cluster with multiple executors to test serde of stateful
 * processors along with use of implicit encoders, if applicable in transformWithState operator.
 */
class TransformWithStateClusterSuite extends StreamTest with TransformWithStateClusterSuiteBase {
  testWithAndWithoutImplicitEncoders("streaming with transformWithState - " +
   "without initial state") { (spark, useImplicits) =>
    import spark.implicits._
    val input = MemoryStream(Encoders.STRING, spark.sqlContext)
    val agg = input.toDS()
      .groupByKey(x => x)
      .transformWithState(new FruitCountStatefulProcessor(useImplicits),
        TimeMode.None(),
        OutputMode.Update()
      )

    val query = agg.writeStream
      .format("memory")
      .outputMode("update")
      .queryName("output")
      .start()

    input.addData("apple", "apple", "orange", "orange", "orange")
    query.processAllAvailable()

    checkAnswer(spark.sql("select * from output"),
      Seq(Row("apple", 2, "non-citrus"),
        Row("orange", 3, "citrus")))

    input.addData("lemon", "lime")
    query.processAllAvailable()
    checkAnswer(spark.sql("select * from output"),
      Seq(Row("apple", 2, "non-citrus"),
        Row("orange", 3, "citrus"),
        Row("lemon", 1, "citrus"),
        Row("lime", 1, "citrus")))

    query.stop()
  }

  testWithAndWithoutImplicitEncoders("streaming with transformWithState - " +
   "with initial state") { (spark, useImplicits) =>
    import spark.implicits._

    val fruitCountInitialDS: Dataset[String] = Seq(
      "apple", "apple", "orange", "orange", "orange").toDS()

    val fruitCountInitial = fruitCountInitialDS
      .groupByKey(x => x)

    val input = MemoryStream(Encoders.STRING, spark.sqlContext)
    val agg = input.toDS()
      .groupByKey(x => x)
      .transformWithState(new FruitCountStatefulProcessorWithInitialState(useImplicits),
        TimeMode.None(),
        OutputMode.Update(), fruitCountInitial)

    val query = agg.writeStream
      .format("memory")
      .outputMode("update")
      .queryName("output")
      .start()

    input.addData("apple", "apple", "orange", "orange", "orange")
    query.processAllAvailable()

    checkAnswer(spark.sql("select * from output"),
      Seq(Row("apple", 4, "non-citrus"),
        Row("orange", 6, "citrus")))

    input.addData("lemon", "lime")
    query.processAllAvailable()
    checkAnswer(spark.sql("select * from output"),
      Seq(Row("apple", 4, "non-citrus"),
        Row("orange", 6, "citrus"),
        Row("lemon", 1, "citrus"),
        Row("lime", 1, "citrus")))

    query.stop()
  }
}

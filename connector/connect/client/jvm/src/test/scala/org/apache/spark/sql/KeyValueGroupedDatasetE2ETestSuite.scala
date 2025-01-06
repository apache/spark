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
package org.apache.spark.sql

import java.sql.Timestamp
import java.util.Arrays

import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Append
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.util.SparkSerDeUtils

case class ClickEvent(id: String, timestamp: Timestamp)

case class ClickState(id: String, count: Int)

/**
 * All tests in this class requires client UDF artifacts synced with the server.
 */
class KeyValueGroupedDatasetE2ETestSuite extends QueryTest with RemoteSparkSession {

  lazy val session: SparkSession = spark
  import session.implicits._

  test("mapGroups") {
    val session: SparkSession = spark
    import session.implicits._
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .mapGroups((_, it) => it.toSeq.size)
      .collectAsList()
    assert(values == Arrays.asList[Int](5, 5))
  }

  test("flatGroupMap") {
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .flatMapGroups((_, it) => Seq(it.toSeq.size))
      .collectAsList()
    assert(values == Arrays.asList[Int](5, 5))
  }

  test("keys") {
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .keys
      .collectAsList()
    assert(values == Arrays.asList[Long](0, 1))
  }

  test("keyAs - keys") {
    // TODO SPARK-44449 make this long again when upcasting is in.
    // It is okay to cast from Long to Double, but not Long to Int.
    val values = spark
      .range(10)
      .groupByKey(v => (v % 2).toDouble)
      .keyAs[Double]
      .keys
      .collectAsList()
    assert(values == Arrays.asList[Double](0, 1))
  }

  test("groupByKey, keyAs - duplicates") {
    val session: SparkSession = spark
    import session.implicits._
    val result = spark
      .range(10)
      .as[Long]
      .groupByKey(id => K2(id % 2, id % 4))
      .keyAs[K1]
      .flatMapGroups((_, it) => Seq(it.toSeq.size))
      .collect()
    assert(result.sorted === Seq(2, 2, 3, 3))
  }

  test("groupByKey, keyAs, keys - duplicates") {
    val session: SparkSession = spark
    import session.implicits._
    val result = spark
      .range(10)
      .as[Long]
      .groupByKey(id => K2(id % 2, id % 4))
      .keyAs[K1]
      .keys
      .collect()
    assert(result.sortBy(_.a) === Seq(K1(0), K1(0), K1(1), K1(1)))
  }

  test("keyAs - flatGroupMap") {
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .keyAs[Double]
      .flatMapGroups((_, it) => Seq(it.toSeq.size))
      .collectAsList()
    assert(values == Arrays.asList[Int](5, 5))
  }

  test("keyAs mapValues - cogroup") {
    val grouped = spark
      .range(10)
      .groupByKey(v => v % 2)
      .keyAs[Double]
      .mapValues(v => v * 2)
    val otherGrouped = spark
      .range(10)
      .groupByKey(v => v / 2)
      .keyAs[Double]
      .mapValues(v => v * 2)
    val values = grouped
      .cogroup(otherGrouped) { (k, it, otherIt) =>
        Iterator(String.valueOf(k), it.mkString(",") + ";" + otherIt.mkString(","))
      }
      .collectAsList()

    assert(
      values == Arrays.asList[String](
        "0.0",
        "0,4,8,12,16;0,2",
        "1.0",
        "2,6,10,14,18;4,6",
        "2.0",
        ";8,10",
        "3.0",
        ";12,14",
        "4.0",
        ";16,18"))
  }

  test("mapValues - flatGroupMap") {
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .mapValues(v => v * 2)
      .flatMapGroups((_, it) => Seq(it.toSeq.sum))
      .collectAsList()
    assert(values == Arrays.asList[Long](40, 50))
  }

  test("mapValues - keys") {
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .mapValues(v => v * 2)
      .keys
      .collectAsList()
    assert(values == Arrays.asList[Long](0, 1))
  }

  test("flatMapSortedGroups") {
    val grouped = spark
      .range(10)
      .groupByKey(v => v % 2)
    val values = grouped
      .flatMapSortedGroups(desc("id")) { (g, iter) =>
        Iterator(String.valueOf(g), iter.mkString(","))
      }
      .collectAsList()

    assert(values == Arrays.asList[String]("0", "8,6,4,2,0", "1", "9,7,5,3,1"))

    // Star is not allowed as group sort column
    val message = intercept[AnalysisException] {
      grouped
        .flatMapSortedGroups(col("*")) { (g, iter) =>
          Iterator(String.valueOf(g), iter.mkString(","))
        }
        .collectAsList()
    }.getMessage
    assert(message.contains("Invalid usage of '*' in MapGroups"))
  }

  test("cogroup") {
    val grouped = spark
      .range(10)
      .groupByKey(v => v % 2)
    val otherGrouped = spark
      .range(10)
      .groupByKey(v => v / 2)
    val values = grouped
      .cogroup(otherGrouped) { (k, it, otherIt) =>
        Seq(it.toSeq.size + otherIt.size)
      }
      .collectAsList()

    assert(values == Arrays.asList[Int](7, 7, 2, 2, 2))
  }

  test("cogroupSorted") {
    val grouped = spark
      .range(10)
      .groupByKey(v => v % 2)
    val otherGrouped = spark
      .range(10)
      .groupByKey(v => v / 2)
    val values = grouped
      .cogroupSorted(otherGrouped)(desc("id"))(desc("id")) { (k, it, otherIt) =>
        Iterator(String.valueOf(k), it.mkString(",") + ";" + otherIt.mkString(","))
      }
      .collectAsList()

    assert(
      values == Arrays.asList[String](
        "0",
        "8,6,4,2,0;1,0",
        "1",
        "9,7,5,3,1;3,2",
        "2",
        ";5,4",
        "3",
        ";7,6",
        "4",
        ";9,8"))
  }

  test("agg, keyAs") {
    // TODO SPARK-44449 make this long again when upcasting is in.
    val ds = spark
      .range(10)
      .groupByKey(v => (v % 2).toDouble)
      .keyAs[Double]
      .agg(count("*"))

    checkDatasetUnorderly(ds, (0.0, 5L), (1.0, 5L))
  }

  test("typed aggregation: expr") {
    val session: SparkSession = spark
    import session.implicits._
    // TODO SPARK-44449 make this int again when upcasting is in.
    val ds = Seq(("a", 10L), ("a", 20L), ("b", 1L), ("b", 2L), ("c", 1L)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(sum("_2").as[Long]),
      ("a", 30L),
      ("b", 3L),
      ("c", 1L))
  }

  test("typed aggregation: expr, expr") {
    // TODO SPARK-44449 make this int again when upcasting is in.
    val ds = Seq(("a", 10L), ("a", 20L), ("b", 1L), ("b", 2L), ("c", 1L)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long]),
      ("a", 30L, 32L),
      ("b", 3L, 5L),
      ("c", 1L, 2L))
  }

  test("typed aggregation: expr, expr, expr") {
    // TODO SPARK-44449 make this int again when upcasting is in.
    val ds = Seq(("a", 10L), ("a", 20L), ("b", 1L), ("b", 2L), ("c", 1L)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long], count("*")),
      ("a", 30L, 32L, 2L),
      ("b", 3L, 5L, 2L),
      ("c", 1L, 2L, 1L))
  }

  test("typed aggregation: expr, expr, expr, expr") {
    // TODO SPARK-44449 make this int again when upcasting is in.
    val ds = Seq(("a", 10L), ("a", 20L), ("b", 1L), ("b", 2L), ("c", 1L)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1)
        .agg(
          sum("_2").as[Long],
          sum($"_2" + 1).as[Long],
          count("*").as[Long],
          avg("_2").as[Double]),
      ("a", 30L, 32L, 2L, 15.0),
      ("b", 3L, 5L, 2L, 1.5),
      ("c", 1L, 2L, 1L, 1.0))
  }

  test("typed aggregation: expr, expr, expr, expr, expr") {
    // TODO SPARK-44449 make this int again when upcasting is in.
    val ds = Seq(("a", 10L), ("a", 20L), ("b", 1L), ("b", 2L), ("c", 1L)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1)
        .agg(
          sum("_2").as[Long],
          sum($"_2" + 1).as[Long],
          count("*").as[Long],
          avg("_2").as[Double],
          countDistinct("*").as[Long]),
      ("a", 30L, 32L, 2L, 15.0, 2L),
      ("b", 3L, 5L, 2L, 1.5, 2L),
      ("c", 1L, 2L, 1L, 1.0, 1L))
  }

  test("typed aggregation: expr, expr, expr, expr, expr, expr") {
    // TODO SPARK-44449 make this int again when upcasting is in.
    val ds = Seq(("a", 10L), ("a", 20L), ("b", 1L), ("b", 2L), ("c", 1L)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1)
        .agg(
          sum("_2").as[Long],
          sum($"_2" + 1).as[Long],
          count("*").as[Long],
          avg("_2").as[Double],
          countDistinct("*").as[Long],
          max("_2").as[Long]),
      ("a", 30L, 32L, 2L, 15.0, 2L, 20L),
      ("b", 3L, 5L, 2L, 1.5, 2L, 2L),
      ("c", 1L, 2L, 1L, 1.0, 1L, 1L))
  }

  test("typed aggregation: expr, expr, expr, expr, expr, expr, expr") {
    // TODO SPARK-44449 make this int again when upcasting is in.
    val ds = Seq(("a", 10L), ("a", 20L), ("b", 1L), ("b", 2L), ("c", 1L)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1)
        .agg(
          sum("_2").as[Long],
          sum($"_2" + 1).as[Long],
          count("*").as[Long],
          avg("_2").as[Double],
          countDistinct("*").as[Long],
          max("_2").as[Long],
          min("_2").as[Long]),
      ("a", 30L, 32L, 2L, 15.0, 2L, 20L, 10L),
      ("b", 3L, 5L, 2L, 1.5, 2L, 2L, 1L),
      ("c", 1L, 2L, 1L, 1.0, 1L, 1L, 1L))
  }

  test("typed aggregation: expr, expr, expr, expr, expr, expr, expr, expr") {
    // TODO SPARK-44449 make this int again when upcasting is in.
    val ds = Seq(("a", 10L), ("a", 20L), ("b", 1L), ("b", 2L), ("c", 1L)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1)
        .agg(
          sum("_2").as[Long],
          sum($"_2" + 1).as[Long],
          count("*").as[Long],
          avg("_2").as[Double],
          countDistinct("*").as[Long],
          max("_2").as[Long],
          min("_2").as[Long],
          mean("_2").as[Double]),
      ("a", 30L, 32L, 2L, 15.0, 2L, 20L, 10L, 15.0),
      ("b", 3L, 5L, 2L, 1.5, 2L, 2L, 1L, 1.5),
      ("c", 1L, 2L, 1L, 1.0, 1L, 1L, 1L, 1.0))
  }

  test("SPARK-24762: Enable top-level Option of Product encoders") {
    val data = Seq(Some((1, "a")), Some((2, "b")), None)
    val ds = data.toDS()

    checkDataset(ds, data: _*)

    val schema = new StructType().add(
      "value",
      new StructType()
        .add("_1", IntegerType, nullable = false)
        .add("_2", StringType, nullable = true),
      nullable = true)

    assert(ds.schema == schema)

    val nestedOptData = Seq(Some((Some((1, "a")), 2.0)), Some((Some((2, "b")), 3.0)))
    val nestedDs = nestedOptData.toDS()

    checkDataset(nestedDs, nestedOptData: _*)

    val nestedSchema = StructType(
      Seq(StructField(
        "value",
        StructType(Seq(
          StructField(
            "_1",
            StructType(Seq(
              StructField("_1", IntegerType, nullable = false),
              StructField("_2", StringType, nullable = true)))),
          StructField("_2", DoubleType, nullable = false))),
        nullable = true)))
    assert(nestedDs.schema == nestedSchema)
  }

  test("SPARK-24762: Resolving Option[Product] field") {
    val ds = Seq((1, ("a", 1.0)), (2, ("b", 2.0)), (3, null))
      .toDS()
      .as[(Int, Option[(String, Double)])]
    checkDataset(ds, (1, Some(("a", 1.0))), (2, Some(("b", 2.0))), (3, None))
  }

  test("SPARK-24762: select Option[Product] field") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    val ds1 = ds.select(expr("struct(_2, _2 + 1)").as[Option[(Int, Int)]])
    checkDataset(ds1, Some((1, 2)), Some((2, 3)), Some((3, 4)))

    val ds2 = ds.select(expr("if(_2 > 2, struct(_2, _2 + 1), null)").as[Option[(Int, Int)]])
    checkDataset(ds2, None, None, Some((3, 4)))
  }

  test("SPARK-24762: typed agg on Option[Product] type") {
    val ds = Seq(Some((1, 2)), Some((2, 3)), Some((1, 3))).toDS()
    assert(ds.groupByKey(_.get._1).count().collect() === Seq((1, 2), (2, 1)))

    assert(
      ds.groupByKey(x => x).count().collect() ===
        Seq((Some((1, 2)), 1), (Some((2, 3)), 1), (Some((1, 3)), 1)))
  }

  test("SPARK-25942: typed aggregation on primitive type") {
    val ds = Seq(1, 2, 3).toDS()

    val agg = ds
      .groupByKey(_ >= 2)
      .agg(sum("value").as[Long], sum($"value" + 1).as[Long])
    checkDatasetUnorderly(agg, (false, 1L, 2L), (true, 5L, 7L))
  }

  test("SPARK-25942: typed aggregation on product type") {
    val ds = Seq((1, 2), (2, 3), (3, 4)).toDS()
    val agg = ds.groupByKey(x => x).agg(sum("_1").as[Long], sum($"_2" + 1).as[Long])
    checkDatasetUnorderly(agg, ((1, 2), 1L, 3L), ((2, 3), 2L, 4L), ((3, 4), 3L, 5L))
  }

  test("SPARK-26085: fix key attribute name for atomic type for typed aggregation") {
    val ds = Seq(1, 2, 3).toDS()
    assert(ds.groupByKey(x => x).count().schema.head.name == "key")

    // Enable legacy flag to follow previous Spark behavior
    withSQLConf("spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue" -> "true") {
      assert(ds.groupByKey(x => x).count().schema.head.name == "value")
    }
  }

  test("reduceGroups") {
    val ds = Seq("abc", "xyz", "hello").toDS()
    checkDatasetUnorderly(
      ds.groupByKey(_.length).reduceGroups(_ + _),
      (3, "abcxyz"),
      (5, "hello"))
  }

  test("groupby") {
    val ds = Seq(("a", 1, 10), ("a", 2, 20), ("b", 2, 1), ("b", 1, 2), ("c", 1, 1))
      .toDF("key", "seq", "value")
    val grouped = ds.groupBy($"key").as[String, (String, Int, Int)]
    val aggregated = grouped
      .flatMapSortedGroups($"seq", expr("length(key)"), $"value") { (g, iter) =>
        Iterator(g, iter.mkString(", "))
      }

    checkDatasetUnorderly(
      aggregated,
      "a",
      "(a,1,10), (a,2,20)",
      "b",
      "(b,1,2), (b,2,1)",
      "c",
      "(c,1,1)")
  }

  test("SPARK-50693: groupby on unresolved plan") {
    val ds = Seq(("a", 1, 10), ("a", 2, 20), ("b", 2, 1), ("b", 1, 2), ("c", 1, 1))
      .toDF("key", "seq", "value")
    val grouped = ds.select("*").groupBy($"key").as[String, (String, Int, Int)]
    val aggregated = grouped
      .flatMapSortedGroups($"seq", expr("length(key)"), $"value") { (g, iter) =>
        Iterator(g, iter.mkString(", "))
      }

    checkDatasetUnorderly(
      aggregated,
      "a",
      "(a,1,10), (a,2,20)",
      "b",
      "(b,1,2), (b,2,1)",
      "c",
      "(c,1,1)")
  }

  test("groupby - keyAs, keys") {
    val ds = Seq(("a", 1, 10), ("a", 2, 20), ("b", 2, 1), ("b", 1, 2), ("c", 1, 1))
      .toDF("key", "seq", "value")
    val grouped = ds.groupBy($"value").as[String, (String, Int, Int)]
    val keys = grouped.keyAs[String].keys.sort($"value")
    checkDataset(keys, "1", "2", "10", "20")
  }

  test("flatMapGroupsWithState") {
    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        if (state.exists) throw new IllegalArgumentException("state.exists should be false")
        Iterator(ClickState(key, values.size))
      }

    val session: SparkSession = spark
    import session.implicits._
    val values = Seq(
      ClickEvent("a", new Timestamp(12345)),
      ClickEvent("a", new Timestamp(12346)),
      ClickEvent("a", new Timestamp(12347)),
      ClickEvent("b", new Timestamp(12348)),
      ClickEvent("b", new Timestamp(12349)),
      ClickEvent("c", new Timestamp(12350)))
      .toDS()
      .groupByKey(_.id)
      .flatMapGroupsWithState(Append, GroupStateTimeout.NoTimeout)(stateFunc)

    checkDataset(values, ClickState("a", 3), ClickState("b", 2), ClickState("c", 1))
  }

  test("flatMapGroupsWithState - with initial state") {
    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        val currState = state.getOption.getOrElse(ClickState(key, 0))
        Iterator(ClickState(key, currState.count + values.size))
      }

    val session: SparkSession = spark
    import session.implicits._

    val initialStateDS = Seq(ClickState("a", 2), ClickState("b", 1)).toDS()
    val initialState = initialStateDS.groupByKey(_.id).mapValues(x => x)

    val values = Seq(
      ClickEvent("a", new Timestamp(12345)),
      ClickEvent("a", new Timestamp(12346)),
      ClickEvent("a", new Timestamp(12347)),
      ClickEvent("b", new Timestamp(12348)),
      ClickEvent("b", new Timestamp(12349)),
      ClickEvent("c", new Timestamp(12350)))
      .toDS()
      .groupByKey(_.id)
      .flatMapGroupsWithState(Append, GroupStateTimeout.NoTimeout, initialState)(stateFunc)

    checkDataset(values, ClickState("a", 5), ClickState("b", 3), ClickState("c", 1))
  }

  test("mapGroupsWithState") {
    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        if (state.exists) throw new IllegalArgumentException("state.exists should be false")
        ClickState(key, values.size)
      }

    val session: SparkSession = spark
    import session.implicits._
    val values = Seq(
      ClickEvent("a", new Timestamp(12345)),
      ClickEvent("a", new Timestamp(12346)),
      ClickEvent("a", new Timestamp(12347)),
      ClickEvent("b", new Timestamp(12348)),
      ClickEvent("b", new Timestamp(12349)),
      ClickEvent("c", new Timestamp(12350)))
      .toDS()
      .groupByKey(_.id)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(stateFunc)

    checkDataset(values, ClickState("a", 3), ClickState("b", 2), ClickState("c", 1))
  }

  test("mapGroupsWithState - with initial state") {
    val stateFunc =
      (key: String, values: Iterator[ClickEvent], state: GroupState[ClickState]) => {
        val currState = state.getOption.getOrElse(ClickState(key, 0))
        ClickState(key, currState.count + values.size)
      }

    val session: SparkSession = spark
    import session.implicits._

    val initialStateDS = Seq(ClickState("a", 2), ClickState("b", 1)).toDS()
    val initialState = initialStateDS.groupByKey(_.id).mapValues(x => x)

    val values = Seq(
      ClickEvent("a", new Timestamp(12345)),
      ClickEvent("a", new Timestamp(12346)),
      ClickEvent("a", new Timestamp(12347)),
      ClickEvent("b", new Timestamp(12348)),
      ClickEvent("b", new Timestamp(12349)),
      ClickEvent("c", new Timestamp(12350)))
      .toDS()
      .groupByKey(_.id)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout, initialState)(stateFunc)

    checkDataset(values, ClickState("a", 5), ClickState("b", 3), ClickState("c", 1))
  }

  test("RowEncoder in udf") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("c1", "c2")

    checkDatasetUnorderly(
      ds.groupByKey(k => k.getAs[String](0)).agg(sum("c2").as[Long]),
      ("a", 30L),
      ("b", 3L),
      ("c", 1L))
  }

  test("SPARK-50693: RowEncoder in udf on unresolved plan") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("c1", "c2")

    checkDatasetUnorderly(
      ds.select("*").groupByKey(k => k.getAs[String](0)).agg(sum("c2").as[Long]),
      ("a", 30L),
      ("b", 3L),
      ("c", 1L))
  }

  test("mapGroups with row encoder") {
    val df = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("c1", "c2")

    checkDataset(
      df.groupByKey(r => r.getAs[String]("c1"))
        .mapGroups((_, it) =>
          it.map(r => {
            r.getAs[Int]("c2")
          }).sum),
      30,
      3,
      1)
  }

  test("SPARK-50693: mapGroups with row encoder on unresolved plan") {
    val df = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("c1", "c2")

    checkDataset(
      df.select("*")
        .groupByKey(r => r.getAs[String]("c1"))
        .mapGroups((_, it) =>
          it.map(r => {
            r.getAs[Int]("c2")
          }).sum),
      30,
      3,
      1)
  }

  test("coGroup with row encoder") {
    val df1 = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("c1", "c2")
    val df2 = Seq(("x", 10), ("x", 20), ("y", 1), ("y", 2), ("a", 1)).toDF("c1", "c2")

    val ds1: KeyValueGroupedDataset[String, Row] =
      df1.groupByKey(r => r.getAs[String]("c1"))
    val ds2: KeyValueGroupedDataset[String, Row] =
      df2.groupByKey(r => r.getAs[String]("c1"))
    checkDataset(
      ds1.cogroup(ds2)((_, it, it2) => {
        val sum1 = it.map(r => r.getAs[Int]("c2")).sum
        val sum2 = it2.map(r => r.getAs[Int]("c2")).sum
        Iterator(sum1 + sum2)
      }),
      31,
      3,
      1,
      30,
      3)
  }

  test("SPARK-50693: coGroup with row encoder on unresolved plan") {
    val df1 = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDF("c1", "c2")
    val df2 = Seq(("x", 10), ("x", 20), ("y", 1), ("y", 2), ("a", 1)).toDF("c1", "c2")

    Seq((df1.select("*"), df2), (df1, df2.select("*")), (df1.select("*"), df2.select("*")))
      .foreach { case (df1, df2) =>
        val ds1: KeyValueGroupedDataset[String, Row] =
          df1.groupByKey(r => r.getAs[String]("c1"))
        val ds2: KeyValueGroupedDataset[String, Row] =
          df2.groupByKey(r => r.getAs[String]("c1"))
        checkDataset(
          ds1.cogroup(ds2)((_, it, it2) => {
            val sum1 = it.map(r => r.getAs[Int]("c2")).sum
            val sum2 = it2.map(r => r.getAs[Int]("c2")).sum
            Iterator(sum1 + sum2)
          }),
          31,
          3,
          1,
          30,
          3)
      }
  }

  test("serialize as null") {
    val kvgds = session.range(10).groupByKey(_ % 2)
    val bytes = SparkSerDeUtils.serialize(kvgds)
    assert(SparkSerDeUtils.deserialize[KeyValueGroupedDataset[Long, Long]](bytes) == null)
  }
}

case class K1(a: Long)
case class K2(a: Long, b: Long)

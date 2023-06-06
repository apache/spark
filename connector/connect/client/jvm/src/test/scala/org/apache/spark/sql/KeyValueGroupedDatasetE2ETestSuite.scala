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

import java.util.Arrays

import io.grpc.StatusRuntimeException

import org.apache.spark.sql.connect.client.util.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * All tests in this class requires client UDF artifacts synced with the server.
 */
class KeyValueGroupedDatasetE2ETestSuite extends QueryTest with SQLHelper {

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
    // It is okay to cast from Long to Double, but not Long to Int.
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .keyAs[Double]
      .keys
      .collectAsList()
    assert(values == Arrays.asList[Double](0, 1))
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
    val message = intercept[StatusRuntimeException] {
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
        Seq(it.toSeq.size + otherIt.seq.size)
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
    val ds = spark
      .range(10)
      .groupByKey(v => v % 2)
      .keyAs[Double]
      .agg(count("*"))

    checkDatasetUnorderly(ds, (0.0, 5L), (1.0, 5L))
  }

  test("typed aggregation: expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(sum("_2").as[Long]),
      ("a", 30L),
      ("b", 3L),
      ("c", 1L))
  }

  test("typed aggregation: expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long]),
      ("a", 30L, 32L),
      ("b", 3L, 5L),
      ("c", 1L, 2L))
  }

  test("typed aggregation: expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long], count("*")),
      ("a", 30L, 32L, 2L),
      ("b", 3L, 5L, 2L),
      ("c", 1L, 2L, 1L))
  }

  test("typed aggregation: expr, expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

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
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

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
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

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
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

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
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

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
    // TODO(SPARK-43416): Recursively rename the position based tuple to the schema name from the
    //  server.
    val ds = Seq(1, 2, 3).toDS()
    assert(ds.groupByKey(x => x).count().schema.head.name == "_1")

    // Enable legacy flag to follow previous Spark behavior
    withSQLConf("spark.sql.legacy.dataset.nameNonStructGroupingKeyAsValue" -> "true") {
      assert(ds.groupByKey(x => x).count().schema.head.name == "_1")
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

  test("groupby - keyAs, keys") {
    val ds = Seq(("a", 1, 10), ("a", 2, 20), ("b", 2, 1), ("b", 1, 2), ("c", 1, 1))
      .toDF("key", "seq", "value")
    val grouped = ds.groupBy($"value").as[String, (String, Int, Int)]
    val keys = grouped.keyAs[String].keys.sort($"value")

    checkDataset(keys, "1", "2", "10", "20")
  }
}

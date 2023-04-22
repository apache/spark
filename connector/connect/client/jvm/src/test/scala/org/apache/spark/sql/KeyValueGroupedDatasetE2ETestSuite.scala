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

import org.apache.spark.sql.connect.client.util.RemoteSparkSession

/**
 * All tests in this class requires client UDF artifacts synced with the server. TODO: It means
 * these tests only works with SBT for now.
 */
class KeyValueGroupedDatasetE2ETestSuite extends RemoteSparkSession {

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
    val session: SparkSession = spark
    import session.implicits._
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .flatMapGroups((_, it) => Seq(it.toSeq.size))
      .collectAsList()
    assert(values == Arrays.asList[Int](5, 5))
  }

  test("keys") {
    val session: SparkSession = spark
    import session.implicits._
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .keys
      .collectAsList()
    assert(values == Arrays.asList[Long](0, 1))
  }

  test("keyAs - keys") {
    val session: SparkSession = spark
    import session.implicits._
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
    val session: SparkSession = spark
    import session.implicits._
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .keyAs[Double]
      .flatMapGroups((_, it) => Seq(it.toSeq.size))
      .collectAsList()
    assert(values == Arrays.asList[Int](5, 5))
  }

  test("keyAs mapValues - cogroup") {
    val session: SparkSession = spark
    import session.implicits._
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
    val session: SparkSession = spark
    import session.implicits._
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .mapValues(v => v * 2)
      .flatMapGroups((_, it) => Seq(it.toSeq.sum))
      .collectAsList()
    assert(values == Arrays.asList[Long](40, 50))
  }

  test("mapValues - keys") {
    val session: SparkSession = spark
    import session.implicits._
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .mapValues(v => v * 2)
      .keys
      .collectAsList()
    assert(values == Arrays.asList[Long](0, 1))
  }

  test("flatMapSortedGroups") {
    val session: SparkSession = spark
    import session.implicits._
    val grouped = spark
      .range(10)
      .groupByKey(v => v % 2)
    val values = grouped
      .flatMapSortedGroups(functions.desc("id")) { (g, iter) =>
        Iterator(String.valueOf(g), iter.mkString(","))
      }
      .collectAsList()

    assert(values == Arrays.asList[String]("0", "8,6,4,2,0", "1", "9,7,5,3,1"))

    // Star is not allowed as group sort column
    val message = intercept[StatusRuntimeException] {
      grouped
        .flatMapSortedGroups(functions.col("*")) { (g, iter) =>
          Iterator(String.valueOf(g), iter.mkString(","))
        }
        .collectAsList()
    }.getMessage
    assert(message.contains("Invalid usage of '*' in MapGroups"))
  }

  test("cogroup") {
    val session: SparkSession = spark
    import session.implicits._
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
    val session: SparkSession = spark
    import session.implicits._
    val grouped = spark
      .range(10)
      .groupByKey(v => v % 2)
    val otherGrouped = spark
      .range(10)
      .groupByKey(v => v / 2)
    val values = grouped
      .cogroupSorted(otherGrouped)(functions.desc("id"))(functions.desc("id")) {
        (k, it, otherIt) =>
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

  test("agg") {
    val session: SparkSession = spark
    import session.implicits._
    val values = spark
      .range(10)
      .groupByKey(v => v % 2)
      .keyAs[Double]
      .agg(functions.count("*"))
      .collectAsList()

    assert(values == Arrays.asList[(Long, Double)]((0, 5), (1, 5)))
  }

  test("reduceGroups") {
    val session: SparkSession = spark
    import session.implicits._
    val ds = Seq("abc", "xyz", "hello").toDS()
    val values = ds.groupByKey(_.length).reduceGroups(_ + _).collectAsList()

    assert(values == Arrays.asList[(Int, String)]((3, "abcxyz"), (5, "hello")))
  }

  test("groupby") {
    val session: SparkSession = spark
    import session.implicits._
    val ds = Seq(("a", 1, 10), ("a", 2, 20), ("b", 2, 1), ("b", 1, 2), ("c", 1, 1))
      .toDF("key", "seq", "value")
    val grouped = ds.groupBy($"key").as[String, (String, Int, Int)]
    val aggregated = grouped.flatMapSortedGroups(
      $"seq", functions.expr("length(key)"), $"value") {
      (g, iter) => Iterator(g, iter.mkString(", "))
    }.collectAsList()

    assert(aggregated == Arrays.asList[String](
      "a", "(a,1,10), (a,2,20)",
      "b", "(b,2,1), (b,1,2)",
      "c", "(c,1,1)"))
  }

  test("groupby - keyAs, keys") {
    val session: SparkSession = spark
    import session.implicits._
    val ds = Seq(("a", 1, 10), ("a", 2, 20), ("b", 2, 1), ("b", 1, 2), ("c", 1, 1))
      .toDF("key", "seq", "value")
    val grouped = ds.groupBy($"value").as[String, (String, Int, Int)]
    val keys = grouped.keyAs[String].keys.collectAsList()

    assert(keys == Arrays.asList[String]("10", "20", "1", "2"))
  }
}

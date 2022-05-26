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

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.sql.{Date, Timestamp}

import org.scalatest.Assertions._
import org.scalatest.exceptions.TestFailedException
import org.scalatest.prop.TableDrivenPropertyChecks._

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.TestUtils.withListener
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.{FooClassWithEnum, FooEnum, ScroogeLikeExample}
import org.apache.spark.sql.catalyst.encoders.{OuterScopes, RowEncoder}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.execution.{LogicalRDD, RDDScanExec, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

case class TestDataPoint(x: Int, y: Double, s: String, t: TestDataPoint2)
case class TestDataPoint2(x: Int, s: String)

object TestForTypeAlias {
  type TwoInt = (Int, Int)
  type ThreeInt = (TwoInt, Int)
  type SeqOfTwoInt = Seq[TwoInt]
  type IntArray = Array[Int]

  def tupleTypeAlias: TwoInt = (1, 1)
  def nestedTupleTypeAlias: ThreeInt = ((1, 1), 2)
  def seqOfTupleTypeAlias: SeqOfTwoInt = Seq((1, 1), (2, 2))
  def aliasedArrayInTuple: (Int, IntArray) = (1, Array(1))
}

class DatasetSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  private implicit val ordering = Ordering.by((c: ClassData) => c.a -> c.b)

  test("checkAnswer should compare map correctly") {
    val data = Seq((1, "2", Map(1 -> 2, 2 -> 1)))
    checkAnswer(
      data.toDF(),
      Seq(Row(1, "2", Map(2 -> 1, 1 -> 2))))
  }

  test("toDS") {
    val data = Seq(("a", 1), ("b", 2), ("c", 3))
    checkDataset(
      data.toDS(),
      data: _*)
  }

  test("toDS should compare map with byte array keys correctly") {
    // Choose the order of arrays in such way, that sorting keys of different maps by _.toString
    // will not incidentally put equal keys together.
    val arrays = (1 to 5).map(_ => Array[Byte](0.toByte, 0.toByte)).sortBy(_.toString).toArray
    arrays(0)(1) = 1.toByte
    arrays(1)(1) = 2.toByte
    arrays(2)(1) = 2.toByte
    arrays(3)(1) = 1.toByte

    val mapA = Map(arrays(0) -> "one", arrays(2) -> "two")
    val subsetOfA = Map(arrays(0) -> "one")
    val equalToA = Map(arrays(1) -> "two", arrays(3) -> "one")
    val notEqualToA1 = Map(arrays(1) -> "two", arrays(3) -> "not one")
    val notEqualToA2 = Map(arrays(1) -> "two", arrays(4) -> "one")

    // Comparing map with itself
    checkDataset(Seq(mapA).toDS(), mapA)

    // Comparing map with equivalent map
    checkDataset(Seq(equalToA).toDS(), mapA)
    checkDataset(Seq(mapA).toDS(), equalToA)

    // Comparing map with it's subset
    intercept[TestFailedException](checkDataset(Seq(subsetOfA).toDS(), mapA))
    intercept[TestFailedException](checkDataset(Seq(mapA).toDS(), subsetOfA))

    // Comparing map with another map differing by single value
    intercept[TestFailedException](checkDataset(Seq(notEqualToA1).toDS(), mapA))
    intercept[TestFailedException](checkDataset(Seq(mapA).toDS(), notEqualToA1))

    // Comparing map with another map differing by single key
    intercept[TestFailedException](checkDataset(Seq(notEqualToA2).toDS(), mapA))
    intercept[TestFailedException](checkDataset(Seq(mapA).toDS(), notEqualToA2))
  }

  test("toDS with RDD") {
    val ds = sparkContext.makeRDD(Seq("a", "b", "c"), 3).toDS()
    checkDataset(
      ds.mapPartitions(_ => Iterator(1)),
      1, 1, 1)
  }

  test("emptyDataset") {
    val ds = spark.emptyDataset[Int]
    assert(ds.count() == 0L)
    assert(ds.collect() sameElements Array.empty[Int])
  }

  test("range") {
    assert(spark.range(10).map(_ + 1).reduce(_ + _) == 55)
    assert(spark.range(10).map{ case i: java.lang.Long => i + 1 }.reduce(_ + _) == 55)
    assert(spark.range(0, 10).map(_ + 1).reduce(_ + _) == 55)
    assert(spark.range(0, 10).map{ case i: java.lang.Long => i + 1 }.reduce(_ + _) == 55)
    assert(spark.range(0, 10, 1, 2).map(_ + 1).reduce(_ + _) == 55)
    assert(spark.range(0, 10, 1, 2).map{ case i: java.lang.Long => i + 1 }.reduce(_ + _) == 55)
  }

  test("SPARK-12404: Datatype Helper Serializability") {
    val ds = sparkContext.parallelize((
      new Timestamp(0),
      new Date(0),
      java.math.BigDecimal.valueOf(1),
      scala.math.BigDecimal(1)) :: Nil).toDS()

    ds.collect()
  }

  test("collect, first, and take should use encoders for serialization") {
    val item = NonSerializableCaseClass("abcd")
    val ds = Seq(item).toDS()
    assert(ds.collect().head == item)
    assert(ds.collectAsList().get(0) == item)
    assert(ds.first() == item)
    assert(ds.take(1).head == item)
    assert(ds.takeAsList(1).get(0) == item)
    assert(ds.toLocalIterator().next() === item)
  }

  test("coalesce, repartition") {
    val data = (1 to 100).map(i => ClassData(i.toString, i))
    val ds = data.toDS()

    intercept[IllegalArgumentException] {
      ds.coalesce(0)
    }

    intercept[IllegalArgumentException] {
      ds.repartition(0)
    }

    assert(ds.repartition(10).rdd.partitions.length == 10)
    checkDatasetUnorderly(
      ds.repartition(10),
      data: _*)

    assert(ds.coalesce(1).rdd.partitions.length == 1)
    checkDatasetUnorderly(
      ds.coalesce(1),
      data: _*)
  }

  test("as tuple") {
    val data = Seq(("a", 1), ("b", 2)).toDF("a", "b")
    checkDataset(
      data.as[(String, Int)],
      ("a", 1), ("b", 2))
  }

  test("as case class / collect") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("a", "b").as[ClassData]
    checkDataset(
      ds,
      ClassData("a", 1), ClassData("b", 2), ClassData("c", 3))
    assert(ds.collect().head == ClassData("a", 1))
  }

  test("as case class - reordered fields by name") {
    val ds = Seq((1, "a"), (2, "b"), (3, "c")).toDF("b", "a").as[ClassData]
    assert(ds.collect() === Array(ClassData("a", 1), ClassData("b", 2), ClassData("c", 3)))
  }

  test("as case class - take") {
    val ds = Seq((1, "a"), (2, "b"), (3, "c")).toDF("b", "a").as[ClassData]
    assert(ds.take(2) === Array(ClassData("a", 1), ClassData("b", 2)))
  }

  test("as case class - tail") {
    val ds = Seq((1, "a"), (2, "b"), (3, "c")).toDF("b", "a").as[ClassData]
    assert(ds.tail(2) === Array(ClassData("b", 2), ClassData("c", 3)))
  }

  test("as seq of case class - reorder fields by name") {
    val df = spark.range(3).select(array(struct($"id".cast("int").as("b"), lit("a").as("a"))))
    val ds = df.as[Seq[ClassData]]
    assert(ds.collect() === Array(
      Seq(ClassData("a", 0)),
      Seq(ClassData("a", 1)),
      Seq(ClassData("a", 2))))
  }

  test("as map of case class - reorder fields by name") {
    val df = spark.range(3).select(map(lit(1), struct($"id".cast("int").as("b"), lit("a").as("a"))))
    val ds = df.as[Map[Int, ClassData]]
    assert(ds.collect() === Array(
      Map(1 -> ClassData("a", 0)),
      Map(1 -> ClassData("a", 1)),
      Map(1 -> ClassData("a", 2))))
  }

  test("map") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    checkDataset(
      ds.map(v => (v._1, v._2 + 1)),
      ("a", 2), ("b", 3), ("c", 4))
  }

  test("map with type change with the exact matched number of attributes") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()

    checkDataset(
      ds.map(identity[(String, Int)])
        .as[OtherTuple]
        .map(identity[OtherTuple]),
      OtherTuple("a", 1), OtherTuple("b", 2), OtherTuple("c", 3))
  }

  test("map with type change with less attributes") {
    val ds = Seq(("a", 1, 3), ("b", 2, 4), ("c", 3, 5)).toDS()

    checkDataset(
      ds.as[OtherTuple]
        .map(identity[OtherTuple]),
      OtherTuple("a", 1), OtherTuple("b", 2), OtherTuple("c", 3))
  }

  test("map and group by with class data") {
    // We inject a group by here to make sure this test case is future proof
    // when we implement better pipelining and local execution mode.
    val ds: Dataset[(ClassData, Long)] = Seq(ClassData("one", 1), ClassData("two", 2)).toDS()
        .map(c => ClassData(c.a, c.b + 1))
        .groupByKey(p => p).count()

    checkDatasetUnorderly(
      ds,
      (ClassData("one", 2), 1L), (ClassData("two", 3), 1L))
  }

  test("select") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    checkDataset(
      ds.select(expr("_2 + 1").as[Int]),
      2, 3, 4)
  }

  test("SPARK-16853: select, case class and tuple") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    checkDataset(
      ds.select(expr("struct(_2, _2)").as[(Int, Int)]): Dataset[(Int, Int)],
      (1, 1), (2, 2), (3, 3))

    checkDataset(
      ds.select(expr("named_struct('a', _1, 'b', _2)").as[ClassData]): Dataset[ClassData],
      ClassData("a", 1), ClassData("b", 2), ClassData("c", 3))
  }

  test("select 2") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    checkDataset(
      ds.select(
        expr("_1").as[String],
        expr("_2").as[Int]) : Dataset[(String, Int)],
      ("a", 1), ("b", 2), ("c", 3))
  }

  test("select 2, primitive and tuple") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    checkDataset(
      ds.select(
        expr("_1").as[String],
        expr("struct(_2, _2)").as[(Int, Int)]),
      ("a", (1, 1)), ("b", (2, 2)), ("c", (3, 3)))
  }

  test("select 2, primitive and class") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    checkDataset(
      ds.select(
        expr("_1").as[String],
        expr("named_struct('a', _1, 'b', _2)").as[ClassData]),
      ("a", ClassData("a", 1)), ("b", ClassData("b", 2)), ("c", ClassData("c", 3)))
  }

  test("select 2, primitive and class, fields reordered") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    checkDataset(
      ds.select(
        expr("_1").as[String],
        expr("named_struct('b', _2, 'a', _1)").as[ClassData]),
      ("a", ClassData("a", 1)), ("b", ClassData("b", 2)), ("c", ClassData("c", 3)))
  }

  test("REGEX column specification") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()

    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
      var e = intercept[AnalysisException] {
        ds.select(expr("`(_1)?+.+`").as[Int])
      }
      assert(e.getErrorClass == "MISSING_COLUMN")
      assert(e.messageParameters.head == "`(_1)?+.+`")

      e = intercept[AnalysisException] {
        ds.select(expr("`(_1|_2)`").as[Int])
      }
      assert(e.getErrorClass == "MISSING_COLUMN")
      assert(e.messageParameters.head == "`(_1|_2)`")

      e = intercept[AnalysisException] {
        ds.select(ds("`(_1)?+.+`"))
      }
      assert(e.getMessage.contains("Cannot resolve column name \"`(_1)?+.+`\""))

      e = intercept[AnalysisException] {
        ds.select(ds("`(_1|_2)`"))
      }
      assert(e.getMessage.contains("Cannot resolve column name \"`(_1|_2)`\""))
    }

    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "true") {
      checkDataset(
        ds.select(ds.col("_2")).as[Int],
        1, 2, 3)

      checkDataset(
        ds.select(ds.colRegex("`(_1)?+.+`")).as[Int],
        1, 2, 3)

      checkDataset(
        ds.select(ds("`(_1|_2)`"))
          .select(expr("named_struct('a', _1, 'b', _2)").as[ClassData]),
        ClassData("a", 1), ClassData("b", 2), ClassData("c", 3))

      checkDataset(
        ds.alias("g")
          .select(ds("g.`(_1|_2)`"))
          .select(expr("named_struct('a', _1, 'b', _2)").as[ClassData]),
        ClassData("a", 1), ClassData("b", 2), ClassData("c", 3))

      checkDataset(
        ds.select(ds("`(_1)?+.+`"))
          .select(expr("_2").as[Int]),
        1, 2, 3)

      checkDataset(
        ds.alias("g")
          .select(ds("g.`(_1)?+.+`"))
          .select(expr("_2").as[Int]),
        1, 2, 3)

      checkDataset(
        ds.select(expr("`(_1)?+.+`").as[Int]),
        1, 2, 3)
      val m = ds.select(expr("`(_1|_2)`"))

      checkDataset(
        ds.select(expr("`(_1|_2)`"))
          .select(expr("named_struct('a', _1, 'b', _2)").as[ClassData]),
        ClassData("a", 1), ClassData("b", 2), ClassData("c", 3))

      checkDataset(
        ds.alias("g")
          .select(expr("g.`(_1)?+.+`").as[Int]),
        1, 2, 3)

      checkDataset(
        ds.alias("g")
          .select(expr("g.`(_1|_2)`"))
          .select(expr("named_struct('a', _1, 'b', _2)").as[ClassData]),
        ClassData("a", 1), ClassData("b", 2), ClassData("c", 3))
    }
  }

  test("filter") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    checkDataset(
      ds.filter(_._1 == "b"),
      ("b", 2))
  }

  test("filter and then select") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    checkDataset(
      ds.filter(_._1 == "b").select(expr("_1").as[String]),
      "b")
  }

  test("SPARK-15632: typed filter should preserve the underlying logical schema") {
    val ds = spark.range(10)
    val ds2 = ds.filter(_ > 3)
    assert(ds.schema.equals(ds2.schema))
  }

  test("foreach") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    val acc = sparkContext.longAccumulator
    ds.foreach(v => acc.add(v._2))
    assert(acc.value == 6)
  }

  test("foreachPartition") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    val acc = sparkContext.longAccumulator
    ds.foreachPartition((it: Iterator[(String, Int)]) => it.foreach(v => acc.add(v._2)))
    assert(acc.value == 6)
  }

  test("reduce") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    assert(ds.reduce((a, b) => ("sum", a._2 + b._2)) == (("sum", 6)))
  }

  test("joinWith, flat schema") {
    val ds1 = Seq(1, 2, 3).toDS().as("a")
    val ds2 = Seq(1, 2).toDS().as("b")

    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value", "inner")

    val expectedSchema = StructType(Seq(
      StructField("_1", IntegerType, nullable = false),
      StructField("_2", IntegerType, nullable = false)
    ))

    assert(joined.schema === expectedSchema)

    checkDataset(
      joined,
      (1, 1), (2, 2))
  }

  test("joinWith tuple with primitive, expression") {
    val ds1 = Seq(1, 1, 2).toDS()
    val ds2 = Seq(("a", 1), ("b", 2)).toDS()

    val joined = ds1.joinWith(ds2, $"value" === $"_2")

    // This is an inner join, so both outputs fields are non-nullable
    val expectedSchema = StructType(Seq(
      StructField("_1", IntegerType, nullable = false),
      StructField("_2",
        StructType(Seq(
          StructField("_1", StringType),
          StructField("_2", IntegerType, nullable = false)
        )), nullable = false)
    ))
    assert(joined.schema === expectedSchema)

    checkDataset(
      joined,
      (1, ("a", 1)), (1, ("a", 1)), (2, ("b", 2)))
  }

  test("joinWith class with primitive, toDF") {
    val ds1 = Seq(1, 1, 2).toDS()
    val ds2 = Seq(ClassData("a", 1), ClassData("b", 2)).toDS()

    checkAnswer(
      ds1.joinWith(ds2, $"value" === $"b").toDF().select($"_1", $"_2.a", $"_2.b"),
      Row(1, "a", 1) :: Row(1, "a", 1) :: Row(2, "b", 2) :: Nil)
  }

  test("multi-level joinWith") {
    val ds1 = Seq(("a", 1), ("b", 2)).toDS().as("a")
    val ds2 = Seq(("a", 1), ("b", 2)).toDS().as("b")
    val ds3 = Seq(("a", 1), ("b", 2)).toDS().as("c")

    checkDataset(
      ds1.joinWith(ds2, $"a._2" === $"b._2").as("ab").joinWith(ds3, $"ab._1._2" === $"c._2"),
      ((("a", 1), ("a", 1)), ("a", 1)),
      ((("b", 2), ("b", 2)), ("b", 2)))
  }

  test("joinWith join types") {
    val ds1 = Seq(1, 2, 3).toDS().as("a")
    val ds2 = Seq(1, 2).toDS().as("b")

    val e1 = intercept[AnalysisException] {
      ds1.joinWith(ds2, $"a.value" === $"b.value", "left_semi")
    }.getMessage
    assert(e1.contains("Invalid join type in joinWith: " + LeftSemi.sql))

    val e2 = intercept[AnalysisException] {
      ds1.joinWith(ds2, $"a.value" === $"b.value", "semi")
    }.getMessage
    assert(e2.contains("Invalid join type in joinWith: " + LeftSemi.sql))

    val e3 = intercept[AnalysisException] {
      ds1.joinWith(ds2, $"a.value" === $"b.value", "left_anti")
    }.getMessage
    assert(e3.contains("Invalid join type in joinWith: " + LeftAnti.sql))

    val e4 = intercept[AnalysisException] {
      ds1.joinWith(ds2, $"a.value" === $"b.value", "anti")
    }.getMessage
    assert(e4.contains("Invalid join type in joinWith: " + LeftAnti.sql))
  }

  test("groupBy function, keys") {
    val ds = Seq(("a", 1), ("b", 1)).toDS()
    val grouped = ds.groupByKey(v => (1, v._2))
    checkDatasetUnorderly(
      grouped.keys,
      (1, 1))
  }

  test("groupBy function, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupByKey(v => (v._1, "word"))
    val aggregated = grouped.mapGroups { (g, iter) => (g._1, iter.map(_._2).sum) }

    checkDatasetUnorderly(
      aggregated,
      ("a", 30), ("b", 3), ("c", 1))
  }

  test("groupBy function, flatMap") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupByKey(v => (v._1, "word"))
    val aggregated = grouped.flatMapGroups { (g, iter) =>
      Iterator(g._1, iter.map(_._2).sum.toString)
    }

    checkDatasetUnorderly(
      aggregated,
      "a", "30", "b", "3", "c", "1")
  }

  test("groupBy function, mapValues, flatMap") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val keyValue = ds.groupByKey(_._1).mapValues(_._2)
    val aggregated = keyValue.mapGroups { (g, iter) => (g, iter.sum) }
    checkDataset(aggregated, ("a", 30), ("b", 3), ("c", 1))

    val keyValue1 = ds.groupByKey(t => (t._1, "key")).mapValues(t => (t._2, "value"))
    val aggregated1 = keyValue1.mapGroups { (g, iter) => (g._1, iter.map(_._1).sum) }
    checkDataset(aggregated1, ("a", 30), ("b", 3), ("c", 1))
  }

  test("groupBy function, reduce") {
    val ds = Seq("abc", "xyz", "hello").toDS()
    val aggregated = ds.groupByKey(_.length).reduceGroups(_ + _)

    checkDatasetUnorderly(
      aggregated,
      3 -> "abcxyz", 5 -> "hello")
  }

  test("groupBy single field class, count") {
    val ds = Seq("abc", "xyz", "hello").toDS()
    val count = ds.groupByKey(s => Tuple1(s.length)).count()

    checkDataset(
      count,
      (Tuple1(3), 2L), (Tuple1(5), 1L)
    )
  }

  test("typed aggregation: expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(sum("_2").as[Long]),
      ("a", 30L), ("b", 3L), ("c", 1L))
  }

  test("typed aggregation: expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long]),
      ("a", 30L, 32L), ("b", 3L, 5L), ("c", 1L, 2L))
  }

  test("typed aggregation: expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long], count("*")),
      ("a", 30L, 32L, 2L), ("b", 3L, 5L, 2L), ("c", 1L, 2L, 1L))
  }

  test("typed aggregation: expr, expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(
        sum("_2").as[Long],
        sum($"_2" + 1).as[Long],
        count("*").as[Long],
        avg("_2").as[Double]),
      ("a", 30L, 32L, 2L, 15.0), ("b", 3L, 5L, 2L, 1.5), ("c", 1L, 2L, 1L, 1.0))
  }

  test("typed aggregation: expr, expr, expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(
        sum("_2").as[Long],
        sum($"_2" + 1).as[Long],
        count("*").as[Long],
        avg("_2").as[Double],
        countDistinct("*").as[Long]),
      ("a", 30L, 32L, 2L, 15.0, 2L), ("b", 3L, 5L, 2L, 1.5, 2L), ("c", 1L, 2L, 1L, 1.0, 1L))
  }

  test("typed aggregation: expr, expr, expr, expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDatasetUnorderly(
      ds.groupByKey(_._1).agg(
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
      ds.groupByKey(_._1).agg(
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
      ds.groupByKey(_._1).agg(
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

  test("cogroup") {
    val ds1 = Seq(1 -> "a", 3 -> "abc", 5 -> "hello", 3 -> "foo").toDS()
    val ds2 = Seq(2 -> "q", 3 -> "w", 5 -> "e", 5 -> "r").toDS()
    val cogrouped = ds1.groupByKey(_._1).cogroup(ds2.groupByKey(_._1)) { case (key, data1, data2) =>
      Iterator(key -> (data1.map(_._2).mkString + "#" + data2.map(_._2).mkString))
    }

    checkDatasetUnorderly(
      cogrouped,
      1 -> "a#", 2 -> "#q", 3 -> "abcfoo#w", 5 -> "hello#er")
  }

  test("cogroup with complex data") {
    val ds1 = Seq(1 -> ClassData("a", 1), 2 -> ClassData("b", 2)).toDS()
    val ds2 = Seq(2 -> ClassData("c", 3), 3 -> ClassData("d", 4)).toDS()
    val cogrouped = ds1.groupByKey(_._1).cogroup(ds2.groupByKey(_._1)) { case (key, data1, data2) =>
      Iterator(key -> (data1.map(_._2.a).mkString + data2.map(_._2.a).mkString))
    }

    checkDatasetUnorderly(
      cogrouped,
      1 -> "a", 2 -> "bc", 3 -> "d")
  }

  test("SPARK-34806: observation on datasets") {
    val namedObservation = Observation("named")
    val unnamedObservation = Observation()

    val df = spark.range(100)
    val observed_df = df
      .observe(
        namedObservation,
        min($"id").as("min_val"),
        max($"id").as("max_val"),
        sum($"id").as("sum_val"),
        count(when($"id" % 2 === 0, 1)).as("num_even")
      )
      .observe(
        unnamedObservation,
        avg($"id").cast("int").as("avg_val")
      )

    def checkMetrics(namedMetric: Observation, unnamedMetric: Observation): Unit = {
      assert(namedMetric.get === Map(
        "min_val" -> 0L, "max_val" -> 99L, "sum_val" -> 4950L, "num_even" -> 50L)
      )
      assert(unnamedMetric.get === Map("avg_val" -> 49))
    }

    observed_df.collect()
    // we can get the result multiple times
    checkMetrics(namedObservation, unnamedObservation)
    checkMetrics(namedObservation, unnamedObservation)

    // an observation can be used only once
    val err = intercept[IllegalArgumentException] {
      df.observe(namedObservation, sum($"id").as("sum_val"))
    }
    assert(err.getMessage.contains("An Observation can be used with a Dataset only once"))

    // streaming datasets are not supported
    val streamDf = new MemoryStream[Int](0, sqlContext).toDF()
    val streamObservation = Observation("stream")
    val streamErr = intercept[IllegalArgumentException] {
      streamDf.observe(streamObservation, avg($"value").cast("int").as("avg_val"))
    }
    assert(streamErr.getMessage.contains("Observation does not support streaming Datasets"))

    // an observation cannot have an empty name
    val err2 = intercept[IllegalArgumentException] {
      Observation("")
    }
    assert(err2.getMessage.contains("Name must not be empty"))
  }

  test("SPARK-37203: Fix NotSerializableException when observe with TypedImperativeAggregate") {
    def observe[T](df: Dataset[T], expected: Map[String, _]): Unit = {
      val namedObservation = Observation("named")
      val observed_df = df.observe(
        namedObservation, percentile_approx($"id", lit(0.5), lit(100)).as("percentile_approx_val"))
      observed_df.collect()
      assert(namedObservation.get === expected)
    }

    observe(spark.range(100), Map("percentile_approx_val" -> 49))
    observe(spark.range(0), Map("percentile_approx_val" -> null))
  }

  test("sample with replacement") {
    val n = 100
    val data = sparkContext.parallelize(1 to n, 2).toDS()
    checkDataset(
      data.sample(withReplacement = true, 0.05, seed = 13),
      5, 10, 52, 73)
  }

  test("sample without replacement") {
    val n = 100
    val data = sparkContext.parallelize(1 to n, 2).toDS()
    checkDataset(
      data.sample(withReplacement = false, 0.05, seed = 13),
      8, 37, 90)
  }

  test("sample fraction should not be negative with replacement") {
    val data = sparkContext.parallelize(1 to 2, 1).toDS()
    val errMsg = intercept[IllegalArgumentException] {
      data.sample(withReplacement = true, -0.1, 0)
    }.getMessage
    assert(errMsg.contains("Sampling fraction (-0.1) must be nonnegative with replacement"))

    // Sampling fraction can be greater than 1 with replacement.
    checkDataset(
      data.sample(withReplacement = true, 1.05, seed = 13),
      1, 2)
  }

  test("sample fraction should be on interval [0, 1] without replacement") {
    val data = sparkContext.parallelize(1 to 2, 1).toDS()
    val errMsg1 = intercept[IllegalArgumentException] {
      data.sample(withReplacement = false, -0.1, 0)
    }.getMessage()
    assert(errMsg1.contains(
      "Sampling fraction (-0.1) must be on interval [0, 1] without replacement"))

    val errMsg2 = intercept[IllegalArgumentException] {
      data.sample(withReplacement = false, 1.1, 0)
    }.getMessage()
    assert(errMsg2.contains(
      "Sampling fraction (1.1) must be on interval [0, 1] without replacement"))
  }

  test("SPARK-16686: Dataset.sample with seed results shouldn't depend on downstream usage") {
    val a = 7
    val simpleUdf = udf((n: Int) => {
      require(n != a, s"simpleUdf shouldn't see id=$a!")
      a
    })

    val df = Seq(
      (0, "string0"),
      (1, "string1"),
      (2, "string2"),
      (3, "string3"),
      (4, "string4"),
      (5, "string5"),
      (6, "string6"),
      (7, "string7"),
      (8, "string8"),
      (9, "string9")
    ).toDF("id", "stringData")
    val sampleDF = df.sample(false, 0.7, 50)
    // After sampling, sampleDF doesn't contain id=a.
    assert(!sampleDF.select("id").as[Int].collect.contains(a))
    // simpleUdf should not encounter id=a.
    checkAnswer(sampleDF.select(simpleUdf($"id")), List.fill(sampleDF.count.toInt)(Row(a)))
  }

  test("SPARK-11436: we should rebind right encoder when join 2 datasets") {
    val ds1 = Seq("1", "2").toDS().as("a")
    val ds2 = Seq(2, 3).toDS().as("b")

    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value")
    checkDataset(joined, ("2", 2))
  }

  test("self join") {
    val ds = Seq("1", "2").toDS().as("a")
    val joined = ds.joinWith(ds, lit(true), "cross")
    checkDataset(joined, ("1", "1"), ("1", "2"), ("2", "1"), ("2", "2"))
  }

  test("toString") {
    val ds = Seq((1, 2)).toDS()
    assert(ds.toString == "[_1: int, _2: int]")
  }

  test("Kryo encoder") {
    implicit val kryoEncoder = Encoders.kryo[KryoData]
    val ds = Seq(KryoData(1), KryoData(2)).toDS()

    assert(ds.groupByKey(p => p).count().collect().toSet ==
      Set((KryoData(1), 1L), (KryoData(2), 1L)))
  }

  test("Kryo encoder self join") {
    implicit val kryoEncoder = Encoders.kryo[KryoData]
    val ds = Seq(KryoData(1), KryoData(2)).toDS()
    assert(ds.joinWith(ds, lit(true), "cross").collect().toSet ==
      Set(
        (KryoData(1), KryoData(1)),
        (KryoData(1), KryoData(2)),
        (KryoData(2), KryoData(1)),
        (KryoData(2), KryoData(2))))
  }

  test("Kryo encoder: check the schema mismatch when converting DataFrame to Dataset") {
    implicit val kryoEncoder = Encoders.kryo[KryoData]
    val df = Seq((1.0)).toDF("a")
    val e = intercept[AnalysisException] {
      df.as[KryoData]
    }.message
    assert(e.contains("cannot cast double to binary"))
  }

  test("Java encoder") {
    implicit val kryoEncoder = Encoders.javaSerialization[JavaData]
    val ds = Seq(JavaData(1), JavaData(2)).toDS()

    assert(ds.groupByKey(p => p).count().collect().toSet ==
      Set((JavaData(1), 1L), (JavaData(2), 1L)))
  }

  test("Java encoder self join") {
    implicit val kryoEncoder = Encoders.javaSerialization[JavaData]
    val ds = Seq(JavaData(1), JavaData(2)).toDS()
    assert(ds.joinWith(ds, lit(true), "cross").collect().toSet ==
      Set(
        (JavaData(1), JavaData(1)),
        (JavaData(1), JavaData(2)),
        (JavaData(2), JavaData(1)),
        (JavaData(2), JavaData(2))))
  }

  test("SPARK-14696: implicit encoders for boxed types") {
    assert(spark.range(1).map { i => i : java.lang.Long }.head == 0L)
  }

  test("SPARK-11894: Incorrect results are returned when using null") {
    val nullInt = null.asInstanceOf[java.lang.Integer]
    val ds1 = Seq((nullInt, "1"), (java.lang.Integer.valueOf(22), "2")).toDS()
    val ds2 = Seq((nullInt, "1"), (java.lang.Integer.valueOf(22), "2")).toDS()

    checkDataset(
      ds1.joinWith(ds2, lit(true), "cross"),
      ((nullInt, "1"), (nullInt, "1")),
      ((nullInt, "1"), (java.lang.Integer.valueOf(22), "2")),
      ((java.lang.Integer.valueOf(22), "2"), (nullInt, "1")),
      ((java.lang.Integer.valueOf(22), "2"), (java.lang.Integer.valueOf(22), "2")))
  }

  test("change encoder with compatible schema") {
    val ds = Seq(2 -> 2.toByte, 3 -> 3.toByte).toDF("a", "b").as[ClassData]
    assert(ds.collect().toSeq == Seq(ClassData("2", 2), ClassData("3", 3)))
  }

  test("verify mismatching field names fail with a good error") {
    val ds = Seq(ClassData("a", 1)).toDS()
    val e = intercept[AnalysisException] {
      ds.as[ClassData2]
    }
    assert(e.getErrorClass == "MISSING_COLUMN")
    assert(e.messageParameters.sameElements(Array("c", "a, b")))
  }

  test("runtime nullability check") {
    val schema = StructType(Seq(
      StructField("f", StructType(Seq(
        StructField("a", StringType, nullable = true),
        StructField("b", IntegerType, nullable = true)
      )), nullable = true)
    ))

    def buildDataset(rows: Row*): Dataset[NestedStruct] = {
      val rowRDD = spark.sparkContext.parallelize(rows)
      spark.createDataFrame(rowRDD, schema).as[NestedStruct]
    }

    checkDataset(
      buildDataset(Row(Row("hello", 1))),
      NestedStruct(ClassData("hello", 1))
    )

    // Shouldn't throw runtime exception when parent object (`ClassData`) is null
    assert(buildDataset(Row(null)).collect() === Array(NestedStruct(null)))

    val message = intercept[RuntimeException] {
      buildDataset(Row(Row("hello", null))).collect()
    }.getMessage

    assert(message.contains("Null value appeared in non-nullable field"))
  }

  test("SPARK-12478: top level null field") {
    val ds0 = Seq(NestedStruct(null)).toDS()
    checkDataset(ds0, NestedStruct(null))
    checkAnswer(ds0.toDF(), Row(null))

    val ds1 = Seq(DeepNestedStruct(NestedStruct(null))).toDS()
    checkDataset(ds1, DeepNestedStruct(NestedStruct(null)))
    checkAnswer(ds1.toDF(), Row(Row(null)))
  }

  test("support inner class in Dataset") {
    val outer = new OuterClass
    OuterScopes.addOuterScope(outer)
    val ds = Seq(outer.InnerClass("1"), outer.InnerClass("2")).toDS()
    checkDataset(ds.map(_.a), "1", "2")
  }

  test("grouping key and grouped value has field with same name") {
    val ds = Seq(ClassData("a", 1), ClassData("a", 2)).toDS()
    val aggregated = ds.groupByKey(d => ClassNullableData(d.a, null)).mapGroups {
      (key, values) => key.a + values.map(_.b).sum
    }

    checkDataset(aggregated, "a3")
  }

  test("cogroup's left and right side has field with same name") {
    val left = Seq(ClassData("a", 1), ClassData("b", 2)).toDS()
    val right = Seq(ClassNullableData("a", 3), ClassNullableData("b", 4)).toDS()
    val cogrouped = left.groupByKey(_.a).cogroup(right.groupByKey(_.a)) {
      case (key, lData, rData) => Iterator(key + lData.map(_.b).sum + rData.map(_.b.toInt).sum)
    }

    checkDataset(cogrouped, "a13", "b24")
  }

  test("SPARK-13440: Resolving option fields") {
    val df = Seq(1, 2, 3).toDS()
    val ds = df.as[Option[Int]]
    checkDataset(
      ds.filter(_ => true),
      Some(1), Some(2), Some(3))
  }

  test("SPARK-13540 Dataset of nested class defined in Scala object") {
    checkDataset(
      Seq(OuterObject.InnerClass("foo")).toDS(),
      OuterObject.InnerClass("foo"))
  }

  test("SPARK-14000: case class with tuple type field") {
    checkDataset(
      Seq(TupleClass((1, "a"))).toDS(),
      TupleClass((1, "a"))
    )
  }

  test("isStreaming returns false for static Dataset") {
    val data = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    assert(!data.isStreaming, "static Dataset returned true for 'isStreaming'.")
  }

  test("isStreaming returns true for streaming Dataset") {
    val data = MemoryStream[Int].toDS()
    assert(data.isStreaming, "streaming Dataset returned false for 'isStreaming'.")
  }

  test("isStreaming returns true after static and streaming Dataset join") {
    val static = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("a", "b")
    val streaming = MemoryStream[Int].toDS().toDF("b")
    val df = streaming.join(static, Seq("b"))
    assert(df.isStreaming, "streaming Dataset returned false for 'isStreaming'.")
  }

  test("SPARK-14554: Dataset.map may generate wrong java code for wide table") {
    val wideDF = spark.range(10).select(Seq.tabulate(1000) {i => ($"id" + i).as(s"c$i")} : _*)
    // Make sure the generated code for this plan can compile and execute.
    checkDataset(wideDF.map(_.getLong(0)), 0L until 10 : _*)
  }

  test("SPARK-14838: estimating sizeInBytes in operators with ObjectProducer shouldn't fail") {
    val dataset = Seq(
      (0, 3, 54f),
      (0, 4, 44f),
      (0, 5, 42f),
      (1, 3, 39f),
      (1, 5, 33f),
      (1, 4, 26f),
      (2, 3, 51f),
      (2, 5, 45f),
      (2, 4, 30f)
    ).toDF("user", "item", "rating")

    val actual = dataset
      .select("user", "item")
      .as[(Int, Int)]
      .groupByKey(_._1)
      .mapGroups { (src, ids) => (src, ids.map(_._2).toArray) }
      .toDF("id", "actual")

    dataset.join(actual, dataset("user") === actual("id")).collect()
  }

  test("SPARK-15097: implicits on dataset's spark can be imported") {
    val dataset = Seq(1, 2, 3).toDS()
    checkDataset(DatasetTransform.addOne(dataset), 2, 3, 4)
  }

  test("dataset.rdd with generic case class") {
    val ds = Seq(Generic(1, 1.0), Generic(2, 2.0)).toDS()
    val ds2 = ds.map(g => Generic(g.id, g.value))
    assert(ds.rdd.map(r => r.id).count === 2)
    assert(ds2.rdd.map(r => r.id).count === 2)

    val ds3 = ds.map(g => java.lang.Long.valueOf(g.id))
    assert(ds3.rdd.map(r => r).count === 2)
  }

  test("runtime null check for RowEncoder") {
    val schema = new StructType().add("i", IntegerType, nullable = false)
    val df = spark.range(10).map(l => {
      if (l % 5 == 0) {
        Row(null)
      } else {
        Row(l)
      }
    })(RowEncoder(schema))

    val message = intercept[Exception] {
      df.collect()
    }.getMessage
    assert(message.contains("The 0th field 'i' of input row cannot be null"))
  }

  test("row nullability mismatch") {
    val schema = new StructType().add("a", StringType, true).add("b", StringType, false)
    val rdd = spark.sparkContext.parallelize(Row(null, "123") :: Row("234", null) :: Nil)
    val message = intercept[Exception] {
      spark.createDataFrame(rdd, schema).collect()
    }.getMessage
    assert(message.contains("The 1th field 'b' of input row cannot be null"))
  }

  test("createTempView") {
    val dataset = Seq(1, 2, 3).toDS()
    dataset.createOrReplaceTempView("tempView")

    // Overrides the existing temporary view with same name
    // No exception should be thrown here.
    dataset.createOrReplaceTempView("tempView")

    // Throws AnalysisException if temp view with same name already exists
    val e = intercept[AnalysisException](
      dataset.createTempView("tempView"))
    intercept[AnalysisException](dataset.createTempView("tempView"))
    assert(e.message.contains("already exists"))
    dataset.sparkSession.catalog.dropTempView("tempView")
  }

  test("SPARK-15381: physical object operator should define `reference` correctly") {
    val df = Seq(1 -> 2).toDF("a", "b")
    checkAnswer(df.map(row => row)(RowEncoder(df.schema)).select("b", "a"), Row(2, 1))
  }

  private def checkShowString[T](ds: Dataset[T], expected: String): Unit = {
    val numRows = expected.split("\n").length - 4
    val actual = ds.showString(numRows, truncate = 20)

    if (expected != actual) {
      fail(
        "Dataset.showString() gives wrong result:\n\n" + sideBySide(
          "== Expected ==\n" + expected,
          "== Actual ==\n" + actual
        ).mkString("\n")
      )
    }
  }

  test("SPARK-15550 Dataset.show() should show contents of the underlying logical plan") {
    val df = Seq((1, "foo", "extra"), (2, "bar", "extra")).toDF("b", "a", "c")
    val ds = df.as[ClassData]
    val expected =
      """+---+---+-----+
        ||  b|  a|    c|
        |+---+---+-----+
        ||  1|foo|extra|
        ||  2|bar|extra|
        |+---+---+-----+
        |""".stripMargin

    checkShowString(ds, expected)
  }

  test("SPARK-15550 Dataset.show() should show inner nested products as rows") {
    val ds = Seq(
      NestedStruct(ClassData("foo", 1)),
      NestedStruct(ClassData("bar", 2))
    ).toDS()

    val expected =
      """+--------+
        ||       f|
        |+--------+
        ||{foo, 1}|
        ||{bar, 2}|
        |+--------+
        |""".stripMargin

    checkShowString(ds, expected)
  }

  test("SPARK-25108 Fix the show method to display the full width character alignment problem") {
    // scalastyle:off nonascii
    val df = Seq(
      (0, null, 1),
      (0, "", 1),
      (0, "ab c", 1),
      (0, "1098", 1),
      (0, "mø", 1),
      (0, "γύρ", 1),
      (0, "pê", 1),
      (0, "ー", 1),
      (0, "测", 1),
      (0, "か", 1),
      (0, "걸", 1),
      (0, "à", 1),
      (0, "焼", 1),
      (0, "羍む", 1),
      (0, "뺭ᾘ", 1),
      (0, "\u0967\u0968\u0969", 1)
    ).toDF("b", "a", "c")
    // scalastyle:on nonascii
    val ds = df.as[ClassData]
    val expected =
      // scalastyle:off nonascii
      """+---+----+---+
        ||  b|   a|  c|
        |+---+----+---+
        ||  0|null|  1|
        ||  0|    |  1|
        ||  0|ab c|  1|
        ||  0|1098|  1|
        ||  0|  mø|  1|
        ||  0| γύρ|  1|
        ||  0|  pê|  1|
        ||  0|  ー|  1|
        ||  0|  测|  1|
        ||  0|  か|  1|
        ||  0|  걸|  1|
        ||  0|   à|  1|
        ||  0|  焼|  1|
        ||  0|羍む|  1|
        ||  0| 뺭ᾘ|  1|
        ||  0| १२३|  1|
        |+---+----+---+
        |""".stripMargin
    // scalastyle:on nonascii
    checkShowString(ds, expected)
  }

  test(
    "SPARK-15112: EmbedDeserializerInFilter should not optimize plan fragment that changes schema"
  ) {
    val ds = Seq(1 -> "foo", 2 -> "bar").toDF("b", "a").as[ClassData]

    assertResult(Seq(ClassData("foo", 1), ClassData("bar", 2))) {
      ds.collect().toSeq
    }

    assertResult(Seq(ClassData("bar", 2))) {
      ds.filter(_.b > 1).collect().toSeq
    }
  }

  test("mapped dataset should resolve duplicated attributes for self join") {
    val ds = Seq(1, 2, 3).toDS().map(_ + 1)
    val ds1 = ds.as("d1")
    val ds2 = ds.as("d2")

    checkDatasetUnorderly(ds1.joinWith(ds2, $"d1.value" === $"d2.value"), (2, 2), (3, 3), (4, 4))
    checkDatasetUnorderly(ds1.intersect(ds2), 2, 3, 4)
    checkDatasetUnorderly(ds1.except(ds1))
  }

  test("SPARK-15441: Dataset outer join") {
    val left = Seq(ClassData("a", 1), ClassData("b", 2)).toDS().as("left")
    val right = Seq(ClassData("x", 2), ClassData("y", 3)).toDS().as("right")
    val joined = left.joinWith(right, $"left.b" === $"right.b", "left")

    val expectedSchema = StructType(Seq(
      StructField("_1",
        StructType(Seq(
          StructField("a", StringType),
          StructField("b", IntegerType, nullable = false)
        )),
        nullable = false),
      // This is a left join, so the right output is nullable:
      StructField("_2",
        StructType(Seq(
          StructField("a", StringType),
          StructField("b", IntegerType, nullable = false)
        )))
    ))
    assert(joined.schema === expectedSchema)

    val result = joined.collect().toSet
    assert(result == Set(ClassData("a", 1) -> null, ClassData("b", 2) -> ClassData("x", 2)))
  }

  test("Dataset should support flat input object to be null") {
    checkDataset(Seq("a", null).toDS(), "a", null)
  }

  test("Dataset should throw RuntimeException if top-level product input object is null") {
    val e = intercept[RuntimeException](Seq(ClassData("a", 1), null).toDS())
    assert(e.getMessage.contains("Null value appeared in non-nullable field"))
    assert(e.getMessage.contains("top level Product or row object"))
  }

  test("dropDuplicates") {
    val ds = Seq(("a", 1), ("a", 2), ("b", 1), ("a", 1)).toDS()
    checkDataset(
      ds.dropDuplicates("_1"),
      ("a", 1), ("b", 1))
    checkDataset(
      ds.dropDuplicates("_2"),
      ("a", 1), ("a", 2))
    checkDataset(
      ds.dropDuplicates("_1", "_2"),
      ("a", 1), ("a", 2), ("b", 1))
  }

  test("dropDuplicates: columns with same column name") {
    val ds1 = Seq(("a", 1), ("a", 2), ("b", 1), ("a", 1)).toDS()
    val ds2 = Seq(("a", 1), ("a", 2), ("b", 1), ("a", 1)).toDS()
    // The dataset joined has two columns of the same name "_2".
    val joined = ds1.join(ds2, "_1").select(ds1("_2").as[Int], ds2("_2").as[Int])
    checkDataset(
      joined.dropDuplicates(),
      (1, 2), (1, 1), (2, 1), (2, 2))
  }

  test("SPARK-16097: Encoders.tuple should handle null object correctly") {
    val enc = Encoders.tuple(Encoders.tuple(Encoders.STRING, Encoders.STRING), Encoders.STRING)
    val data = Seq((("a", "b"), "c"), (null, "d"))
    val ds = spark.createDataset(data)(enc)
    checkDataset(ds, (("a", "b"), "c"), (null, "d"))
  }

  test("SPARK-16995: flat mapping on Dataset containing a column created with lit/expr") {
    val df = Seq("1").toDF("a")

    import df.sparkSession.implicits._

    checkDataset(
      df.withColumn("b", lit(0)).as[ClassData]
        .groupByKey(_.a).flatMapGroups { (_, _) => List[Int]() })
    checkDataset(
      df.withColumn("b", expr("0")).as[ClassData]
        .groupByKey(_.a).flatMapGroups { (_, _) => List[Int]() })
  }

  test("SPARK-18125: Spark generated code causes CompileException") {
    val data = Array(
      Route("a", "b", 1),
      Route("a", "b", 2),
      Route("a", "c", 2),
      Route("a", "d", 10),
      Route("b", "a", 1),
      Route("b", "a", 5),
      Route("b", "c", 6))
    val ds = sparkContext.parallelize(data).toDF.as[Route]

    val grouped = ds.map(r => GroupedRoutes(r.src, r.dest, Seq(r)))
      .groupByKey(r => (r.src, r.dest))
      .reduceGroups { (g1: GroupedRoutes, g2: GroupedRoutes) =>
        GroupedRoutes(g1.src, g1.dest, g1.routes ++ g2.routes)
      }.map(_._2)

    val expected = Seq(
      GroupedRoutes("a", "d", Seq(Route("a", "d", 10))),
      GroupedRoutes("b", "c", Seq(Route("b", "c", 6))),
      GroupedRoutes("a", "b", Seq(Route("a", "b", 1), Route("a", "b", 2))),
      GroupedRoutes("b", "a", Seq(Route("b", "a", 1), Route("b", "a", 5))),
      GroupedRoutes("a", "c", Seq(Route("a", "c", 2)))
    )

    implicit def ordering[GroupedRoutes]: Ordering[GroupedRoutes] =
      (x: GroupedRoutes, y: GroupedRoutes) => x.toString.compareTo(y.toString)

    checkDatasetUnorderly(grouped, expected: _*)
  }

  test("SPARK-18189: Fix serialization issue in KeyValueGroupedDataset") {
    val resultValue = 12345
    val keyValueGrouped = Seq((1, 2), (3, 4)).toDS().groupByKey(_._1)
    val mapGroups = keyValueGrouped.mapGroups((k, v) => (k, 1))
    val broadcasted = spark.sparkContext.broadcast(resultValue)

    // Using broadcast triggers serialization issue in KeyValueGroupedDataset
    val dataset = mapGroups.map(_ => broadcasted.value)

    assert(dataset.collect() sameElements Array(resultValue, resultValue))
  }

  test("SPARK-18284: Serializer should have correct nullable value") {
    val df1 = Seq(1, 2, 3, 4).toDF
    assert(df1.schema(0).nullable == false)
    val df2 = Seq(Integer.valueOf(1), Integer.valueOf(2)).toDF
    assert(df2.schema(0).nullable)

    val df3 = Seq(Seq(1, 2), Seq(3, 4)).toDF
    assert(df3.schema(0).nullable)
    assert(df3.schema(0).dataType.asInstanceOf[ArrayType].containsNull == false)
    val df4 = Seq(Seq("a", "b"), Seq("c", "d")).toDF
    assert(df4.schema(0).nullable)
    assert(df4.schema(0).dataType.asInstanceOf[ArrayType].containsNull)

    val df5 = Seq((0, 1.0), (2, 2.0)).toDF("id", "v")
    assert(df5.schema(0).nullable == false)
    assert(df5.schema(1).nullable == false)
    val df6 = Seq((0, 1.0, "a"), (2, 2.0, "b")).toDF("id", "v1", "v2")
    assert(df6.schema(0).nullable == false)
    assert(df6.schema(1).nullable == false)
    assert(df6.schema(2).nullable)

    val df7 = (Tuple1(Array(1, 2, 3)) :: Nil).toDF("a")
    assert(df7.schema(0).nullable)
    assert(df7.schema(0).dataType.asInstanceOf[ArrayType].containsNull == false)

    val df8 = (Tuple1(Array((null: Integer), (null: Integer))) :: Nil).toDF("a")
    assert(df8.schema(0).nullable)
    assert(df8.schema(0).dataType.asInstanceOf[ArrayType].containsNull)

    val df9 = (Tuple1(Map(2 -> 3)) :: Nil).toDF("m")
    assert(df9.schema(0).nullable)
    assert(df9.schema(0).dataType.asInstanceOf[MapType].valueContainsNull == false)

    val df10 = (Tuple1(Map(1 -> (null: Integer))) :: Nil).toDF("m")
    assert(df10.schema(0).nullable)
    assert(df10.schema(0).dataType.asInstanceOf[MapType].valueContainsNull)

    val df11 = Seq(TestDataPoint(1, 2.2, "a", null),
                   TestDataPoint(3, 4.4, "null", (TestDataPoint2(33, "b")))).toDF
    assert(df11.schema(0).nullable == false)
    assert(df11.schema(1).nullable == false)
    assert(df11.schema(2).nullable)
    assert(df11.schema(3).nullable)
    assert(df11.schema(3).dataType.asInstanceOf[StructType].fields(0).nullable == false)
    assert(df11.schema(3).dataType.asInstanceOf[StructType].fields(1).nullable)
  }

  Seq(true, false).foreach { eager =>
    Seq(true, false).foreach { reliable =>
      def testCheckpointing(testName: String)(f: => Unit): Unit = {
        test(s"Dataset.checkpoint() - $testName (eager = $eager, reliable = $reliable)") {
          if (reliable) {
            withTempDir { dir =>
              val originalCheckpointDir = spark.sparkContext.checkpointDir

              try {
                spark.sparkContext.setCheckpointDir(dir.getCanonicalPath)
                f
              } finally {
                // Since the original checkpointDir can be None, we need
                // to set the variable directly.
                spark.sparkContext.checkpointDir = originalCheckpointDir
              }
            }
          } else {
            // Local checkpoints don't require checkpoint_dir
            f
          }
        }
      }

      testCheckpointing("basic") {
        val ds = spark
          .range(10)
          // Num partitions is set to 1 to avoid a RangePartitioner in the orderBy below
          .repartition(1, $"id" % 2)
          .filter($"id" > 5)
          .orderBy($"id".desc)
        @volatile var jobCounter = 0
        val listener = new SparkListener {
          override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
            jobCounter += 1
          }
        }
        var cp = ds
        withListener(spark.sparkContext, listener) { _ =>
          // AQE adds a job per shuffle. The expression above does multiple shuffles and
          // that screws up the job counting
          withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
            cp = if (reliable) ds.checkpoint(eager) else ds.localCheckpoint(eager)
          }
        }
        if (eager) {
          assert(jobCounter === 1)
        }

        val logicalRDD = cp.logicalPlan match {
          case plan: LogicalRDD => plan
          case _ =>
            val treeString = cp.logicalPlan.treeString(verbose = true)
            fail(s"Expecting a LogicalRDD, but got\n$treeString")
        }

        val dsPhysicalPlan = ds.queryExecution.executedPlan
        val cpPhysicalPlan = cp.queryExecution.executedPlan

        assertResult(dsPhysicalPlan.outputPartitioning) {
          logicalRDD.outputPartitioning
        }
        assertResult(dsPhysicalPlan.outputOrdering) {
          logicalRDD.outputOrdering
        }

        assertResult(dsPhysicalPlan.outputPartitioning) {
          cpPhysicalPlan.outputPartitioning
        }
        assertResult(dsPhysicalPlan.outputOrdering) {
          cpPhysicalPlan.outputOrdering
        }

        // For a lazy checkpoint() call, the first check also materializes the checkpoint.
        checkDataset(cp, (9L to 6L by -1L).map(java.lang.Long.valueOf): _*)

        // Reads back from checkpointed data and check again.
        checkDataset(cp, (9L to 6L by -1L).map(java.lang.Long.valueOf): _*)
      }

      testCheckpointing("should preserve partitioning information") {
        val ds = spark.range(10).repartition($"id" % 2)
        val cp = if (reliable) ds.checkpoint(eager) else ds.localCheckpoint(eager)

        val agg = cp.groupBy($"id" % 2).agg(count($"id"))

        agg.queryExecution.executedPlan.collectFirst {
          case ShuffleExchangeExec(_, _: RDDScanExec, _) =>
          case BroadcastExchangeExec(_, _: RDDScanExec) =>
        }.foreach { _ =>
          fail(
            "No Exchange should be inserted above RDDScanExec since the checkpointed Dataset " +
              "preserves partitioning information:\n\n" + agg.queryExecution
          )
        }

        checkAnswer(agg, ds.groupBy($"id" % 2).agg(count($"id")))
      }
    }
  }

  test("identity map for primitive arrays") {
    val arrayByte = Array(1.toByte, 2.toByte, 3.toByte)
    val arrayInt = Array(1, 2, 3)
    val arrayLong = Array(1.toLong, 2.toLong, 3.toLong)
    val arrayDouble = Array(1.1, 2.2, 3.3)
    val arrayString = Array("a", "b", "c")
    val dsByte = sparkContext.parallelize(Seq(arrayByte), 1).toDS.map(e => e)
    val dsInt = sparkContext.parallelize(Seq(arrayInt), 1).toDS.map(e => e)
    val dsLong = sparkContext.parallelize(Seq(arrayLong), 1).toDS.map(e => e)
    val dsDouble = sparkContext.parallelize(Seq(arrayDouble), 1).toDS.map(e => e)
    val dsString = sparkContext.parallelize(Seq(arrayString), 1).toDS.map(e => e)
    checkDataset(dsByte, arrayByte)
    checkDataset(dsInt, arrayInt)
    checkDataset(dsLong, arrayLong)
    checkDataset(dsDouble, arrayDouble)
    checkDataset(dsString, arrayString)
  }

  test ("SPARK-17460: the sizeInBytes in Statistics shouldn't overflow to a negative number") {
    // Since the sizeInBytes in Statistics could exceed the limit of an Int, we should use BigInt
    // instead of Int for avoiding possible overflow.
    val ds = (0 to 10000).map( i =>
      (i, Seq((i, Seq((i, "This is really not that long of a string")))))).toDS()
    val sizeInBytes = ds.logicalPlan.stats.sizeInBytes
    // sizeInBytes is 2404280404, before the fix, it overflows to a negative number
    assert(sizeInBytes > 0)
  }

  test("SPARK-18717: code generation works for both scala.collection.Map" +
    " and scala.collection.immutable.Map") {
    val ds = Seq(WithImmutableMap("hi", Map(42L -> "foo"))).toDS
    checkDataset(ds.map(t => t), WithImmutableMap("hi", Map(42L -> "foo")))

    val ds2 = Seq(WithMap("hi", Map(42L -> "foo"))).toDS
    checkDataset(ds2.map(t => t), WithMap("hi", Map(42L -> "foo")))
  }

  test("SPARK-18746: add implicit encoder for BigDecimal, date, timestamp") {
    // For this implicit encoder, 18 is the default scale
    assert(spark.range(1).map { x => new java.math.BigDecimal(1) }.head ==
      new java.math.BigDecimal(1).setScale(18))

    assert(spark.range(1).map { x => scala.math.BigDecimal(1, 18) }.head ==
      scala.math.BigDecimal(1, 18))

    assert(spark.range(1).map { x => java.sql.Date.valueOf("2016-12-12") }.head ==
      java.sql.Date.valueOf("2016-12-12"))

    assert(spark.range(1).map { x => new java.sql.Timestamp(100000) }.head ==
      new java.sql.Timestamp(100000))
  }

  test("SPARK-19896: cannot have circular references in case class") {
    val errMsg1 = intercept[UnsupportedOperationException] {
      Seq(CircularReferenceClassA(null)).toDS
    }
    assert(errMsg1.getMessage.startsWith("cannot have circular references in class, but got the " +
      "circular reference of class"))
    val errMsg2 = intercept[UnsupportedOperationException] {
      Seq(CircularReferenceClassC(null)).toDS
    }
    assert(errMsg2.getMessage.startsWith("cannot have circular references in class, but got the " +
      "circular reference of class"))
    val errMsg3 = intercept[UnsupportedOperationException] {
      Seq(CircularReferenceClassD(null)).toDS
    }
    assert(errMsg3.getMessage.startsWith("cannot have circular references in class, but got the " +
      "circular reference of class"))
  }

  test("SPARK-20125: option of map") {
    val ds = Seq(WithMapInOption(Some(Map(1 -> 1)))).toDS()
    checkDataset(ds, WithMapInOption(Some(Map(1 -> 1))))
  }

  test("SPARK-20399: do not unescaped regex pattern when ESCAPED_STRING_LITERALS is enabled") {
    withSQLConf(SQLConf.ESCAPED_STRING_LITERALS.key -> "true") {
      val data = Seq("\u0020\u0021\u0023", "abc")
      val df = data.toDF()
      val rlike1 = df.filter("value rlike '^\\x20[\\x20-\\x23]+$'")
      val rlike2 = df.filter($"value".rlike("^\\x20[\\x20-\\x23]+$"))
      val rlike3 = df.filter("value rlike '^\\\\x20[\\\\x20-\\\\x23]+$'")
      checkAnswer(rlike1, rlike2)
      assert(rlike3.count() == 0)
    }
  }

  test("SPARK-21538: Attribute resolution inconsistency in Dataset API") {
    val df = spark.range(3).withColumnRenamed("id", "x")
    val expected = Row(0) :: Row(1) :: Row (2) :: Nil
    checkAnswer(df.sort("id"), expected)
    checkAnswer(df.sort(col("id")), expected)
    checkAnswer(df.sort($"id"), expected)
    checkAnswer(df.orderBy("id"), expected)
    checkAnswer(df.orderBy(col("id")), expected)
    checkAnswer(df.orderBy($"id"), expected)
  }

  test("SPARK-21567: Dataset should work with type alias") {
    checkDataset(
      Seq(1).toDS().map(_ => ("", TestForTypeAlias.tupleTypeAlias)),
      ("", (1, 1)))

    checkDataset(
      Seq(1).toDS().map(_ => ("", TestForTypeAlias.nestedTupleTypeAlias)),
      ("", ((1, 1), 2)))

    checkDataset(
      Seq(1).toDS().map(_ => ("", TestForTypeAlias.seqOfTupleTypeAlias)),
      ("", Seq((1, 1), (2, 2))))
  }

  test("SPARK-38042: Dataset should work with a product containing an aliased array type") {
    checkDataset(
      Seq(1).toDS().map(_ => ("", TestForTypeAlias.aliasedArrayInTuple)),
      ("", (1, Array(1))))
  }

  test("Check RelationalGroupedDataset toString: Single data") {
    val kvDataset = (1 to 3).toDF("id").groupBy("id")
    val expected = "RelationalGroupedDataset: [" +
      "grouping expressions: [id: int], value: [id: int], type: GroupBy]"
    val actual = kvDataset.toString
    assert(expected === actual)
  }

  test("Check RelationalGroupedDataset toString: over length schema ") {
    val kvDataset = (1 to 3).map( x => (x, x.toString, x.toLong))
      .toDF("id", "val1", "val2").groupBy("id")
    val expected = "RelationalGroupedDataset:" +
      " [grouping expressions: [id: int]," +
      " value: [id: int, val1: string ... 1 more field]," +
      " type: GroupBy]"
    val actual = kvDataset.toString
    assert(expected === actual)
  }


  test("Check KeyValueGroupedDataset toString: Single data") {
    val kvDataset = (1 to 3).toDF("id").as[SingleData].groupByKey(identity)
    val expected = "KeyValueGroupedDataset: [key: [id: int], value: [id: int]]"
    val actual = kvDataset.toString
    assert(expected === actual)
  }

  test("Check KeyValueGroupedDataset toString: Unnamed KV-pair") {
    val kvDataset = (1 to 3).map(x => (x, x.toString))
      .toDF("id", "val1").as[DoubleData].groupByKey(x => (x.id, x.val1))
    val expected = "KeyValueGroupedDataset:" +
      " [key: [_1: int, _2: string]," +
      " value: [id: int, val1: string]]"
    val actual = kvDataset.toString
    assert(expected === actual)
  }

  test("Check KeyValueGroupedDataset toString: Named KV-pair") {
    val kvDataset = (1 to 3).map( x => (x, x.toString))
      .toDF("id", "val1").as[DoubleData].groupByKey(x => DoubleData(x.id, x.val1))
    val expected = "KeyValueGroupedDataset:" +
      " [key: [id: int, val1: string]," +
      " value: [id: int, val1: string]]"
    val actual = kvDataset.toString
    assert(expected === actual)
  }

  test("Check KeyValueGroupedDataset toString: over length schema ") {
    val kvDataset = (1 to 3).map( x => (x, x.toString, x.toLong))
      .toDF("id", "val1", "val2").as[TripleData].groupByKey(identity)
    val expected = "KeyValueGroupedDataset:" +
      " [key: [id: int, val1: string ... 1 more field(s)]," +
      " value: [id: int, val1: string ... 1 more field(s)]]"
    val actual = kvDataset.toString
    assert(expected === actual)
  }

  test("SPARK-22442: Generate correct field names for special characters") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val data = """{"field.1": 1, "field 2": 2}"""
      Seq(data).toDF().repartition(1).write.text(path)
      val ds = spark.read.json(path).as[SpecialCharClass]
      checkDataset(ds, SpecialCharClass("1", "2"))
    }
  }

  test("SPARK-23627: provide isEmpty in DataSet") {
    val ds1 = spark.emptyDataset[Int]
    val ds2 = Seq(1, 2, 3).toDS()

    assert(ds1.isEmpty)
    assert(ds2.isEmpty == false)
  }

  test("SPARK-22472: add null check for top-level primitive values") {
    // If the primitive values are from Option, we need to do runtime null check.
    val ds = Seq(Some(1), None).toDS().as[Int]
    val e1 = intercept[RuntimeException](ds.collect())
    assert(e1.getCause.isInstanceOf[NullPointerException])
    val e2 = intercept[SparkException](ds.map(_ * 2).collect())
    assert(e2.getCause.isInstanceOf[NullPointerException])

    withTempPath { path =>
      Seq(Integer.valueOf(1), null).toDF("i").write.parquet(path.getCanonicalPath)
      // If the primitive values are from files, we need to do runtime null check.
      val ds = spark.read.parquet(path.getCanonicalPath).as[Int]
      val e1 = intercept[RuntimeException](ds.collect())
      assert(e1.getCause.isInstanceOf[NullPointerException])
      val e2 = intercept[SparkException](ds.map(_ * 2).collect())
      assert(e2.getCause.isInstanceOf[NullPointerException])
    }
  }

  test("SPARK-23025: Add support for null type in scala reflection") {
    val data = Seq(("a", null))
    checkDataset(data.toDS(), data: _*)
  }

  test("SPARK-23614: Union produces incorrect results when caching is used") {
    val cached = spark.createDataset(Seq(TestDataUnion(1, 2, 3), TestDataUnion(4, 5, 6))).cache()
    val group1 = cached.groupBy("x").agg(min(col("y")) as "value")
    val group2 = cached.groupBy("x").agg(min(col("z")) as "value")
    checkAnswer(group1.union(group2), Row(4, 5) :: Row(1, 2) :: Row(4, 6) :: Row(1, 3) :: Nil)
  }

  test("SPARK-23835: null primitive data type should throw NullPointerException") {
    val ds = Seq[(Option[Int], Option[Int])]((Some(1), None)).toDS()
    val e = intercept[RuntimeException](ds.as[(Int, Int)].collect())
    assert(e.getCause.isInstanceOf[NullPointerException])
  }

  test("SPARK-24569: Option of primitive types are mistakenly mapped to struct type") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      val a = Seq(Some(1)).toDS
      val b = Seq(Some(1.2)).toDS
      val expected = Seq((Some(1), Some(1.2))).toDS
      val joined = a.joinWith(b, lit(true))
      assert(joined.schema == expected.schema)
      checkDataset(joined, expected.collect: _*)
    }
  }

  test("SPARK-24548: Dataset with tuple encoders should have correct schema") {
    val encoder = Encoders.tuple(newStringEncoder,
      Encoders.tuple(newStringEncoder, newStringEncoder))

    val data = Seq(("a", ("1", "2")), ("b", ("3", "4")))
    val rdd = sparkContext.parallelize(data)

    val ds1 = spark.createDataset(rdd)
    val ds2 = spark.createDataset(rdd)(encoder)
    assert(ds1.schema == ds2.schema)
    checkDataset(ds1.select("_2._2"), ds2.select("_2._2").collect(): _*)
  }

  test("SPARK-23862: Spark ExpressionEncoder should support Java Enum type from Scala") {
    val saveModeSeq =
      Seq(SaveMode.Append, SaveMode.Overwrite, SaveMode.ErrorIfExists, SaveMode.Ignore, null)
    assert(saveModeSeq.toDS().collect().toSeq === saveModeSeq)
    assert(saveModeSeq.toDS().schema === new StructType().add("value", StringType, nullable = true))

    val saveModeCaseSeq = saveModeSeq.map(SaveModeCase.apply)
    assert(saveModeCaseSeq.toDS().collect().toSet === saveModeCaseSeq.toSet)
    assert(saveModeCaseSeq.toDS().schema ===
      new StructType().add("mode", StringType, nullable = true))

    val saveModeArrayCaseSeq =
      Seq(SaveModeArrayCase(Array()), SaveModeArrayCase(saveModeSeq.toArray))
    val collected = saveModeArrayCaseSeq.toDS().collect()
    assert(collected.length === 2)
    val sortedByLength = collected.sortBy(_.modes.length)
    assert(sortedByLength(0).modes === Array())
    assert(sortedByLength(1).modes === saveModeSeq.toArray)
    assert(saveModeArrayCaseSeq.toDS().schema ===
      new StructType().add("modes", ArrayType(StringType, containsNull = true), nullable = true))

    // Enum is stored as string, so it is possible to convert to/from string
    val stringSeq = saveModeSeq.map(Option.apply).map(_.map(_.toString).orNull)
    assert(stringSeq.toDS().as[SaveMode].collect().toSet === saveModeSeq.toSet)
    assert(saveModeSeq.toDS().as[String].collect().toSet === stringSeq.toSet)
  }

  test("SPARK-24571: filtering of string values by char literal") {
    val df = Seq("Amsterdam", "San Francisco", "X").toDF("city")
    checkAnswer(df.where($"city" === 'X'), Seq(Row("X")))
    checkAnswer(
      df.where($"city".contains(java.lang.Character.valueOf('A'))),
      Seq(Row("Amsterdam")))
  }

  test("SPARK-24762: Enable top-level Option of Product encoders") {
    val data = Seq(Some((1, "a")), Some((2, "b")), None)
    val ds = data.toDS()

    checkDataset(
      ds,
      data: _*)

    val schema = new StructType().add(
      "value",
      new StructType()
        .add("_1", IntegerType, nullable = false)
        .add("_2", StringType, nullable = true),
      nullable = true)

    assert(ds.schema == schema)

    val nestedOptData = Seq(Some((Some((1, "a")), 2.0)), Some((Some((2, "b")), 3.0)))
    val nestedDs = nestedOptData.toDS()

    checkDataset(
      nestedDs,
      nestedOptData: _*)

    val nestedSchema = StructType(Seq(
      StructField("value", StructType(Seq(
        StructField("_1", StructType(Seq(
          StructField("_1", IntegerType, nullable = false),
          StructField("_2", StringType, nullable = true)))),
        StructField("_2", DoubleType, nullable = false)
      )), nullable = true)
    ))
    assert(nestedDs.schema == nestedSchema)
  }

  test("SPARK-24762: Resolving Option[Product] field") {
    val ds = Seq((1, ("a", 1.0)), (2, ("b", 2.0)), (3, null)).toDS()
      .as[(Int, Option[(String, Double)])]
    checkDataset(ds,
      (1, Some(("a", 1.0))), (2, Some(("b", 2.0))), (3, None))
  }

  test("SPARK-24762: select Option[Product] field") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    val ds1 = ds.select(expr("struct(_2, _2 + 1)").as[Option[(Int, Int)]])
    checkDataset(ds1,
      Some((1, 2)), Some((2, 3)), Some((3, 4)))

    val ds2 = ds.select(expr("if(_2 > 2, struct(_2, _2 + 1), null)").as[Option[(Int, Int)]])
    checkDataset(ds2,
      None, None, Some((3, 4)))
  }

  test("SPARK-24762: joinWith on Option[Product]") {
    val ds1 = Seq(Some((1, 2)), Some((2, 3)), None).toDS().as("a")
    val ds2 = Seq(Some((1, 2)), Some((2, 3)), None).toDS().as("b")
    val joined = ds1.joinWith(ds2, $"a.value._1" === $"b.value._2", "inner")
    checkDataset(joined, (Some((2, 3)), Some((1, 2))))
  }

  test("SPARK-24762: typed agg on Option[Product] type") {
    val ds = Seq(Some((1, 2)), Some((2, 3)), Some((1, 3))).toDS()
    assert(ds.groupByKey(_.get._1).count().collect() === Seq((1, 2), (2, 1)))

    assert(ds.groupByKey(x => x).count().collect() ===
      Seq((Some((1, 2)), 1), (Some((2, 3)), 1), (Some((1, 3)), 1)))
  }

  test("SPARK-25942: typed aggregation on primitive type") {
    val ds = Seq(1, 2, 3).toDS()

    val agg = ds.groupByKey(_ >= 2)
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
    withSQLConf(SQLConf.NAME_NON_STRUCT_GROUPING_KEY_AS_VALUE.key -> "true") {
      assert(ds.groupByKey(x => x).count().schema.head.name == "value")
    }
  }

  test("SPARK-8288: class with only a companion object constructor") {
    val data = Seq(ScroogeLikeExample(1), ScroogeLikeExample(2))
    val ds = data.toDS
    checkDataset(ds, data: _*)
    checkAnswer(ds.select("x"), Seq(Row(1), Row(2)))
  }

  test("SPARK-26233: serializer should enforce decimal precision and scale") {
    val s = StructType(Seq(StructField("a", StringType), StructField("b", DecimalType(38, 8))))
    val encoder = RowEncoder(s)
    implicit val uEnc = encoder
    val df = spark.range(2).map(l => Row(l.toString, BigDecimal.valueOf(l + 0.1111)))
    checkAnswer(df.groupBy(col("a")).agg(first(col("b"))),
      Seq(Row("0", BigDecimal.valueOf(0.1111)), Row("1", BigDecimal.valueOf(1.1111))))
  }

  test("SPARK-26366: return nulls which are not filtered in except") {
    val inputDF = sqlContext.createDataFrame(
      sparkContext.parallelize(Seq(Row("0", "a"), Row("1", null))),
      StructType(Seq(
        StructField("a", StringType, nullable = true),
        StructField("b", StringType, nullable = true))))

    val exceptDF = inputDF.filter(col("a").isin("0") or col("b") > "c")
    checkAnswer(inputDF.except(exceptDF), Seq(Row("1", null)))
  }

  test("SPARK-26706: Fix Cast.mayTruncate for bytes") {
    val thrownException = intercept[AnalysisException] {
      spark.range(Long.MaxValue - 10, Long.MaxValue).as[Byte]
        .map(b => b - 1)
        .collect()
    }
    assert(thrownException.message.contains("""Cannot up cast id from "BIGINT" to "TINYINT""""))
  }

  test("SPARK-26690: checkpoints should be executed with an execution id") {
    def assertExecutionId: UserDefinedFunction = udf(AssertExecutionId.apply _)
    spark.range(10).select(assertExecutionId($"id")).localCheckpoint(true)
  }

  test("implicit encoder for LocalDate and Instant") {
    val localDate = java.time.LocalDate.of(2019, 3, 30)
    assert(spark.range(1).map { _ => localDate }.head === localDate)

    val instant = java.time.Instant.parse("2019-03-30T09:54:00Z")
    assert(spark.range(1).map { _ => instant }.head === instant)
  }

  val dotColumnTestModes = Table(
    ("caseSensitive", "colName"),
    ("true", "field.1"),
    ("false", "Field.1")
  )

  test("SPARK-25153: Improve error messages for columns with dots/periods") {
    forAll(dotColumnTestModes) { (caseSensitive, colName) =>
      val ds = Seq(SpecialCharClass("1", "2")).toDS
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
        val errorMsg = intercept[AnalysisException] {
          ds(colName)
        }
        assert(errorMsg.getMessage.contains(s"did you mean to quote the `$colName` column?"))
      }
    }
  }

  test("groupBy.as") {
    val df1 = Seq(DoubleData(1, "one"), DoubleData(2, "two"), DoubleData(3, "three")).toDS()
      .repartition($"id").sortWithinPartitions("id")
    val df2 = Seq(DoubleData(5, "one"), DoubleData(1, "two"), DoubleData(3, "three")).toDS()
      .repartition($"id").sortWithinPartitions("id")

    val df3 = df1.groupBy("id").as[Int, DoubleData]
      .cogroup(df2.groupBy("id").as[Int, DoubleData]) { case (key, data1, data2) =>
        if (key == 1) {
          Iterator(DoubleData(key, (data1 ++ data2).foldLeft("")((cur, next) => cur + next.val1)))
        } else Iterator.empty
      }
    checkDataset(df3, DoubleData(1, "onetwo"))

    // Assert that no extra shuffle introduced by cogroup.
    val exchanges = collect(df3.queryExecution.executedPlan) {
      case h: ShuffleExchangeExec => h
    }
    assert(exchanges.size == 2)
  }

  test("tail with different numbers") {
    Seq(0, 2, 5, 10, 50, 100, 1000).foreach { n =>
      assert(spark.range(n).tail(6) === (math.max(n - 6, 0) until n))
    }
  }

  test("tail should not accept minus value") {
    val e = intercept[AnalysisException](spark.range(1).tail(-1))
    e.getMessage.contains("tail expression must be equal to or greater than 0")
  }

  test("SparkSession.active should be the same instance after dataset operations") {
    val active = SparkSession.getActiveSession.get
    val clone = active.cloneSession()
    val ds = new Dataset(clone, spark.range(10).queryExecution.logical, Encoders.INT)

    ds.queryExecution.analyzed

    assert(active eq SparkSession.getActiveSession.get)
  }

  test("SPARK-30791: sameSemantics and semanticHash work") {
    val df1 = Seq((1, 2), (4, 5)).toDF("col1", "col2")
    val df2 = Seq((1, 2), (4, 5)).toDF("col1", "col2")
    val df3 = Seq((0, 2), (4, 5)).toDF("col1", "col2")
    val df4 = Seq((0, 2), (4, 5)).toDF("col0", "col2")

    assert(df1.sameSemantics(df2) === true)
    assert(df1.sameSemantics(df3) === false)
    assert(df3.sameSemantics(df4) === true)

    assert(df1.semanticHash === df2.semanticHash)
    assert(df1.semanticHash !== df3.semanticHash)
    assert(df3.semanticHash === df4.semanticHash)
  }

  test("SPARK-31854: Invoke in MapElementsExec should not propagate null") {
    Seq("true", "false").foreach { wholeStage =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage) {
        val ds = Seq(1.asInstanceOf[Integer], null.asInstanceOf[Integer]).toDS()
        val expectedAnswer = Seq[(Integer, Integer)]((1, 1), (null, null))
        checkDataset(ds.map(v => (v, v)), expectedAnswer: _*)
      }
    }
  }

  test("SPARK-32585: Support scala enumeration in ScalaReflection") {
    checkDataset(
      Seq(FooClassWithEnum(1, FooEnum.E1), FooClassWithEnum(2, FooEnum.E2)).toDS(),
      Seq(FooClassWithEnum(1, FooEnum.E1), FooClassWithEnum(2, FooEnum.E2)): _*
    )

    // test null
    checkDataset(
      Seq(FooClassWithEnum(1, null), FooClassWithEnum(2, FooEnum.E2)).toDS(),
      Seq(FooClassWithEnum(1, null), FooClassWithEnum(2, FooEnum.E2)): _*
    )
  }

  test("SPARK-33390: Make Literal support char array") {
    val df = Seq("aa", "bb", "cc", "abc").toDF("zoo")
    checkAnswer(df.where($"zoo" === Array('a', 'a')), Seq(Row("aa")))
    checkAnswer(
      df.where($"zoo".contains(Array('a', 'b'))),
      Seq(Row("abc")))
  }

  test("SPARK-33469: Add current_timezone function") {
    val df = Seq(1).toDF("c")
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Shanghai") {
      val timezone = df.selectExpr("current_timezone()").collect().head.getString(0)
      assert(timezone == "Asia/Shanghai")
    }
  }

  test("SPARK-34002: Fix broken Option input/output in UDF") {
    def f1(bar: Bar): Option[Bar] = {
      None
    }

    def f2(bar: Option[Bar]): Option[Bar] = {
      bar
    }

    val udf1 = udf(f1 _).withName("f1")
    val udf2 = udf(f2 _).withName("f2")

    val df = (1 to 2).map(i => Tuple1(Bar(1))).toDF("c0")
    val withUDF = df
      .withColumn("c1", udf1(col("c0")))
      .withColumn("c2", udf2(col("c1")))

    assert(withUDF.schema == StructType(
      StructField("c0", StructType(StructField("a", IntegerType, false) :: Nil)) ::
        StructField("c1", StructType(StructField("a", IntegerType, false) :: Nil)) ::
        StructField("c2", StructType(StructField("a", IntegerType, false) :: Nil)) :: Nil))

    checkAnswer(withUDF, Row(Row(1), null, null) :: Row(Row(1), null, null) :: Nil)
  }

  test("SPARK-35664: implicit encoder for java.time.LocalDateTime") {
    val localDateTime = java.time.LocalDateTime.parse("2021-06-08T12:31:58.999999")
    assert(Seq(localDateTime).toDS().head() === localDateTime)
  }

  test("SPARK-34605: implicit encoder for java.time.Duration") {
    val duration = java.time.Duration.ofMinutes(10)
    assert(spark.range(1).map { _ => duration }.head === duration)
  }

  test("SPARK-34615: implicit encoder for java.time.Period") {
    val period = java.time.Period.ofYears(9999).withMonths(11)
    assert(spark.range(1).map { _ => period }.head === period)
  }

  test("SPARK-35652: joinWith on two table generated from same one performing a cartesian join," +
    " which should be inner join") {
    val df = Seq(1, 2, 3).toDS()

    val joined = df.joinWith(df, df("value") === df("value"), "inner")

    val expectedSchema = StructType(Seq(
      StructField("_1", IntegerType, nullable = false),
      StructField("_2", IntegerType, nullable = false)
    ))

    assert(joined.schema === expectedSchema)

    checkDataset(
      joined,
      (1, 1), (2, 2), (3, 3))
  }

  test("SPARK-36210: withColumns preserve insertion ordering") {
    val df = Seq(1, 2, 3).toDS()

    val colNames = (1 to 10).map(i => s"value${i}")
    val cols = (1 to 10).map(i => col("value") + i)

    val inserted = df.withColumns(colNames, cols)

    assert(inserted.columns === "value" +: colNames)

    checkDataset(
      inserted.as[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)],
      (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
      (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
      (3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
  }
}

case class Bar(a: Int)

object AssertExecutionId {
  def apply(id: Long): Long = {
    assert(TaskContext.get().getLocalProperty(SQLExecution.EXECUTION_ID_KEY) != null)
    id
  }
}

case class TestDataUnion(x: Int, y: Int, z: Int)

case class SingleData(id: Int)
case class DoubleData(id: Int, val1: String)
case class TripleData(id: Int, val1: String, val2: Long)

case class WithImmutableMap(id: String, map_test: scala.collection.immutable.Map[Long, String])
case class WithMap(id: String, map_test: scala.collection.Map[Long, String])
case class WithMapInOption(m: Option[scala.collection.Map[Int, Int]])

case class Generic[T](id: T, value: Double)

case class OtherTuple(_1: String, _2: Int)

case class TupleClass(data: (Int, String))

class OuterClass extends Serializable {
  case class InnerClass(a: String)
}

object OuterObject {
  case class InnerClass(a: String)
}

case class ClassData(a: String, b: Int)
case class ClassData2(c: String, d: Int)
case class ClassNullableData(a: String, b: Integer)

case class NestedStruct(f: ClassData)
case class DeepNestedStruct(f: NestedStruct)

/**
 * A class used to test serialization using encoders. This class throws exceptions when using
 * Java serialization -- so the only way it can be "serialized" is through our encoders.
 */
case class NonSerializableCaseClass(value: String) extends Externalizable {
  override def readExternal(in: ObjectInput): Unit = {
    throw new UnsupportedOperationException
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    throw new UnsupportedOperationException
  }
}

/** Used to test Kryo encoder. */
class KryoData(val a: Int) {
  override def equals(other: Any): Boolean = {
    a == other.asInstanceOf[KryoData].a
  }
  override def hashCode: Int = a
  override def toString: String = s"KryoData($a)"
}

object KryoData {
  def apply(a: Int): KryoData = new KryoData(a)
}

/** Used to test Java encoder. */
class JavaData(val a: Int) extends Serializable {
  override def equals(other: Any): Boolean = {
    a == other.asInstanceOf[JavaData].a
  }
  override def hashCode: Int = a
  override def toString: String = s"JavaData($a)"
}

object JavaData {
  def apply(a: Int): JavaData = new JavaData(a)
}

/** Used to test importing dataset.spark.implicits._ */
object DatasetTransform {
  def addOne(ds: Dataset[Int]): Dataset[Int] = {
    import ds.sparkSession.implicits._
    ds.map(_ + 1)
  }
}

case class Route(src: String, dest: String, cost: Int)
case class GroupedRoutes(src: String, dest: String, routes: Seq[Route])

case class CircularReferenceClassA(cls: CircularReferenceClassB)
case class CircularReferenceClassB(cls: CircularReferenceClassA)
case class CircularReferenceClassC(ar: Array[CircularReferenceClassC])
case class CircularReferenceClassD(map: Map[String, CircularReferenceClassE])
case class CircularReferenceClassE(id: String, list: List[CircularReferenceClassD])

case class SpecialCharClass(`field.1`: String, `field 2`: String)

/** Used to test Java Enums from Scala code */
case class SaveModeCase(mode: SaveMode)
case class SaveModeArrayCase(modes: Array[SaveMode])

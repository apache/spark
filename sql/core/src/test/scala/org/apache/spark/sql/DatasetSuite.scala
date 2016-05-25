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

import scala.language.postfixOps

import org.scalatest.words.MatcherWords.be

import org.apache.spark.sql.catalyst.encoders.{OuterScopes, RowEncoder}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DatasetSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("toDS") {
    val data = Seq(("a", 1), ("b", 2), ("c", 3))
    checkDataset(
      data.toDS(),
      data: _*)
  }

  test("toDS with RDD") {
    val ds = sparkContext.makeRDD(Seq("a", "b", "c"), 3).toDS()
    checkDataset(
      ds.mapPartitions(_ => Iterator(1)),
      1, 1, 1)
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
    checkDataset(
      ds.repartition(10),
      data: _*)

    assert(ds.coalesce(1).rdd.partitions.length == 1)
    checkDataset(
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

    checkDataset(
      ds,
      (ClassData("one", 2), 1L), (ClassData("two", 3), 1L))
  }

  test("select") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    checkDataset(
      ds.select(expr("_2 + 1").as[Int]),
      2, 3, 4)
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
    checkDecoding(
      ds.select(
        expr("_1").as[String],
        expr("named_struct('b', _2, 'a', _1)").as[ClassData]),
      ("a", ClassData("a", 1)), ("b", ClassData("b", 2)), ("c", ClassData("c", 3)))
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
      ("b"))
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
    ds.foreachPartition(_.foreach(v => acc.add(v._2)))
    assert(acc.value == 6)
  }

  test("reduce") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    assert(ds.reduce((a, b) => ("sum", a._2 + b._2)) == ("sum", 6))
  }

  test("joinWith, flat schema") {
    val ds1 = Seq(1, 2, 3).toDS().as("a")
    val ds2 = Seq(1, 2).toDS().as("b")

    checkDataset(
      ds1.joinWith(ds2, $"a.value" === $"b.value", "inner"),
      (1, 1), (2, 2))
  }

  test("joinWith, expression condition, outer join") {
    val nullInteger = null.asInstanceOf[Integer]
    val nullString = null.asInstanceOf[String]
    val ds1 = Seq(ClassNullableData("a", 1),
      ClassNullableData("c", 3)).toDS()
    val ds2 = Seq(("a", new Integer(1)),
      ("b", new Integer(2))).toDS()

    checkDataset(
      ds1.joinWith(ds2, $"_1" === $"a", "outer"),
      (ClassNullableData("a", 1), ("a", new Integer(1))),
      (ClassNullableData("c", 3), (nullString, nullInteger)),
      (ClassNullableData(nullString, nullInteger), ("b", new Integer(2))))
  }

  test("joinWith tuple with primitive, expression") {
    val ds1 = Seq(1, 1, 2).toDS()
    val ds2 = Seq(("a", 1), ("b", 2)).toDS()

    checkDataset(
      ds1.joinWith(ds2, $"value" === $"_2"),
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

  test("groupBy function, keys") {
    val ds = Seq(("a", 1), ("b", 1)).toDS()
    val grouped = ds.groupByKey(v => (1, v._2))
    checkDataset(
      grouped.keys,
      (1, 1))
  }

  test("groupBy function, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupByKey(v => (v._1, "word"))
    val agged = grouped.mapGroups { case (g, iter) => (g._1, iter.map(_._2).sum) }

    checkDataset(
      agged,
      ("a", 30), ("b", 3), ("c", 1))
  }

  test("groupBy function, flatMap") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupByKey(v => (v._1, "word"))
    val agged = grouped.flatMapGroups { case (g, iter) =>
      Iterator(g._1, iter.map(_._2).sum.toString)
    }

    checkDataset(
      agged,
      "a", "30", "b", "3", "c", "1")
  }

  test("groupBy function, reduce") {
    val ds = Seq("abc", "xyz", "hello").toDS()
    val agged = ds.groupByKey(_.length).reduceGroups(_ + _)

    checkDataset(
      agged,
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

    checkDataset(
      ds.groupByKey(_._1).agg(sum("_2").as[Long]),
      ("a", 30L), ("b", 3L), ("c", 1L))
  }

  test("typed aggregation: expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDataset(
      ds.groupByKey(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long]),
      ("a", 30L, 32L), ("b", 3L, 5L), ("c", 1L, 2L))
  }

  test("typed aggregation: expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDataset(
      ds.groupByKey(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long], count("*")),
      ("a", 30L, 32L, 2L), ("b", 3L, 5L, 2L), ("c", 1L, 2L, 1L))
  }

  test("typed aggregation: expr, expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDataset(
      ds.groupByKey(_._1).agg(
        sum("_2").as[Long],
        sum($"_2" + 1).as[Long],
        count("*").as[Long],
        avg("_2").as[Double]),
      ("a", 30L, 32L, 2L, 15.0), ("b", 3L, 5L, 2L, 1.5), ("c", 1L, 2L, 1L, 1.0))
  }

  test("cogroup") {
    val ds1 = Seq(1 -> "a", 3 -> "abc", 5 -> "hello", 3 -> "foo").toDS()
    val ds2 = Seq(2 -> "q", 3 -> "w", 5 -> "e", 5 -> "r").toDS()
    val cogrouped = ds1.groupByKey(_._1).cogroup(ds2.groupByKey(_._1)) { case (key, data1, data2) =>
      Iterator(key -> (data1.map(_._2).mkString + "#" + data2.map(_._2).mkString))
    }

    checkDataset(
      cogrouped,
      1 -> "a#", 2 -> "#q", 3 -> "abcfoo#w", 5 -> "hello#er")
  }

  test("cogroup with complex data") {
    val ds1 = Seq(1 -> ClassData("a", 1), 2 -> ClassData("b", 2)).toDS()
    val ds2 = Seq(2 -> ClassData("c", 3), 3 -> ClassData("d", 4)).toDS()
    val cogrouped = ds1.groupByKey(_._1).cogroup(ds2.groupByKey(_._1)) { case (key, data1, data2) =>
      Iterator(key -> (data1.map(_._2.a).mkString + data2.map(_._2.a).mkString))
    }

    checkDataset(
      cogrouped,
      1 -> "a", 2 -> "bc", 3 -> "d")
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
      3, 17, 27, 58, 62)
  }

  test("SPARK-11436: we should rebind right encoder when join 2 datasets") {
    val ds1 = Seq("1", "2").toDS().as("a")
    val ds2 = Seq(2, 3).toDS().as("b")

    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value")
    checkDataset(joined, ("2", 2))
  }

  test("self join") {
    val ds = Seq("1", "2").toDS().as("a")
    val joined = ds.joinWith(ds, lit(true))
    checkDataset(joined, ("1", "1"), ("1", "2"), ("2", "1"), ("2", "2"))
  }

  test("toString") {
    val ds = Seq((1, 2)).toDS()
    assert(ds.toString == "[_1: int, _2: int]")
  }

  test("showString: Kryo encoder") {
    implicit val kryoEncoder = Encoders.kryo[KryoData]
    val ds = Seq(KryoData(1), KryoData(2)).toDS()

    val expectedAnswer = """+-----------+
                           ||      value|
                           |+-----------+
                           ||KryoData(1)|
                           ||KryoData(2)|
                           |+-----------+
                           |""".stripMargin
    assert(ds.showString(10) === expectedAnswer)
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
    assert(ds.joinWith(ds, lit(true)).collect().toSet ==
      Set(
        (KryoData(1), KryoData(1)),
        (KryoData(1), KryoData(2)),
        (KryoData(2), KryoData(1)),
        (KryoData(2), KryoData(2))))
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
    assert(ds.joinWith(ds, lit(true)).collect().toSet ==
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
    val ds1 = Seq((nullInt, "1"), (new java.lang.Integer(22), "2")).toDS()
    val ds2 = Seq((nullInt, "1"), (new java.lang.Integer(22), "2")).toDS()

    checkDataset(
      ds1.joinWith(ds2, lit(true)),
      ((nullInt, "1"), (nullInt, "1")),
      ((new java.lang.Integer(22), "2"), (nullInt, "1")),
      ((nullInt, "1"), (new java.lang.Integer(22), "2")),
      ((new java.lang.Integer(22), "2"), (new java.lang.Integer(22), "2")))
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
    assert(e.getMessage.contains("cannot resolve '`c`' given input columns: [a, b]"), e.getMessage)
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
    val agged = ds.groupByKey(d => ClassNullableData(d.a, null)).mapGroups {
      case (key, values) => key.a + values.map(_.b).sum
    }

    checkDataset(agged, "a3")
  }

  test("cogroup's left and right side has field with same name") {
    val left = Seq(ClassData("a", 1), ClassData("b", 2)).toDS()
    val right = Seq(ClassNullableData("a", 3), ClassNullableData("b", 4)).toDS()
    val cogrouped = left.groupByKey(_.a).cogroup(right.groupByKey(_.a)) {
      case (key, lData, rData) => Iterator(key + lData.map(_.b).sum + rData.map(_.b.toInt).sum)
    }

    checkDataset(cogrouped, "a13", "b24")
  }

  test("give nice error message when the real number of fields doesn't match encoder schema") {
    val ds = Seq(ClassData("a", 1), ClassData("b", 2)).toDS()

    val message = intercept[AnalysisException] {
      ds.as[(String, Int, Long)]
    }.message
    assert(message ==
      "Try to map struct<a:string,b:int> to Tuple3, " +
        "but failed as the number of fields does not line up.\n" +
        " - Input schema: struct<a:string,b:int>\n" +
        " - Target schema: struct<_1:string,_2:int,_3:bigint>")

    val message2 = intercept[AnalysisException] {
      ds.as[Tuple1[String]]
    }.message
    assert(message2 ==
      "Try to map struct<a:string,b:int> to Tuple1, " +
        "but failed as the number of fields does not line up.\n" +
        " - Input schema: struct<a:string,b:int>\n" +
        " - Target schema: struct<_1:string>")
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
      TupleClass(1, "a")
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
    val wideDF = spark.range(10).select(Seq.tabulate(1000) {i => ('id + i).as(s"c$i")} : _*)
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
      .mapGroups { case (src, ids) => (src, ids.map(_._2).toArray) }
      .toDF("id", "actual")

    dataset.join(actual, dataset("user") === actual("id")).collect()
  }

  test("SPARK-15097: implicits on dataset's spark can be imported") {
    val dataset = Seq(1, 2, 3).toDS()
    checkDataset(DatasetTransform.addOne(dataset), 2, 3, 4)
  }

  test("dataset.rdd with generic case class") {
    val ds = Seq(Generic(1, 1.0), Generic(2, 2.0)).toDS
    val ds2 = ds.map(g => Generic(g.id, g.value))
    assert(ds.rdd.map(r => r.id).count === 2)
    assert(ds2.rdd.map(r => r.id).count === 2)

    val ds3 = ds.map(g => new java.lang.Long(g.id))
    assert(ds3.rdd.map(r => r).count === 2)
  }

  test("runtime null check for RowEncoder") {
    val schema = new StructType().add("i", IntegerType, nullable = false)
    val df = sqlContext.range(10).map(l => {
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
    val rdd = sqlContext.sparkContext.parallelize(Row(null, "123") :: Row("234", null) :: Nil)
    val message = intercept[Exception] {
      sqlContext.createDataFrame(rdd, schema).collect()
    }.getMessage
    assert(message.contains("The 1th field 'b' of input row cannot be null"))
  }

  test("createTempView") {
    val dataset = Seq(1, 2, 3).toDS()
    dataset.createOrReplaceTempView("tempView")

    // Overrrides the existing temporary view with same name
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
}

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

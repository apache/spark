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

import java.io.{ObjectInput, ObjectOutput, Externalizable}
import java.sql.{Date, Timestamp}

import scala.language.postfixOps

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


class DatasetSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("toDS") {
    val data = Seq(("a", 1) , ("b", 2), ("c", 3))
    checkAnswer(
      data.toDS(),
      data: _*)
  }

  test("toDS with RDD") {
    val ds = sparkContext.makeRDD(Seq("a", "b", "c"), 3).toDS()
    checkAnswer(
      ds.mapPartitions(_ => Iterator(1)),
      1, 1, 1)
  }


  test("SPARK-12404: Datatype Helper Serializablity") {
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
  }

  test("coalesce, repartition") {
    val data = (1 to 100).map(i => ClassData(i.toString, i))
    val ds = data.toDS()

    assert(ds.repartition(10).rdd.partitions.length == 10)
    checkAnswer(
      ds.repartition(10),
      data: _*)

    assert(ds.coalesce(1).rdd.partitions.length == 1)
    checkAnswer(
      ds.coalesce(1),
      data: _*)
  }

  test("as tuple") {
    val data = Seq(("a", 1), ("b", 2)).toDF("a", "b")
    checkAnswer(
      data.as[(String, Int)],
      ("a", 1), ("b", 2))
  }

  test("as case class / collect") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDF("a", "b").as[ClassData]
    checkAnswer(
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
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.map(v => (v._1, v._2 + 1)),
      ("a", 2), ("b", 3), ("c", 4))
  }

  test("map and group by with class data") {
    // We inject a group by here to make sure this test case is future proof
    // when we implement better pipelining and local execution mode.
    val ds: Dataset[(ClassData, Long)] = Seq(ClassData("one", 1), ClassData("two", 2)).toDS()
        .map(c => ClassData(c.a, c.b + 1))
        .groupBy(p => p).count()

    checkAnswer(
      ds,
      (ClassData("one", 2), 1L), (ClassData("two", 3), 1L))
  }

  test("select") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.select(expr("_2 + 1").as[Int]),
      2, 3, 4)
  }

  test("select 2") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.select(
        expr("_1").as[String],
        expr("_2").as[Int]) : Dataset[(String, Int)],
      ("a", 1), ("b", 2), ("c", 3))
  }

  test("select 2, primitive and tuple") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.select(
        expr("_1").as[String],
        expr("struct(_2, _2)").as[(Int, Int)]),
      ("a", (1, 1)), ("b", (2, 2)), ("c", (3, 3)))
  }

  test("select 2, primitive and class") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.select(
        expr("_1").as[String],
        expr("named_struct('a', _1, 'b', _2)").as[ClassData]),
      ("a", ClassData("a", 1)), ("b", ClassData("b", 2)), ("c", ClassData("c", 3)))
  }

  test("select 2, primitive and class, fields reordered") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkDecoding(
      ds.select(
        expr("_1").as[String],
        expr("named_struct('b', _2, 'a', _1)").as[ClassData]),
      ("a", ClassData("a", 1)), ("b", ClassData("b", 2)), ("c", ClassData("c", 3)))
  }

  test("filter") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    checkAnswer(
      ds.filter(_._1 == "b"),
      ("b", 2))
  }

  test("foreach") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    val acc = sparkContext.accumulator(0)
    ds.foreach(v => acc += v._2)
    assert(acc.value == 6)
  }

  test("foreachPartition") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    val acc = sparkContext.accumulator(0)
    ds.foreachPartition(_.foreach(v => acc += v._2))
    assert(acc.value == 6)
  }

  test("reduce") {
    val ds = Seq(("a", 1) , ("b", 2), ("c", 3)).toDS()
    assert(ds.reduce((a, b) => ("sum", a._2 + b._2)) == ("sum", 6))
  }

  test("joinWith, flat schema") {
    val ds1 = Seq(1, 2, 3).toDS().as("a")
    val ds2 = Seq(1, 2).toDS().as("b")

    checkAnswer(
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

    checkAnswer(
      ds1.joinWith(ds2, $"_1" === $"a", "outer"),
      (ClassNullableData("a", 1), ("a", new Integer(1))),
      (ClassNullableData("c", 3), (nullString, nullInteger)),
      (ClassNullableData(nullString, nullInteger), ("b", new Integer(2))))
  }

  test("joinWith tuple with primitive, expression") {
    val ds1 = Seq(1, 1, 2).toDS()
    val ds2 = Seq(("a", 1), ("b", 2)).toDS()

    checkAnswer(
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

    checkAnswer(
      ds1.joinWith(ds2, $"a._2" === $"b._2").as("ab").joinWith(ds3, $"ab._1._2" === $"c._2"),
      ((("a", 1), ("a", 1)), ("a", 1)),
      ((("b", 2), ("b", 2)), ("b", 2)))
  }

  test("groupBy function, keys") {
    val ds = Seq(("a", 1), ("b", 1)).toDS()
    val grouped = ds.groupBy(v => (1, v._2))
    checkAnswer(
      grouped.keys,
      (1, 1))
  }

  test("groupBy function, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy(v => (v._1, "word"))
    val agged = grouped.mapGroups { case (g, iter) => (g._1, iter.map(_._2).sum) }

    checkAnswer(
      agged,
      ("a", 30), ("b", 3), ("c", 1))
  }

  test("groupBy function, flatMap") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy(v => (v._1, "word"))
    val agged = grouped.flatMapGroups { case (g, iter) =>
      Iterator(g._1, iter.map(_._2).sum.toString)
    }

    checkAnswer(
      agged,
      "a", "30", "b", "3", "c", "1")
  }

  test("groupBy function, reduce") {
    val ds = Seq("abc", "xyz", "hello").toDS()
    val agged = ds.groupBy(_.length).reduce(_ + _)

    checkAnswer(
      agged,
      3 -> "abcxyz", 5 -> "hello")
  }

  test("groupBy single field class, count") {
    val ds = Seq("abc", "xyz", "hello").toDS()
    val count = ds.groupBy(s => Tuple1(s.length)).count()

    checkAnswer(
      count,
      (Tuple1(3), 2L), (Tuple1(5), 1L)
    )
  }

  test("groupBy columns, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy($"_1")
    val agged = grouped.mapGroups { case (g, iter) => (g.getString(0), iter.map(_._2).sum) }

    checkAnswer(
      agged,
      ("a", 30), ("b", 3), ("c", 1))
  }

  test("groupBy columns, count") {
    val ds = Seq("a" -> 1, "b" -> 1, "a" -> 2).toDS()
    val count = ds.groupBy($"_1").count()

    checkAnswer(
      count,
      (Row("a"), 2L), (Row("b"), 1L))
  }

  test("groupBy columns asKey, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy($"_1").keyAs[String]
    val agged = grouped.mapGroups { case (g, iter) => (g, iter.map(_._2).sum) }

    checkAnswer(
      agged,
      ("a", 30), ("b", 3), ("c", 1))
  }

  test("groupBy columns asKey tuple, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy($"_1", lit(1)).keyAs[(String, Int)]
    val agged = grouped.mapGroups { case (g, iter) => (g, iter.map(_._2).sum) }

    checkAnswer(
      agged,
      (("a", 1), 30), (("b", 1), 3), (("c", 1), 1))
  }

  test("groupBy columns asKey class, map") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupBy($"_1".as("a"), lit(1).as("b")).keyAs[ClassData]
    val agged = grouped.mapGroups { case (g, iter) => (g, iter.map(_._2).sum) }

    checkAnswer(
      agged,
      (ClassData("a", 1), 30), (ClassData("b", 1), 3), (ClassData("c", 1), 1))
  }

  test("typed aggregation: expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkAnswer(
      ds.groupBy(_._1).agg(sum("_2").as[Long]),
      ("a", 30L), ("b", 3L), ("c", 1L))
  }

  test("typed aggregation: expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkAnswer(
      ds.groupBy(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long]),
      ("a", 30L, 32L), ("b", 3L, 5L), ("c", 1L, 2L))
  }

  test("typed aggregation: expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkAnswer(
      ds.groupBy(_._1).agg(sum("_2").as[Long], sum($"_2" + 1).as[Long], count("*")),
      ("a", 30L, 32L, 2L), ("b", 3L, 5L, 2L), ("c", 1L, 2L, 1L))
  }

  test("typed aggregation: expr, expr, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkAnswer(
      ds.groupBy(_._1).agg(
        sum("_2").as[Long],
        sum($"_2" + 1).as[Long],
        count("*").as[Long],
        avg("_2").as[Double]),
      ("a", 30L, 32L, 2L, 15.0), ("b", 3L, 5L, 2L, 1.5), ("c", 1L, 2L, 1L, 1.0))
  }

  test("cogroup") {
    val ds1 = Seq(1 -> "a", 3 -> "abc", 5 -> "hello", 3 -> "foo").toDS()
    val ds2 = Seq(2 -> "q", 3 -> "w", 5 -> "e", 5 -> "r").toDS()
    val cogrouped = ds1.groupBy(_._1).cogroup(ds2.groupBy(_._1)) { case (key, data1, data2) =>
      Iterator(key -> (data1.map(_._2).mkString + "#" + data2.map(_._2).mkString))
    }

    checkAnswer(
      cogrouped,
      1 -> "a#", 2 -> "#q", 3 -> "abcfoo#w", 5 -> "hello#er")
  }

  test("cogroup with complex data") {
    val ds1 = Seq(1 -> ClassData("a", 1), 2 -> ClassData("b", 2)).toDS()
    val ds2 = Seq(2 -> ClassData("c", 3), 3 -> ClassData("d", 4)).toDS()
    val cogrouped = ds1.groupBy(_._1).cogroup(ds2.groupBy(_._1)) { case (key, data1, data2) =>
      Iterator(key -> (data1.map(_._2.a).mkString + data2.map(_._2.a).mkString))
    }

    checkAnswer(
      cogrouped,
      1 -> "a", 2 -> "bc", 3 -> "d")
  }

  test("sample with replacement") {
    val n = 100
    val data = sparkContext.parallelize(1 to n, 2).toDS()
    checkAnswer(
      data.sample(withReplacement = true, 0.05, seed = 13),
      5, 10, 52, 73)
  }

  test("sample without replacement") {
    val n = 100
    val data = sparkContext.parallelize(1 to n, 2).toDS()
    checkAnswer(
      data.sample(withReplacement = false, 0.05, seed = 13),
      3, 17, 27, 58, 62)
  }

  test("SPARK-11436: we should rebind right encoder when join 2 datasets") {
    val ds1 = Seq("1", "2").toDS().as("a")
    val ds2 = Seq(2, 3).toDS().as("b")

    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value")
    checkAnswer(joined, ("2", 2))
  }

  test("self join") {
    val ds = Seq("1", "2").toDS().as("a")
    val joined = ds.joinWith(ds, lit(true))
    checkAnswer(joined, ("1", "1"), ("1", "2"), ("2", "1"), ("2", "2"))
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

    assert(ds.groupBy(p => p).count().collect().toSeq ==
      Seq((KryoData(1), 1L), (KryoData(2), 1L)))
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

    assert(ds.groupBy(p => p).count().collect().toSeq ==
      Seq((JavaData(1), 1L), (JavaData(2), 1L)))
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

  test("SPARK-11894: Incorrect results are returned when using null") {
    val nullInt = null.asInstanceOf[java.lang.Integer]
    val ds1 = Seq((nullInt, "1"), (new java.lang.Integer(22), "2")).toDS()
    val ds2 = Seq((nullInt, "1"), (new java.lang.Integer(22), "2")).toDS()

    checkAnswer(
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
      ds.as[ClassData2].collect()
    }
    assert(e.getMessage.contains("cannot resolve 'c' given input columns: [a, b]"), e.getMessage)
  }

  test("runtime nullability check") {
    val schema = StructType(Seq(
      StructField("f", StructType(Seq(
        StructField("a", StringType, nullable = true),
        StructField("b", IntegerType, nullable = false)
      )), nullable = true)
    ))

    def buildDataset(rows: Row*): Dataset[NestedStruct] = {
      val rowRDD = sqlContext.sparkContext.parallelize(rows)
      sqlContext.createDataFrame(rowRDD, schema).as[NestedStruct]
    }

    checkAnswer(
      buildDataset(Row(Row("hello", 1))),
      NestedStruct(ClassData("hello", 1))
    )

    // Shouldn't throw runtime exception when parent object (`ClassData`) is null
    assert(buildDataset(Row(null)).collect() === Array(NestedStruct(null)))

    val message = intercept[RuntimeException] {
      buildDataset(Row(Row("hello", null))).collect()
    }.getMessage

    assert(message.contains(
      "Null value appeared in non-nullable field org.apache.spark.sql.ClassData.b of type Int."
    ))
  }

  test("SPARK-12478: top level null field") {
    val ds0 = Seq(NestedStruct(null)).toDS()
    checkAnswer(ds0, NestedStruct(null))
    checkAnswer(ds0.toDF(), Row(null))

    val ds1 = Seq(DeepNestedStruct(NestedStruct(null))).toDS()
    checkAnswer(ds1, DeepNestedStruct(NestedStruct(null)))
    checkAnswer(ds1.toDF(), Row(Row(null)))
  }
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

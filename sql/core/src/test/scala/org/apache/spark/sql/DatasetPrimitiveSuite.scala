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

import scala.collection.immutable.{HashSet => HSet}
import scala.collection.immutable.Queue
import scala.collection.mutable.{LinkedHashMap => LHMap}

import org.apache.spark.sql.test.SharedSparkSession

case class IntClass(value: Int)

case class SeqClass(s: Seq[Int])

case class ListClass(l: List[Int])

case class QueueClass(q: Queue[Int])

case class MapClass(m: Map[Int, Int])

case class LHMapClass(m: LHMap[Int, Int])

case class ComplexClass(seq: SeqClass, list: ListClass, queue: QueueClass)

case class ComplexMapClass(map: MapClass, lhmap: LHMapClass)

case class InnerData(name: String, value: Int)
case class NestedData(id: Int, param: Map[String, InnerData])

package object packageobject {
  case class PackageClass(value: Int)
}

class DatasetPrimitiveSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("toDS") {
    val data = Seq(1, 2, 3, 4, 5, 6)
    checkDataset(
      data.toDS(),
      data: _*)
  }

  test("as case class / collect") {
    val ds = Seq(1, 2, 3).toDS().as[IntClass]
    checkDataset(
      ds,
      IntClass(1), IntClass(2), IntClass(3))

    assert(ds.collect().head == IntClass(1))
  }

  test("map") {
    val ds = Seq(1, 2, 3).toDS()
    checkDataset(
      ds.map(_ + 1),
      2, 3, 4)
  }

  test("mapPrimitive") {
    val dsInt = Seq(1, 2, 3).toDS()
    checkDataset(dsInt.map(_ > 1), false, true, true)
    checkDataset(dsInt.map(_ + 1), 2, 3, 4)
    checkDataset(dsInt.map(_ + 8589934592L), 8589934593L, 8589934594L, 8589934595L)
    checkDataset(dsInt.map(_ + 1.1F), 2.1F, 3.1F, 4.1F)
    checkDataset(dsInt.map(_ + 1.23D), 2.23D, 3.23D, 4.23D)

    val dsLong = Seq(1L, 2L, 3L).toDS()
    checkDataset(dsLong.map(_ > 1), false, true, true)
    checkDataset(dsLong.map(e => (e + 1).toInt), 2, 3, 4)
    checkDataset(dsLong.map(_ + 8589934592L), 8589934593L, 8589934594L, 8589934595L)
    checkDataset(dsLong.map(_ + 1.1F), 2.1F, 3.1F, 4.1F)
    checkDataset(dsLong.map(_ + 1.23D), 2.23D, 3.23D, 4.23D)

    val dsFloat = Seq(1F, 2F, 3F).toDS()
    checkDataset(dsFloat.map(_ > 1), false, true, true)
    checkDataset(dsFloat.map(e => (e + 1).toInt), 2, 3, 4)
    checkDataset(dsFloat.map(e => (e + 123456L).toLong), 123457L, 123458L, 123459L)
    checkDataset(dsFloat.map(_ + 1.1F), 2.1F, 3.1F, 4.1F)
    checkDataset(dsFloat.map(_ + 1.23D), 2.23D, 3.23D, 4.23D)

    val dsDouble = Seq(1D, 2D, 3D).toDS()
    checkDataset(dsDouble.map(_ > 1), false, true, true)
    checkDataset(dsDouble.map(e => (e + 1).toInt), 2, 3, 4)
    checkDataset(dsDouble.map(e => (e + 8589934592L).toLong),
      8589934593L, 8589934594L, 8589934595L)
    checkDataset(dsDouble.map(e => (e + 1.1F).toFloat), 2.1F, 3.1F, 4.1F)
    checkDataset(dsDouble.map(_ + 1.23D), 2.23D, 3.23D, 4.23D)

    val dsBoolean = Seq(true, false).toDS()
    checkDataset(dsBoolean.map(e => !e), false, true)
  }

  test("mapPrimitiveArray") {
    val dsInt = Seq(Array(1, 2), Array(3, 4)).toDS()
    checkDataset(dsInt.map(e => e), Array(1, 2), Array(3, 4))
    checkDataset(dsInt.map(e => null: Array[Int]), null, null)

    val dsDouble = Seq(Array(1D, 2D), Array(3D, 4D)).toDS()
    checkDataset(dsDouble.map(e => e), Array(1D, 2D), Array(3D, 4D))
    checkDataset(dsDouble.map(e => null: Array[Double]), null, null)
  }

  test("filter") {
    val ds = Seq(1, 2, 3, 4).toDS()
    checkDataset(
      ds.filter(_ % 2 == 0),
      2, 4)
  }

  test("filterPrimitive") {
    val dsInt = Seq(1, 2, 3).toDS()
    checkDataset(dsInt.filter(_ > 1), 2, 3)

    val dsLong = Seq(1L, 2L, 3L).toDS()
    checkDataset(dsLong.filter(_ > 1), 2L, 3L)

    val dsFloat = Seq(1F, 2F, 3F).toDS()
    checkDataset(dsFloat.filter(_ > 1), 2F, 3F)

    val dsDouble = Seq(1D, 2D, 3D).toDS()
    checkDataset(dsDouble.filter(_ > 1), 2D, 3D)

    val dsBoolean = Seq(true, false).toDS()
    checkDataset(dsBoolean.filter(e => !e), false)
  }

  test("foreach") {
    val ds = Seq(1, 2, 3).toDS()
    val acc = sparkContext.longAccumulator
    ds.foreach(acc.add(_))
    assert(acc.value == 6)
  }

  test("foreachPartition") {
    val ds = Seq(1, 2, 3).toDS()
    val acc = sparkContext.longAccumulator
    ds.foreachPartition((it: Iterator[Int]) => it.foreach(acc.add(_)))
    assert(acc.value == 6)
  }

  test("reduce") {
    val ds = Seq(1, 2, 3).toDS()
    assert(ds.reduce(_ + _) == 6)
  }

  test("groupBy function, keys") {
    val ds = Seq(1, 2, 3, 4, 5).toDS()
    val grouped = ds.groupByKey(_ % 2)
    checkDatasetUnorderly(
      grouped.keys,
      0, 1)
  }

  test("groupBy function, map") {
    val ds = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11).toDS()
    val grouped = ds.groupByKey(_ % 2)
    val aggregated = grouped.mapGroups { (g, iter) =>
      val name = if (g == 0) "even" else "odd"
      (name, iter.size)
    }

    checkDatasetUnorderly(
      aggregated,
      ("even", 5), ("odd", 6))
  }

  test("groupBy function, flatMap") {
    val ds = Seq("a", "b", "c", "xyz", "hello").toDS()
    val grouped = ds.groupByKey(_.length)
    val aggregated = grouped.flatMapGroups { (g, iter) => Iterator(g.toString, iter.mkString) }

    checkDatasetUnorderly(
      aggregated,
      "1", "abc", "3", "xyz", "5", "hello")
  }

  test("Arrays and Lists") {
    checkDataset(Seq(Seq(1)).toDS(), Seq(1))
    checkDataset(Seq(Seq(1.toLong)).toDS(), Seq(1.toLong))
    checkDataset(Seq(Seq(1.toDouble)).toDS(), Seq(1.toDouble))
    checkDataset(Seq(Seq(1.toFloat)).toDS(), Seq(1.toFloat))
    checkDataset(Seq(Seq(1.toByte)).toDS(), Seq(1.toByte))
    checkDataset(Seq(Seq(1.toShort)).toDS(), Seq(1.toShort))
    checkDataset(Seq(Seq(true)).toDS(), Seq(true))
    checkDataset(Seq(Seq("test")).toDS(), Seq("test"))
    checkDataset(Seq(Seq(Tuple1(1))).toDS(), Seq(Tuple1(1)))

    checkDataset(Seq(Array(1)).toDS(), Array(1))
    checkDataset(Seq(Array(1.toLong)).toDS(), Array(1.toLong))
    checkDataset(Seq(Array(1.toDouble)).toDS(), Array(1.toDouble))
    checkDataset(Seq(Array(1.toFloat)).toDS(), Array(1.toFloat))
    checkDataset(Seq(Array(1.toByte)).toDS(), Array(1.toByte))
    checkDataset(Seq(Array(1.toShort)).toDS(), Array(1.toShort))
    checkDataset(Seq(Array(true)).toDS(), Array(true))
    checkDataset(Seq(Array("test")).toDS(), Array("test"))
    checkDataset(Seq(Array(Tuple1(1))).toDS(), Array(Tuple1(1)))
  }

  test("arbitrary sequences") {
    checkDataset(Seq(Queue(1)).toDS(), Queue(1))
    checkDataset(Seq(Queue(1.toLong)).toDS(), Queue(1.toLong))
    checkDataset(Seq(Queue(1.toDouble)).toDS(), Queue(1.toDouble))
    checkDataset(Seq(Queue(1.toFloat)).toDS(), Queue(1.toFloat))
    checkDataset(Seq(Queue(1.toByte)).toDS(), Queue(1.toByte))
    checkDataset(Seq(Queue(1.toShort)).toDS(), Queue(1.toShort))
    checkDataset(Seq(Queue(true)).toDS(), Queue(true))
    checkDataset(Seq(Queue("test")).toDS(), Queue("test"))
    checkDataset(Seq(Queue(Tuple1(1))).toDS(), Queue(Tuple1(1)))
  }

  test("sequence and product combinations") {
    // Case classes
    checkDataset(Seq(SeqClass(Seq(1))).toDS(), SeqClass(Seq(1)))
    checkDataset(Seq(Seq(SeqClass(Seq(1)))).toDS(), Seq(SeqClass(Seq(1))))
    checkDataset(Seq(List(SeqClass(Seq(1)))).toDS(), List(SeqClass(Seq(1))))
    checkDataset(Seq(Queue(SeqClass(Seq(1)))).toDS(), Queue(SeqClass(Seq(1))))

    checkDataset(Seq(ListClass(List(1))).toDS(), ListClass(List(1)))
    checkDataset(Seq(Seq(ListClass(List(1)))).toDS(), Seq(ListClass(List(1))))
    checkDataset(Seq(List(ListClass(List(1)))).toDS(), List(ListClass(List(1))))
    checkDataset(Seq(Queue(ListClass(List(1)))).toDS(), Queue(ListClass(List(1))))

    checkDataset(Seq(QueueClass(Queue(1))).toDS(), QueueClass(Queue(1)))
    checkDataset(Seq(Seq(QueueClass(Queue(1)))).toDS(), Seq(QueueClass(Queue(1))))
    checkDataset(Seq(List(QueueClass(Queue(1)))).toDS(), List(QueueClass(Queue(1))))
    checkDataset(Seq(Queue(QueueClass(Queue(1)))).toDS(), Queue(QueueClass(Queue(1))))

    val complex = ComplexClass(SeqClass(Seq(1)), ListClass(List(2)), QueueClass(Queue(3)))
    checkDataset(Seq(complex).toDS(), complex)
    checkDataset(Seq(Seq(complex)).toDS(), Seq(complex))
    checkDataset(Seq(List(complex)).toDS(), List(complex))
    checkDataset(Seq(Queue(complex)).toDS(), Queue(complex))

    // Tuples
    checkDataset(Seq(Seq(1) -> Seq(2)).toDS(), Seq(1) -> Seq(2))
    checkDataset(Seq(List(1) -> Queue(2)).toDS(), List(1) -> Queue(2))
    checkDataset(Seq(List(Seq("test1") -> List(Queue("test2")))).toDS(),
      List(Seq("test1") -> List(Queue("test2"))))

    // Complex
    checkDataset(Seq(ListClass(List(1)) -> Queue("test" -> SeqClass(Seq(2)))).toDS(),
      ListClass(List(1)) -> Queue("test" -> SeqClass(Seq(2))))
  }

  test("arbitrary maps") {
    checkDataset(Seq(Map(1 -> 2)).toDS(), Map(1 -> 2))
    checkDataset(Seq(Map(1.toLong -> 2.toLong)).toDS(), Map(1.toLong -> 2.toLong))
    checkDataset(Seq(Map(1.toDouble -> 2.toDouble)).toDS(), Map(1.toDouble -> 2.toDouble))
    checkDataset(Seq(Map(1.toFloat -> 2.toFloat)).toDS(), Map(1.toFloat -> 2.toFloat))
    checkDataset(Seq(Map(1.toByte -> 2.toByte)).toDS(), Map(1.toByte -> 2.toByte))
    checkDataset(Seq(Map(1.toShort -> 2.toShort)).toDS(), Map(1.toShort -> 2.toShort))
    checkDataset(Seq(Map(true -> false)).toDS(), Map(true -> false))
    checkDataset(Seq(Map("test1" -> "test2")).toDS(), Map("test1" -> "test2"))
    checkDataset(Seq(Map(Tuple1(1) -> Tuple1(2))).toDS(), Map(Tuple1(1) -> Tuple1(2)))
    checkDataset(Seq(Map(1 -> Tuple1(2))).toDS(), Map(1 -> Tuple1(2)))
    checkDataset(Seq(Map("test" -> 2.toLong)).toDS(), Map("test" -> 2.toLong))

    checkDataset(Seq(LHMap(1 -> 2)).toDS(), LHMap(1 -> 2))
    checkDataset(Seq(LHMap(1.toLong -> 2.toLong)).toDS(), LHMap(1.toLong -> 2.toLong))
    checkDataset(Seq(LHMap(1.toDouble -> 2.toDouble)).toDS(), LHMap(1.toDouble -> 2.toDouble))
    checkDataset(Seq(LHMap(1.toFloat -> 2.toFloat)).toDS(), LHMap(1.toFloat -> 2.toFloat))
    checkDataset(Seq(LHMap(1.toByte -> 2.toByte)).toDS(), LHMap(1.toByte -> 2.toByte))
    checkDataset(Seq(LHMap(1.toShort -> 2.toShort)).toDS(), LHMap(1.toShort -> 2.toShort))
    checkDataset(Seq(LHMap(true -> false)).toDS(), LHMap(true -> false))
    checkDataset(Seq(LHMap("test1" -> "test2")).toDS(), LHMap("test1" -> "test2"))
    checkDataset(Seq(LHMap(Tuple1(1) -> Tuple1(2))).toDS(), LHMap(Tuple1(1) -> Tuple1(2)))
    checkDataset(Seq(LHMap(1 -> Tuple1(2))).toDS(), LHMap(1 -> Tuple1(2)))
    checkDataset(Seq(LHMap("test" -> 2.toLong)).toDS(), LHMap("test" -> 2.toLong))
  }

  test("SPARK-25817: map and product combinations") {
    // Case classes
    checkDataset(Seq(MapClass(Map(1 -> 2))).toDS(), MapClass(Map(1 -> 2)))
    checkDataset(Seq(Map(1 -> MapClass(Map(2 -> 3)))).toDS(), Map(1 -> MapClass(Map(2 -> 3))))
    checkDataset(Seq(Map(MapClass(Map(1 -> 2)) -> 3)).toDS(), Map(MapClass(Map(1 -> 2)) -> 3))
    checkDataset(Seq(Map(MapClass(Map(1 -> 2)) -> MapClass(Map(3 -> 4)))).toDS(),
      Map(MapClass(Map(1 -> 2)) -> MapClass(Map(3 -> 4))))
    checkDataset(Seq(LHMap(1 -> MapClass(Map(2 -> 3)))).toDS(), LHMap(1 -> MapClass(Map(2 -> 3))))
    checkDataset(Seq(LHMap(MapClass(Map(1 -> 2)) -> 3)).toDS(), LHMap(MapClass(Map(1 -> 2)) -> 3))
    checkDataset(Seq(LHMap(MapClass(Map(1 -> 2)) -> MapClass(Map(3 -> 4)))).toDS(),
      LHMap(MapClass(Map(1 -> 2)) -> MapClass(Map(3 -> 4))))

    checkDataset(Seq(LHMapClass(LHMap(1 -> 2))).toDS(), LHMapClass(LHMap(1 -> 2)))
    checkDataset(Seq(Map(1 -> LHMapClass(LHMap(2 -> 3)))).toDS(),
      Map(1 -> LHMapClass(LHMap(2 -> 3))))
    checkDataset(Seq(Map(LHMapClass(LHMap(1 -> 2)) -> 3)).toDS(),
      Map(LHMapClass(LHMap(1 -> 2)) -> 3))
    checkDataset(Seq(Map(LHMapClass(LHMap(1 -> 2)) -> LHMapClass(LHMap(3 -> 4)))).toDS(),
      Map(LHMapClass(LHMap(1 -> 2)) -> LHMapClass(LHMap(3 -> 4))))
    checkDataset(Seq(LHMap(1 -> LHMapClass(LHMap(2 -> 3)))).toDS(),
      LHMap(1 -> LHMapClass(LHMap(2 -> 3))))
    checkDataset(Seq(LHMap(LHMapClass(LHMap(1 -> 2)) -> 3)).toDS(),
      LHMap(LHMapClass(LHMap(1 -> 2)) -> 3))
    checkDataset(Seq(LHMap(LHMapClass(LHMap(1 -> 2)) -> LHMapClass(LHMap(3 -> 4)))).toDS(),
      LHMap(LHMapClass(LHMap(1 -> 2)) -> LHMapClass(LHMap(3 -> 4))))

    val complex = ComplexMapClass(MapClass(Map(1 -> 2)), LHMapClass(LHMap(3 -> 4)))
    checkDataset(Seq(complex).toDS(), complex)
    checkDataset(Seq(Map(1 -> complex)).toDS(), Map(1 -> complex))
    checkDataset(Seq(Map(complex -> 5)).toDS(), Map(complex -> 5))
    checkDataset(Seq(Map(complex -> complex)).toDS(), Map(complex -> complex))
    checkDataset(Seq(LHMap(1 -> complex)).toDS(), LHMap(1 -> complex))
    checkDataset(Seq(LHMap(complex -> 5)).toDS(), LHMap(complex -> 5))
    checkDataset(Seq(LHMap(complex -> complex)).toDS(), LHMap(complex -> complex))

    // Tuples
    checkDataset(Seq(Map(1 -> 2) -> Map(3 -> 4)).toDS(), Map(1 -> 2) -> Map(3 -> 4))
    checkDataset(Seq(LHMap(1 -> 2) -> Map(3 -> 4)).toDS(), LHMap(1 -> 2) -> Map(3 -> 4))
    checkDataset(Seq(Map(1 -> 2) -> LHMap(3 -> 4)).toDS(), Map(1 -> 2) -> LHMap(3 -> 4))
    checkDataset(Seq(LHMap(1 -> 2) -> LHMap(3 -> 4)).toDS(), LHMap(1 -> 2) -> LHMap(3 -> 4))
    checkDataset(Seq(LHMap((Map("test1" -> 1) -> 2) -> (3 -> LHMap(4 -> "test2")))).toDS(),
      LHMap((Map("test1" -> 1) -> 2) -> (3 -> LHMap(4 -> "test2"))))

    // Complex
    checkDataset(Seq(LHMapClass(LHMap(1 -> 2)) -> LHMap("test" -> MapClass(Map(3 -> 4)))).toDS(),
      LHMapClass(LHMap(1 -> 2)) -> LHMap("test" -> MapClass(Map(3 -> 4))))
  }

  test("arbitrary sets") {
    checkDataset(Seq(Set(1, 2, 3, 4)).toDS(), Set(1, 2, 3, 4))
    checkDataset(Seq(Set(1.toLong, 2.toLong)).toDS(), Set(1.toLong, 2.toLong))
    checkDataset(Seq(Set(1.toDouble, 2.toDouble)).toDS(), Set(1.toDouble, 2.toDouble))
    checkDataset(Seq(Set(1.toFloat, 2.toFloat)).toDS(), Set(1.toFloat, 2.toFloat))
    checkDataset(Seq(Set(1.toByte, 2.toByte)).toDS(), Set(1.toByte, 2.toByte))
    checkDataset(Seq(Set(1.toShort, 2.toShort)).toDS(), Set(1.toShort, 2.toShort))
    checkDataset(Seq(Set(true, false)).toDS(), Set(true, false))
    checkDataset(Seq(Set("test1", "test2")).toDS(), Set("test1", "test2"))
    checkDataset(Seq(Set(Tuple1(1), Tuple1(2))).toDS(), Set(Tuple1(1), Tuple1(2)))

    checkDataset(Seq(HSet(1, 2)).toDS(), HSet(1, 2))
    checkDataset(Seq(HSet(1.toLong, 2.toLong)).toDS(), HSet(1.toLong, 2.toLong))
    checkDataset(Seq(HSet(1.toDouble, 2.toDouble)).toDS(), HSet(1.toDouble, 2.toDouble))
    checkDataset(Seq(HSet(1.toFloat, 2.toFloat)).toDS(), HSet(1.toFloat, 2.toFloat))
    checkDataset(Seq(HSet(1.toByte, 2.toByte)).toDS(), HSet(1.toByte, 2.toByte))
    checkDataset(Seq(HSet(1.toShort, 2.toShort)).toDS(), HSet(1.toShort, 2.toShort))
    checkDataset(Seq(HSet(true, false)).toDS(), HSet(true, false))
    checkDataset(Seq(HSet("test1", "test2")).toDS(), HSet("test1", "test2"))
    checkDataset(Seq(HSet(Tuple1(1), Tuple1(2))).toDS(), HSet(Tuple1(1), Tuple1(2)))

    checkDataset(Seq(Seq(Some(1), None), Seq(Some(2))).toDF("c").as[Set[Integer]],
      Seq(Set[Integer](1, null), Set[Integer](2)): _*)
  }

  test("nested sequences") {
    checkDataset(Seq(Seq(Seq(1))).toDS(), Seq(Seq(1)))
    checkDataset(Seq(List(Queue(1))).toDS(), List(Queue(1)))
  }

  test("nested maps") {
    checkDataset(Seq(Map(1 -> LHMap(2 -> 3))).toDS(), Map(1 -> LHMap(2 -> 3)))
    checkDataset(Seq(LHMap(Map(1 -> 2) -> 3)).toDS(), LHMap(Map(1 -> 2) -> 3))
  }

  test("nested set") {
    checkDataset(Seq(Set(HSet(1, 2), HSet(3, 4))).toDS(), Set(HSet(1, 2), HSet(3, 4)))
    checkDataset(Seq(HSet(Set(1, 2), Set(3, 4))).toDS(), HSet(Set(1, 2), Set(3, 4)))
  }

  test("package objects") {
    import packageobject._
    checkDataset(Seq(PackageClass(1)).toDS(), PackageClass(1))
  }

  test("SPARK-19104: Lambda variables in ExternalMapToCatalyst should be global") {
    val data = Seq.tabulate(10)(i => NestedData(1, Map("key" -> InnerData("name", i + 100))))
    val ds = spark.createDataset(data)
    checkDataset(ds, data: _*)
  }

  test("special floating point values") {
    import org.scalatest.exceptions.TestFailedException

    // Spark distinguishes -0.0 and 0.0
    intercept[TestFailedException] {
      checkDataset(Seq(-0.0d).toDS(), 0.0d)
    }
    intercept[TestFailedException] {
      checkAnswer(Seq(-0.0d).toDF(), Row(0.0d))
    }
    intercept[TestFailedException] {
      checkDataset(Seq(-0.0f).toDS(), 0.0f)
    }
    intercept[TestFailedException] {
      checkAnswer(Seq(-0.0f).toDF(), Row(0.0f))
    }
    intercept[TestFailedException] {
      checkDataset(Seq(Tuple1(-0.0)).toDS(), Tuple1(0.0))
    }
    intercept[TestFailedException] {
      checkAnswer(Seq(Tuple1(-0.0)).toDF(), Row(Row(0.0)))
    }
    intercept[TestFailedException] {
      checkDataset(Seq(Seq(-0.0)).toDS(), Seq(0.0))
    }
    intercept[TestFailedException] {
      checkAnswer(Seq(Seq(-0.0)).toDF(), Row(Seq(0.0)))
    }

    val floats = Seq[Float](-0.0f, 0.0f, Float.NaN)
    checkDataset(floats.toDS(), floats: _*)

    val arrayOfFloats = Seq[Array[Float]](Array(0.0f, -0.0f), Array(-0.0f, Float.NaN))
    checkDataset(arrayOfFloats.toDS(), arrayOfFloats: _*)

    val doubles = Seq[Double](-0.0d, 0.0d, Double.NaN)
    checkDataset(doubles.toDS(), doubles: _*)

    val arrayOfDoubles = Seq[Array[Double]](Array(0.0d, -0.0d), Array(-0.0d, Double.NaN))
    checkDataset(arrayOfDoubles.toDS(), arrayOfDoubles: _*)

    val tuples = Seq[(Float, Float, Double, Double)](
      (0.0f, -0.0f, 0.0d, -0.0d),
      (-0.0f, Float.NaN, -0.0d, Double.NaN))
    checkDataset(tuples.toDS(), tuples: _*)

    val complex = Map(Array(Seq(Tuple1(Double.NaN))) -> Map(Tuple2(Float.NaN, null)))
    checkDataset(Seq(complex).toDS(), complex)
  }
}

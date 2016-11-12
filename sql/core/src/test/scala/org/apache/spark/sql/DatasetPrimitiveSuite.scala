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

import org.apache.spark.sql.test.SharedSQLContext

case class IntClass(value: Int)

package object packageobject {
  case class PackageClass(value: Int)
}

class DatasetPrimitiveSuite extends QueryTest with SharedSQLContext {
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

  test("filter") {
    val ds = Seq(1, 2, 3, 4).toDS()
    checkDataset(
      ds.filter(_ % 2 == 0),
      2, 4)
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
    ds.foreachPartition(_.foreach(acc.add(_)))
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
    val agged = grouped.mapGroups { case (g, iter) =>
      val name = if (g == 0) "even" else "odd"
      (name, iter.size)
    }

    checkDatasetUnorderly(
      agged,
      ("even", 5), ("odd", 6))
  }

  test("groupBy function, flatMap") {
    val ds = Seq("a", "b", "c", "xyz", "hello").toDS()
    val grouped = ds.groupByKey(_.length)
    val agged = grouped.flatMapGroups { case (g, iter) => Iterator(g.toString, iter.mkString) }

    checkDatasetUnorderly(
      agged,
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

  test("package objects") {
    import packageobject._
    checkDataset(Seq(PackageClass(1)).toDS(), PackageClass(1))
  }

}

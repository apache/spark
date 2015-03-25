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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions._

abstract class TestMultiwayJoin extends MultiwayJoin {
  override def childrenOutputs: Seq[Seq[Attribute]] =
    Seq(
      Seq(AttributeReference("a", IntegerType)(), AttributeReference("b", IntegerType)()),
      Seq(AttributeReference("c", IntegerType)(), AttributeReference("d", IntegerType)()))
}

abstract class TestMultiwayJoin2 extends MultiwayJoin {
  override def childrenOutputs: Seq[Seq[Attribute]] =
    Seq(
      Seq(AttributeReference("a", IntegerType)(), AttributeReference("b", IntegerType)()),
      Seq(AttributeReference("c", IntegerType)(), AttributeReference("d", IntegerType)()),
      Seq(AttributeReference("e", IntegerType)(), AttributeReference("f", IntegerType)()))
}

class MultiWayJoinSuite extends FunSuite {
  val tables = Array(Array(Row(3, 5), Row(7, 10)), Array(Row(4, 4), Row(7, 7)))
  val tables2 = Array(Array(Row(3, 5), Row(7, 10)), Array(Row(4, 4), Row(7, 7)), Array(Row(1,1), Row(8, 8)))

  test("With Multiple Input Row") {
    val row1 = new MultiJoinedRow(2, 2)
    val row2 = new MultiJoinedRow(1, 2)
    val a = new GenericRow(Array[Any](1))
    val b = new GenericRow(Array[Any]("1", "2"))
    val c = new GenericRow(Array[Any]("3", 4))
    row1.withNewTable(0, b).withNewTable(1, c)
    row2.withNewTable(0, a).withNewTable(1, c)

    assert("1" === row1(0))
    assert("2" === row1(1))
    assert("3" === row1(2))
    assert(4 === row1(3))
    assert("1" === row1.getString(0))
    assert("2" === row1.getString(1))
    assert("3" === row1.getString(2))
    assert(4 === row1.getInt(3))

    assert(Row("1", "2", "3", 4) === row1.copy())
    assert(Row(1, "3", 4) === row2.copy())

    assert(1 === row2(0))
    assert("3" === row2(1))
    assert(4 === row2(2))

    assert(1 === row2.getInt(0))
    assert("3" === row2.getString(1))
    assert(4 === row2.getInt(2))

    assert(false === row1.isNullAt(0))
    assert(false === row1.isNullAt(1))
    assert(false === row1.isNullAt(2))
    row1.clearTable(0)
    assert(true === row1.isNullAt(0))
    assert(true === row1.isNullAt(1))
    assert(false === row1.isNullAt(2))
    assert(false === row1.isNullAt(3))
  }

  test("Test multiway join #1") {
    val mwj = new TestMultiwayJoin() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(Inner, GreaterThan(BoundReference(0, IntegerType, true), BoundReference(2, IntegerType, true))))
    }
    val results = mwj.product(tables).map(_.copy()).toArray
    assert(1 === results.length)
    assert(Row(7, 10, 4, 4) === results(0))
  }

  test("Test multiway join #2") {
    val mwj = new TestMultiwayJoin() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(LeftOuter, GreaterThan(BoundReference(0, IntegerType, true), Literal(3, IntegerType))))
    }

    val results = mwj.product(tables).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(3 === results.length)
    assert(Row(3, 5, null, null) === results(0))
    assert(Row(7, 10, 4, 4) === results(1))
    assert(Row(7, 10, 7, 7) === results(2))
  }


  test("Test multiway join #3") {
    val mwj = new TestMultiwayJoin() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(LeftOuter, GreaterThan(BoundReference(0, IntegerType, true), BoundReference(2, IntegerType, true))))
    }

    val results = mwj.product(tables).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(2 === results.length)
    assert(Row(3, 5, null, null) === results(0))
    assert(Row(7, 10, 4, 4) === results(1))
  }

  test("Test multiway join #4") {
    val mwj = new TestMultiwayJoin() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(RightOuter, GreaterThan(BoundReference(0, IntegerType, true), Literal(5, IntegerType))))
    }

    val results = mwj.product(tables).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(2 === results.length)
    assert(Row(7, 10, 4, 4) === results(0))
    assert(Row(7, 10, 7, 7) === results(1))
  }

  test("Test multiway join #5") {
    val mwj = new TestMultiwayJoin() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(RightOuter, GreaterThan(BoundReference(2, IntegerType, true), Literal(5, IntegerType))))
    }

    val results = mwj.product(tables).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(3 === results.length)
    assert(Row(3, 5, 7, 7) === results(0))
    assert(Row(7, 10, 7, 7) === results(1))
    assert(Row(null, null, 4, 4) === results(2))
  }

  test("Test multiway join #6") {
    val mwj = new TestMultiwayJoin() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(FullOuter, GreaterThan(BoundReference(0, IntegerType, true), Literal(5, IntegerType))))
    }

    val results = mwj.product(tables).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(3 === results.length)
    assert(Row(3, 5, null, null) === results(0))
    assert(Row(7, 10, 4, 4) === results(1))
    assert(Row(7, 10, 7, 7) === results(2))
  }

  test("Test multiway join #7") {
    val mwj = new TestMultiwayJoin() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(FullOuter, GreaterThan(BoundReference(2, IntegerType, true), Literal(5, IntegerType))))
    }

    val results = mwj.product(tables).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(3 === results.length)
    assert(Row(3, 5, 7, 7) === results(0))
    assert(Row(7, 10, 7, 7) === results(1))
    assert(Row(null, null, 4, 4) === results(2))
  }

  test("Test multiway join #8") {
    val mwj = new TestMultiwayJoin() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(LeftSemi, GreaterThan(BoundReference(2, IntegerType, true), Literal(5, IntegerType))))
    }

    val results = mwj.product(tables).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(2 === results.length)
    assert(Row(3, 5, null, null) === results(0))
    assert(Row(7, 10, null, null) === results(1))
  }

  test("Test multiway join #9") {
    val mwj = new TestMultiwayJoin() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(LeftSemi, GreaterThan(BoundReference(0, IntegerType, true), Literal(5, IntegerType))))
    }

    val results = mwj.product(tables).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(1 === results.length)
    assert(Row(7, 10, null, null) === results(0))
  }

  test("Test multiway join #10") {
    val mwj = new TestMultiwayJoin2() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(LeftOuter, GreaterThan(BoundReference(0, IntegerType, true), Literal(5, IntegerType))),
        JoinFilter(RightOuter, GreaterThan(BoundReference(4, IntegerType, true), Literal(5, IntegerType))))
    }

    val results = mwj.product(tables2).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(4 === results.length)
    assert(Row(3, 5, null, null, 8, 8) === results(0))
    assert(Row(7, 10, 4, 4, 8, 8) === results(1))
    assert(Row(7, 10, 7, 7, 8, 8) === results(2))
    assert(Row(null, null, null, null, 1, 1) === results(3))
  }

  test("Test multiway join #11") {
    val mwj = new TestMultiwayJoin2() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(LeftOuter, GreaterThan(BoundReference(0, IntegerType, true), Literal(5, IntegerType))),
        JoinFilter(RightOuter, GreaterThan(BoundReference(4, IntegerType, true), Literal(5, IntegerType))))
    }

    val results = mwj.product(tables2).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(4 === results.length)
    assert(Row(3, 5, null, null, 8, 8) === results(0))
    assert(Row(7, 10, 4, 4, 8, 8) === results(1))
    assert(Row(7, 10, 7, 7, 8, 8) === results(2))
    assert(Row(null, null, null, null, 1, 1) === results(3))
  }

  test("Test multiway join #12") {
    val mwj = new TestMultiwayJoin2() {
      var joinFilters: Array[JoinFilter] = Array(
        JoinFilter(LeftOuter, GreaterThan(BoundReference(0, IntegerType, true), BoundReference(2, IntegerType, true))),
        JoinFilter(RightOuter, GreaterThan(BoundReference(2, IntegerType, true), BoundReference(4, IntegerType, true))))
    }

    val results = mwj.product(tables2).map(_.copy()).toArray.sortWith(_.toString < _.toString)
    assert(2 === results.length)
    assert(Row(7, 10, 4, 4, 1, 1) === results(0))
    assert(Row(null, null, null, null, 8, 8) === results(1))
  }
}


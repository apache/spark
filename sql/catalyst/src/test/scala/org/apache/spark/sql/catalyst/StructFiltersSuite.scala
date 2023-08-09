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

package org.apache.spark.sql.catalyst

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.{AlwaysFalse, AlwaysTrue, Filter}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.unsafe.types.UTF8String

abstract class StructFiltersSuite extends SparkFunSuite {

  protected def createFilters(filters: Seq[sources.Filter], schema: StructType): StructFilters

  test("filter to expression conversion") {
    val ref = BoundReference(0, IntegerType, true)
    def check(f: Filter, expr: Expression): Unit = {
      assert(StructFilters.filterToExpression(f, _ => Some(ref)).get === expr)
    }

    check(sources.AlwaysTrue, Literal(true))
    check(sources.AlwaysFalse, Literal(false))
    check(sources.IsNull("a"), IsNull(ref))
    check(sources.Not(sources.IsNull("a")), Not(IsNull(ref)))
    check(sources.IsNotNull("a"), IsNotNull(ref))
    check(sources.EqualTo("a", "b"), EqualTo(ref, Literal("b")))
    check(sources.EqualNullSafe("a", "b"), EqualNullSafe(ref, Literal("b")))
    check(sources.StringStartsWith("a", "b"), StartsWith(ref, Literal("b")))
    check(sources.StringEndsWith("a", "b"), EndsWith(ref, Literal("b")))
    check(sources.StringContains("a", "b"), Contains(ref, Literal("b")))
    check(sources.LessThanOrEqual("a", 1), LessThanOrEqual(ref, Literal(1)))
    check(sources.LessThan("a", 1), LessThan(ref, Literal(1)))
    check(sources.GreaterThanOrEqual("a", 1), GreaterThanOrEqual(ref, Literal(1)))
    check(sources.GreaterThan("a", 1), GreaterThan(ref, Literal(1)))
    check(sources.And(sources.AlwaysTrue, sources.AlwaysTrue), And(Literal(true), Literal(true)))
    check(sources.Or(sources.AlwaysTrue, sources.AlwaysTrue), Or(Literal(true), Literal(true)))
    check(sources.In("a", Array(1)), In(ref, Seq(Literal(1))))
  }

  private def getSchema(str: String): StructType = str match {
    case "" => new StructType()
    case _ => StructType.fromDDL(str)
  }

  test("skipping rows") {
    def check(
      requiredSchema: String = "i INTEGER, d DOUBLE",
      filters: Seq[Filter],
      row: InternalRow,
      pos: Int,
      skip: Boolean): Unit = {
      val structFilters = createFilters(filters, getSchema(requiredSchema))
      structFilters.reset()
      assert(structFilters.skipRow(row, pos) === skip)
    }

    check(filters = Seq(), row = InternalRow(3.14), pos = 0, skip = false)
    check(filters = Seq(AlwaysTrue), row = InternalRow(1), pos = 0, skip = false)
    check(filters = Seq(AlwaysFalse), row = InternalRow(1), pos = 0, skip = true)
    check(
      filters = Seq(sources.EqualTo("i", 1), sources.LessThan("d", 10), sources.AlwaysFalse),
      row = InternalRow(1, 3.14),
      pos = 0,
      skip = true)
    check(
      filters = Seq(sources.EqualTo("i", 10)),
      row = InternalRow(10, 3.14),
      pos = 0,
      skip = false)
    check(
      filters = Seq(sources.IsNotNull("d"), sources.GreaterThanOrEqual("d", 2.96)),
      row = InternalRow(3.14),
      pos = 0,
      skip = false)
    check(
      filters = Seq(sources.In("i", Array(10, 20)), sources.LessThanOrEqual("d", 2.96)),
      row = InternalRow(10, 3.14),
      pos = 1,
      skip = true)
    val filters1 = Seq(
      sources.Or(
        sources.AlwaysTrue,
        sources.And(
          sources.Not(sources.IsNull("i")),
          sources.Not(
            sources.And(
              sources.StringEndsWith("s", "ab"),
              sources.StringEndsWith("s", "cd")
            )
          )
        )
      ),
      sources.GreaterThan("d", 0),
      sources.LessThan("i", 500)
    )
    val filters2 = Seq(
      sources.And(
        sources.StringContains("s", "abc"),
        sources.And(
          sources.Not(sources.IsNull("i")),
          sources.And(
            sources.StringEndsWith("s", "ab"),
            sources.StringEndsWith("s", "bc")
          )
        )
      ),
      sources.GreaterThan("d", 100),
      sources.LessThan("i", 0)
    )
    Seq(filters1 -> false, filters2 -> true).foreach { case (filters, skip) =>
      val schema = "i INTEGER, d DOUBLE, s STRING"
      val row = InternalRow(10, 3.14, UTF8String.fromString("abc"))
      val structFilters = createFilters(filters, getSchema(schema))
      structFilters.reset()
      for { p <- 0 until 3 if !skip } {
        assert(structFilters.skipRow(row, p) === skip, s"p = $p filters = $filters skip = $skip")
      }
    }
  }
}

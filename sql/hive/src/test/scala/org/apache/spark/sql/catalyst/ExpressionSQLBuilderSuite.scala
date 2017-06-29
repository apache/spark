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

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{If, Literal, SpecifiedWindowFrame, TimeAdd,
  TimeSub, WindowSpecDefinition}
import org.apache.spark.unsafe.types.CalendarInterval

class ExpressionSQLBuilderSuite extends SQLBuilderTest {
  test("literal") {
    checkSQL(Literal("foo"), "'foo'")
    checkSQL(Literal("\"foo\""), "'\"foo\"'")
    checkSQL(Literal("'foo'"), "'\\'foo\\''")
    checkSQL(Literal(1: Byte), "1Y")
    checkSQL(Literal(2: Short), "2S")
    checkSQL(Literal(4: Int), "4")
    checkSQL(Literal(8: Long), "8L")
    checkSQL(Literal(1.5F), "CAST(1.5 AS FLOAT)")
    checkSQL(Literal(Float.PositiveInfinity), "CAST('Infinity' AS FLOAT)")
    checkSQL(Literal(Float.NegativeInfinity), "CAST('-Infinity' AS FLOAT)")
    checkSQL(Literal(Float.NaN), "CAST('NaN' AS FLOAT)")
    checkSQL(Literal(2.5D), "2.5D")
    checkSQL(Literal(Double.PositiveInfinity), "CAST('Infinity' AS DOUBLE)")
    checkSQL(Literal(Double.NegativeInfinity), "CAST('-Infinity' AS DOUBLE)")
    checkSQL(Literal(Double.NaN), "CAST('NaN' AS DOUBLE)")
    checkSQL(Literal(BigDecimal("10.0000000").underlying), "10.0000000BD")
    checkSQL(Literal(Array(0x01, 0xA3).map(_.toByte)), "X'01A3'")
    checkSQL(
      Literal(Timestamp.valueOf("2016-01-01 00:00:00")), "TIMESTAMP('2016-01-01 00:00:00.0')")
    // TODO tests for decimals
  }

  test("attributes") {
    checkSQL('a.int, "`a`")
    checkSQL(Symbol("foo bar").int, "`foo bar`")
    // Keyword
    checkSQL('int.int, "`int`")
  }

  test("binary comparisons") {
    checkSQL('a.int === 'b.int, "(`a` = `b`)")
    checkSQL('a.int <=> 'b.int, "(`a` <=> `b`)")
    checkSQL('a.int =!= 'b.int, "(NOT (`a` = `b`))")

    checkSQL('a.int < 'b.int, "(`a` < `b`)")
    checkSQL('a.int <= 'b.int, "(`a` <= `b`)")
    checkSQL('a.int > 'b.int, "(`a` > `b`)")
    checkSQL('a.int >= 'b.int, "(`a` >= `b`)")

    checkSQL('a.int in ('b.int, 'c.int), "(`a` IN (`b`, `c`))")
    checkSQL('a.int in (1, 2), "(`a` IN (1, 2))")

    checkSQL('a.int.isNull, "(`a` IS NULL)")
    checkSQL('a.int.isNotNull, "(`a` IS NOT NULL)")
  }

  test("logical operators") {
    checkSQL('a.boolean && 'b.boolean, "(`a` AND `b`)")
    checkSQL('a.boolean || 'b.boolean, "(`a` OR `b`)")
    checkSQL(!'a.boolean, "(NOT `a`)")
    checkSQL(If('a.boolean, 'b.int, 'c.int), "(IF(`a`, `b`, `c`))")
  }

  test("arithmetic expressions") {
    checkSQL('a.int + 'b.int, "(`a` + `b`)")
    checkSQL('a.int - 'b.int, "(`a` - `b`)")
    checkSQL('a.int * 'b.int, "(`a` * `b`)")
    checkSQL('a.int / 'b.int, "(`a` / `b`)")
    checkSQL('a.int % 'b.int, "(`a` % `b`)")

    checkSQL(-'a.int, "(- `a`)")
    checkSQL(-('a.int + 'b.int), "(- (`a` + `b`))")
  }

  test("window specification") {
    val frame = SpecifiedWindowFrame.defaultWindowFrame(
      hasOrderSpecification = true,
      acceptWindowFrame = true
    )

    checkSQL(
      WindowSpecDefinition('a.int :: Nil, Nil, frame),
      s"(PARTITION BY `a` $frame)"
    )

    checkSQL(
      WindowSpecDefinition('a.int :: 'b.string :: Nil, Nil, frame),
      s"(PARTITION BY `a`, `b` $frame)"
    )

    checkSQL(
      WindowSpecDefinition(Nil, 'a.int.asc :: Nil, frame),
      s"(ORDER BY `a` ASC NULLS FIRST $frame)"
    )

    checkSQL(
      WindowSpecDefinition(Nil, 'a.int.asc :: 'b.string.desc :: Nil, frame),
      s"(ORDER BY `a` ASC NULLS FIRST, `b` DESC NULLS LAST $frame)"
    )

    checkSQL(
      WindowSpecDefinition('a.int :: 'b.string :: Nil, 'c.int.asc :: 'd.string.desc :: Nil, frame),
      s"(PARTITION BY `a`, `b` ORDER BY `c` ASC NULLS FIRST, `d` DESC NULLS LAST $frame)"
    )
  }

  test("interval arithmetic") {
    val interval = Literal(new CalendarInterval(0, CalendarInterval.MICROS_PER_DAY))

    checkSQL(
      TimeAdd('a, interval),
      "`a` + interval 1 days"
    )

    checkSQL(
      TimeSub('a, interval),
      "`a` - interval 1 days"
    )
  }
}

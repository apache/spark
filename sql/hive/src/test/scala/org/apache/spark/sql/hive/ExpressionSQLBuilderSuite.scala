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

package org.apache.spark.sql.hive

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{If, Literal}

class ExpressionSQLBuilderSuite extends SQLBuilderTest {
  test("literal") {
    checkSQL(Literal("foo"), "\"foo\"")
    checkSQL(Literal("\"foo\""), "\"\\\"foo\\\"\"")
    checkSQL(Literal(1: Byte), "CAST(1 AS TINYINT)")
    checkSQL(Literal(2: Short), "CAST(2 AS SMALLINT)")
    checkSQL(Literal(4: Int), "4")
    checkSQL(Literal(8: Long), "CAST(8 AS BIGINT)")
    checkSQL(Literal(1.5F), "CAST(1.5 AS FLOAT)")
    checkSQL(Literal(2.5D), "2.5")
    checkSQL(
      Literal(Timestamp.valueOf("2016-01-01 00:00:00")), "TIMESTAMP('2016-01-01 00:00:00.0')")
    // TODO tests for decimals
  }

  test("binary comparisons") {
    checkSQL('a.int === 'b.int, "(`a` = `b`)")
    checkSQL('a.int <=> 'b.int, "(`a` <=> `b`)")
    checkSQL('a.int !== 'b.int, "(NOT (`a` = `b`))")

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

    checkSQL(-'a.int, "(-`a`)")
    checkSQL(-('a.int + 'b.int), "(-(`a` + `b`))")
  }
}

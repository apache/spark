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

package test.org.apache.spark.sql

import org.apache.spark.sql.{Column, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.classic.{ClassicConversions, ColumnConversions}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests the public Expression <-> Column conversions from a package outside of
 * `org.apache.spark`, which is the only way to catch that a step of the path is package-private.
 * Compiling this suite is as much a part of the test as running it.
 */
class ExpressionToColumnSuite extends QueryTest with SharedSparkSession {

  test("SPARK-49828: build a Column from an Expression via the Column companion") {
    val e: Expression = Literal(1)
    val c: Column = Column(e)
    assert(ColumnConversions.expression(c) == e)
  }

  test("SPARK-49828: build a Column from an Expression via ClassicConversions.column") {
    val e: Expression = Literal(1)
    val c: Column = ClassicConversions.column(e)
    assert(ColumnConversions.expression(c) == e)
  }

  test("SPARK-49828: a Column built from an Expression is usable in a query") {
    val df = spark.range(2).select(Column(Literal(1)).as("one"))
    checkAnswer(df, Seq(Row(1), Row(1)))
  }
}

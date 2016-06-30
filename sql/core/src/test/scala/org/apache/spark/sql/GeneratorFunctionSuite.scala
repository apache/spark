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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

class GeneratorFunctionSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("single explode") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")
    checkAnswer(
      df.select(explode('intList)),
      Row(1) :: Row(2) :: Row(3) :: Nil)
  }

  test("single posexplode") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")
    checkAnswer(
      df.select(posexplode('intList)),
      Row(0, 1) :: Row(1, 2) :: Row(2, 3) :: Nil)
  }

  test("explode and other columns") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")

    checkAnswer(
      df.select($"a", explode('intList)),
      Row(1, 1) ::
      Row(1, 2) ::
      Row(1, 3) :: Nil)

    checkAnswer(
      df.select($"*", explode('intList)),
      Row(1, Seq(1, 2, 3), 1) ::
      Row(1, Seq(1, 2, 3), 2) ::
      Row(1, Seq(1, 2, 3), 3) :: Nil)
  }

  test("aliased explode") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")

    checkAnswer(
      df.select(explode('intList).as('int)).select('int),
      Row(1) :: Row(2) :: Row(3) :: Nil)

    checkAnswer(
      df.select(explode('intList).as('int)).select(sum('int)),
      Row(6) :: Nil)
  }

  test("explode on map") {
    val df = Seq((1, Map("a" -> "b"))).toDF("a", "map")

    checkAnswer(
      df.select(explode('map)),
      Row("a", "b"))
  }

  test("explode on map with aliases") {
    val df = Seq((1, Map("a" -> "b"))).toDF("a", "map")

    checkAnswer(
      df.select(explode('map).as("key1" :: "value1" :: Nil)).select("key1", "value1"),
      Row("a", "b"))
  }

  test("self join explode") {
    val df = Seq((1, Seq(1, 2, 3))).toDF("a", "intList")
    val exploded = df.select(explode('intList).as('i))

    checkAnswer(
      exploded.join(exploded, exploded("i") === exploded("i")).agg(count("*")),
      Row(3) :: Nil)
  }
}

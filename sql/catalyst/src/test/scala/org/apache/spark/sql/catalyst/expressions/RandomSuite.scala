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

package org.apache.spark.sql.catalyst.expressions

import org.scalatest.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{IntegerType, LongType}

class RandomSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("random") {
    checkDoubleEvaluation(Rand(30), 0.31429268272540556 +- 0.001)
    checkDoubleEvaluation(Randn(30), -0.4798519469521663 +- 0.001)

    checkDoubleEvaluation(
      new Rand(Literal.create(null, LongType)), 0.8446490682263027 +- 0.001)
    checkDoubleEvaluation(
      new Randn(Literal.create(null, IntegerType)), 1.1164209726833079 +- 0.001)
  }

  test("SPARK-9127 codegen with long seed") {
    checkDoubleEvaluation(Rand(5419823303878592871L), 0.2304755080444375 +- 0.001)
    checkDoubleEvaluation(Randn(5419823303878592871L), -1.2824262718225607 +- 0.001)
  }
}

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

import org.apache.spark.sql.types.IntegerType
import org.scalatest.{Matchers, FunSuite}

class AggregationSchemaSuite extends FunSuite with Matchers {

  test("Sum should keep (non-)nullable attributes (non-)nullable") {
    Sum(createExpression(true)).nullable should be (true)
    Sum(createExpression(false)).nullable should be (false)
  }

  test("Average should keep (non-)nullable attributes (non-)nullable") {
    Average(createExpression(true)).nullable should be (true)
    Average(createExpression(false)).nullable should be (false)
  }

  test("First should keep (non-)nullable attributes (non-)nullable") {
    First(createExpression(true)).nullable should be (true)
    First(createExpression(false)).nullable should be (false)
  }

  test("Last should keep (non-)nullable attributes (non-)nullable") {
    Last(createExpression(true)).nullable should be (true)
    Last(createExpression(false)).nullable should be (false)
  }

  test("Min should keep (non-)nullable attributes (non-)nullable") {
    Min(createExpression(true)).nullable should be (true)
    Min(createExpression(false)).nullable should be (false)
  }

  test("Max should keep (non-)nullable attributes (non-)nullable") {
    Max(createExpression(true)).nullable should be (true)
    Max(createExpression(false)).nullable should be (false)
  }

  private def createExpression(nullable: Boolean) =
    AttributeReference("i", IntegerType, nullable = nullable)()

}

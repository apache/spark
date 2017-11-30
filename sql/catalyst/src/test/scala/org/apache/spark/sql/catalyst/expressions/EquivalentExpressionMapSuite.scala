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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._

class EquivalentExpressionMapSuite extends SparkFunSuite {

  private val onePlusTwo = Literal(1) + Literal(2)
  private val twoPlusOne = Literal(2) + Literal(1)
  private val rand = Rand(10)

  test("behaviour of the equivalent expression map") {
    val equivalentExpressionMap = new EquivalentExpressionMap()
    equivalentExpressionMap.put(onePlusTwo, 'a)
    equivalentExpressionMap.put(Literal(1) + Literal(3), 'b)
    equivalentExpressionMap.put(rand, 'c)

    // 1 + 2 should be equivalent to 2 + 1
    assertResult(ExpressionSet(Seq('a)))(equivalentExpressionMap.get(twoPlusOne))
    // non-deterministic expressions should not be equivalent
    assertResult(ExpressionSet.empty)(equivalentExpressionMap.get(rand))

    // if the same (key, value) is added several times, the map still returns only one entry
    equivalentExpressionMap.put(onePlusTwo, 'a)
    equivalentExpressionMap.put(twoPlusOne, 'a)
    assertResult(ExpressionSet(Seq('a)))(equivalentExpressionMap.get(twoPlusOne))

    // get several equivalent attributes
    equivalentExpressionMap.put(onePlusTwo, 'e)
    assertResult(ExpressionSet(Seq('a, 'e)))(equivalentExpressionMap.get(onePlusTwo))
    assertResult(2)(equivalentExpressionMap.get(onePlusTwo).size)

    // several non-deterministic expressions should not be equivalent
    equivalentExpressionMap.put(rand, 'd)
    assertResult(ExpressionSet.empty)(equivalentExpressionMap.get(rand))
    assertResult(0)(equivalentExpressionMap.get(rand).size)
  }

}

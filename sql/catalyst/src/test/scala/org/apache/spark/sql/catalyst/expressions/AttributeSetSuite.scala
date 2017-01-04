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
import org.apache.spark.sql.types.IntegerType

class AttributeSetSuite extends SparkFunSuite {

  val aUpper = AttributeReference("A", IntegerType)(exprId = ExprId(1))
  val aLower = AttributeReference("a", IntegerType)(exprId = ExprId(1))
  val fakeA = AttributeReference("a", IntegerType)(exprId = ExprId(3))
  val aSet = AttributeSet(aLower :: Nil)

  val bUpper = AttributeReference("B", IntegerType)(exprId = ExprId(2))
  val bLower = AttributeReference("b", IntegerType)(exprId = ExprId(2))
  val bSet = AttributeSet(bUpper :: Nil)

  val aAndBSet = AttributeSet(aUpper :: bUpper :: Nil)

  test("sanity check") {
    assert(aUpper != aLower)
    assert(bUpper != bLower)
  }

  test("checks by id not name") {
    assert(aSet.contains(aUpper) === true)
    assert(aSet.contains(aLower) === true)
    assert(aSet.contains(fakeA) === false)

    assert(aSet.contains(bUpper) === false)
    assert(aSet.contains(bLower) === false)
  }

  test("++ preserves AttributeSet")  {
    assert((aSet ++ bSet).contains(aUpper) === true)
    assert((aSet ++ bSet).contains(aLower) === true)
  }

  test("extracts all references ") {
    val addSet = AttributeSet(Add(aUpper, Alias(bUpper, "test")()):: Nil)
    assert(addSet.contains(aUpper))
    assert(addSet.contains(aLower))
    assert(addSet.contains(bUpper))
    assert(addSet.contains(bLower))
  }

  test("dedups attributes") {
    assert(AttributeSet(aUpper :: aLower :: Nil).size === 1)
  }

  test("subset") {
    assert(aSet.subsetOf(aAndBSet) === true)
    assert(aAndBSet.subsetOf(aSet) === false)
  }

  test("equality") {
    assert(aSet != aAndBSet)
    assert(aAndBSet != aSet)
    assert(aSet != bSet)
    assert(bSet != aSet)

    assert(aSet == aSet)
    assert(aSet == AttributeSet(aUpper :: Nil))
  }
}

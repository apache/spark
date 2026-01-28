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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, SequentialUnion}

class SequentialUnionSuite extends SparkFunSuite {

  private val testRelation1 = LocalRelation($"a".int, $"b".int)
  private val testRelation2 = LocalRelation($"a".int, $"b".int)
  private val testRelation3 = LocalRelation($"a".int, $"b".int)

  test("SequentialUnion - basic creation") {
    val union = SequentialUnion(Seq(testRelation1, testRelation2))
    assert(union.children.length === 2)
    assert(union.children.head === testRelation1)
    assert(union.children(1) === testRelation2)
  }

  test("SequentialUnion - requires at least 2 children") {
    val ex = intercept[AssertionError] {
      SequentialUnion(Seq(testRelation1))
    }
    assert(ex.getMessage.contains("at least 2 children"))
  }

  test("SequentialUnion - output schema") {
    val union = SequentialUnion(Seq(testRelation1, testRelation2))
    assert(union.output.length === 2)
    assert(union.output.map(_.name) === Seq("a", "b"))
  }

  test("SequentialUnion - byName parameter") {
    val union = SequentialUnion(Seq(testRelation1, testRelation2), byName = true)
    assert(union.byName === true)
  }

  test("SequentialUnion - allowMissingCol requires byName") {
    // Should succeed when byName is true
    val union1 = SequentialUnion(
      Seq(testRelation1, testRelation2),
      byName = true,
      allowMissingCol = true
    )
    assert(union1.allowMissingCol === true)

    // Should fail when byName is false
    val ex = intercept[AssertionError] {
      SequentialUnion(
        Seq(testRelation1, testRelation2),
        byName = false,
        allowMissingCol = true
      )
    }
    assert(ex.getMessage.contains("byName"))
  }

  test("SequentialUnion.flatten - single level") {
    val plans = Seq(testRelation1, testRelation2, testRelation3)
    val flattened = SequentialUnion.flatten(plans)
    assert(flattened === plans)
  }

  test("SequentialUnion.flatten - nested unions") {
    val innerUnion = SequentialUnion(Seq(testRelation1, testRelation2))
    val plans = Seq(innerUnion, testRelation3)
    val flattened = SequentialUnion.flatten(plans)

    assert(flattened.length === 3)
    assert(flattened === Seq(testRelation1, testRelation2, testRelation3))
  }

  test("SequentialUnion.flatten - deeply nested unions") {
    val union1 = SequentialUnion(Seq(testRelation1, testRelation2))
    val union2 = SequentialUnion(Seq(union1, testRelation3))

    val flattened = SequentialUnion.flatten(Seq(union2))

    assert(flattened.length === 3)
    assert(flattened === Seq(testRelation1, testRelation2, testRelation3))
  }

  test("SequentialUnion.flatten - multiple nested unions") {
    val union1 = SequentialUnion(Seq(testRelation1, testRelation2))
    val union2 = SequentialUnion(Seq(testRelation2, testRelation3))

    val flattened = SequentialUnion.flatten(Seq(union1, union2))

    assert(flattened.length === 4)
    assert(flattened === Seq(testRelation1, testRelation2, testRelation2, testRelation3))
  }

  test("SequentialUnion - three children") {
    val union = SequentialUnion(Seq(testRelation1, testRelation2, testRelation3))
    assert(union.children.length === 3)
  }
}

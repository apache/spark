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
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.IntegerType

class DynamicPruningSubquerySuite extends SparkFunSuite {
  private val pruningKeyExpression = Literal(1)

  private val validDynamicPruningSubquery = DynamicPruningSubquery(
    pruningKey = pruningKeyExpression,
    buildQuery = Project(Seq(AttributeReference("id", IntegerType)()),
      LocalRelation(AttributeReference("id", IntegerType)())),
    buildKeys = Seq(pruningKeyExpression),
    broadcastKeyIndices = Seq(0),
    onlyInBroadcast = false
  )

  test("pruningKey data type matches single buildKey") {
    val dynamicPruningSubquery = validDynamicPruningSubquery
      .copy(buildKeys = Seq(Literal(2023)))
    assert(dynamicPruningSubquery.resolved == true)
  }

  test("pruningKey data type is a Struct and matches with Struct buildKey") {
    val dynamicPruningSubquery = validDynamicPruningSubquery
      .copy(pruningKey = CreateStruct(Seq(Literal(1), Literal.FalseLiteral)),
        buildKeys = Seq(CreateStruct(Seq(Literal(2), Literal.TrueLiteral))))
    assert(dynamicPruningSubquery.resolved == true)
  }

  test("multiple buildKeys but only one broadcastKeyIndex") {
    val dynamicPruningSubquery = validDynamicPruningSubquery
      .copy(buildKeys = Seq(Literal(0), Literal(2), Literal(0), Literal(9)),
        broadcastKeyIndices = Seq(1))
    assert(dynamicPruningSubquery.resolved == true)
  }

  test("pruningKey data type does not match the single buildKey") {
    val dynamicPruningSubquery = validDynamicPruningSubquery.copy(
      pruningKey = Literal.TrueLiteral,
      buildKeys = Seq(Literal(2013)))
    assert(dynamicPruningSubquery.resolved == false)
  }

  test("pruningKey data type is a Struct but mismatch with Struct buildKey") {
    val dynamicPruningSubquery = validDynamicPruningSubquery
      .copy(pruningKey = CreateStruct(Seq(Literal(1), Literal.FalseLiteral)),
        buildKeys = Seq(CreateStruct(Seq(Literal.TrueLiteral, Literal(2)))))
    assert(dynamicPruningSubquery.resolved == false)
  }

  test("DynamicPruningSubquery should only have a single broadcasting key") {
    val dynamicPruningSubquery = validDynamicPruningSubquery
      .copy(buildKeys = Seq(Literal(2025), Literal(2), Literal(1809)),
        broadcastKeyIndices = Seq(0, 2))
    assert(dynamicPruningSubquery.resolved == false)
  }

  test("duplicates in broadcastKeyIndices, and also should not be allowed") {
    val dynamicPruningSubquery = validDynamicPruningSubquery
      .copy(buildKeys = Seq(Literal(2)),
        broadcastKeyIndices = Seq(0, 0))
    assert(dynamicPruningSubquery.resolved == false)
  }

  test("broadcastKeyIndex out of bounds") {
    val dynamicPruningSubquery = validDynamicPruningSubquery
      .copy(broadcastKeyIndices = Seq(1))
    assert(dynamicPruningSubquery.resolved == false)
  }
}

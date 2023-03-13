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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{GetJsonObject, JsonTuple}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

class RewriteGetJsonObjectSuite extends PlanTest {

  private val testRelation = LocalRelation($"a".string, $"b".string, $"c".string)

  test("Rewrite to a single JsonTuple") {
    val query = testRelation
      .select(
        GetJsonObject($"a", stringToLiteral("$.c1")).as("c1"),
        GetJsonObject($"a", stringToLiteral("$.c2")).as("c2"),
        GetJsonObject($"a", stringToLiteral("$.c3")).as("c3"))

    val correctAnswer = testRelation
      .generate(
        JsonTuple(Seq($"a", stringToLiteral("c1"), stringToLiteral("c2"), stringToLiteral("c3"))),
        Nil,
        alias = Some("a"),
        outputNames = Seq("c1", "c2", "c3"))
      .select($"c1".as("c1"), $"c2".as("c2"), $"c3".as("c3"))

    val getJsonObjects = query.expressions.flatMap { _.collect {
        case gjo: GetJsonObject if gjo.rewrittenPathName.nonEmpty => gjo
    }}
    assert(getJsonObjects.forall(_.rewrittenPathName.nonEmpty))
    comparePlans(RewriteGetJsonObject(query.analyze), correctAnswer.analyze)
  }

  test("Rewrite to multiple JsonTuples") {
    val query = testRelation
      .select(
        GetJsonObject($"a", stringToLiteral("$.c1")).as("c1"),
        GetJsonObject($"a", stringToLiteral("$.c2")).as("c2"),
        GetJsonObject($"b", stringToLiteral("$.c1")).as("c3"),
        GetJsonObject($"b", stringToLiteral("$.c2")).as("c4"),
        GetJsonObject($"c", stringToLiteral("$.c1")).as("c5"))

    val correctAnswer = testRelation
      .generate(
        JsonTuple(Seq($"a", stringToLiteral("c1"), stringToLiteral("c2"))),
        Nil,
        alias = Some("a"),
        outputNames = Seq("c1", "c2"))
      .generate(
        JsonTuple(Seq($"b", stringToLiteral("c1"), stringToLiteral("c2"))),
        Nil,
        alias = Some("b"),
        outputNames = Seq("c1", "c2"))
      .select(
        $"a.c1".as("c1"), $"a.c2".as("c2"),
        $"b.c1".as("c3"), $"b.c2".as("c4"),
        GetJsonObject($"c", stringToLiteral("$.c1")).as("c5"))

    val getJsonObjects = query.expressions.flatMap {
      _.collect {
        case gjo: GetJsonObject if gjo.rewrittenPathName.nonEmpty => gjo
      }
    }
    assert(getJsonObjects.forall(_.rewrittenPathName.nonEmpty))
    comparePlans(RewriteGetJsonObject(query.analyze), correctAnswer.analyze)
  }

  test("Do not rewrite is rewrittenPathName is empty") {
    val query = testRelation
      .select(
        GetJsonObject($"a", stringToLiteral("c1")).as("c1"),
        GetJsonObject($"a", stringToLiteral("c2")).as("c2"),
        GetJsonObject($"a", stringToLiteral("c3")).as("c3"))

    val getJsonObjects = query.expressions.flatMap {
      _.collect {
        case gjo: GetJsonObject if gjo.rewrittenPathName.nonEmpty => gjo
      }
    }
    assert(getJsonObjects.forall(_.rewrittenPathName.isEmpty))
    comparePlans(RewriteGetJsonObject(query.analyze), query.analyze)
  }
}

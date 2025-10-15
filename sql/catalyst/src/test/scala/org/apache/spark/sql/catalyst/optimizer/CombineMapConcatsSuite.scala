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

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class CombineMapConcatsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("CombineMapConcatsSuite", FixedPoint(50), CombineMapConcats) :: Nil
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer =
      Limit(Literal(1), Project(Alias(e2, "out")() :: Nil, OneRowRelation())).analyze
    val actual = Optimize.execute(
      Limit(Literal(1), Project(Alias(e1, "out")() :: Nil, OneRowRelation())).analyze)
    comparePlans(actual, correctAnswer)
  }

  test("flatten and optimize nested MapConcat expressions") {
    // map_concat() => map()
    assertEquivalent(MapConcat(Seq()), MapConcat(Seq()))
    // map_concat(map_concat(map_concat())) => map()
    assertEquivalent(MapConcat(Seq(MapConcat(Seq(MapConcat(Seq()))))), MapConcat(Seq()))

    /*
     * map_concat(
     *  map('a', 'a1'),
     *  map_concat(
     *    map('b', 'b1'),
     *    map('c', 'c1'),
     *    map_concat(map('d', 'd1')
     *  ),
     *  map_concat(),
     *  map('e', 'e1')
     * )
     * => map_concat(map('a', 'a1'), map('b', 'b1'), map('c', 'c1'), map('d', 'd1'), map('e', 'e1'))
     */
    assertEquivalent(
      MapConcat(
        Seq(
          CreateMap(Seq(Literal("a"), Literal("a1"))),
          MapConcat(
            Seq(
              CreateMap(Seq(Literal("b"), Literal("b1"))),
              CreateMap(Seq(Literal("c"), Literal("c1"))),
              MapConcat(Seq(CreateMap(Seq(Literal("d"), Literal("d1"))))))),
          MapConcat(Seq()),
          CreateMap(Seq(Literal("e"), Literal("e1"))))),
      MapConcat(
        Seq(
          CreateMap(Seq(Literal("a"), Literal("a1"))),
          CreateMap(Seq(Literal("b"), Literal("b1"))),
          CreateMap(Seq(Literal("c"), Literal("c1"))),
          CreateMap(Seq(Literal("d"), Literal("d1"))),
          CreateMap(Seq(Literal("e"), Literal("e1"))))))
  }

  test("extract child directly from MapConcat with single element") {
    // map_concat(map('a', 'a1')) => map('a', 'a1')
    assertEquivalent(
      MapConcat(Seq(CreateMap(Seq(Literal("a"), Literal("a1"))))),
      CreateMap(Seq(Literal("a"), Literal("a1"))))

    // map_concat(map('a', 'a1'), map_concat()) => map('a', 'a1')
    assertEquivalent(
      MapConcat(Seq(CreateMap(Seq(Literal("a"), Literal("a1"))), MapConcat(Seq()))),
      CreateMap(Seq(Literal("a"), Literal("a1"))))
  }

}

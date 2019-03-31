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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{And, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation


class PhysicalOperationSuite extends PlanTest {
  import PhysicalOperation.collectProjectsAndFilters

  val relation = LocalRelation('a.int, 'b.int, 'c.int)

  test("test non-deterministic conditions in filter") {
    val expr1 = ('a.int === Literal(1)) && rand(1) < 1
    val expr2 = 'a.int === Literal(1)
    compareExpressions(collectProjectsAndFilters(relation.where(expr1))._2.reduce(And), expr2)

    val expr3 = (('a.int === Literal(1)) && rand(1) < 1) && ('b.int === Literal(1))
    val expr4 = ('a.int === Literal(1)) && ('b.int === Literal(1))
    compareExpressions(collectProjectsAndFilters(relation.where(expr3))._2.reduce(And), expr4)

    val expr5 = ('a.int === Literal(1)) || rand(1) < 1
    assert(collectProjectsAndFilters(relation.where(expr5))._2.isEmpty)
  }

}

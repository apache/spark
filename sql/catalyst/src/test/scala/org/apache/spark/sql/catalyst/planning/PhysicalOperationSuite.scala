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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest

class PhysicalOperationSuite extends PlanTest {
  import PhysicalOperation.extractDeterministicExpressions

  test("Extract deterministic expressions") {
    val expr1 = ('col.int === Literal(1)) && rand(1) < 1
    val expr2 = 'col.int === Literal(1)
    compareExpressions(extractDeterministicExpressions(expr1).get, expr2)

    val expr3 = (('col1.int === Literal(1)) && rand(1) < 1) && ('col2.int === Literal(1))
    val expr4 = ('col1.int === Literal(1)) && ('col2.int === Literal(1))
    compareExpressions(extractDeterministicExpressions(expr3).get, expr4)

    val expr5 = ('col.int === Literal(1)) || rand(1) < 1
    val expr6 = Literal(null)
    compareExpressions(extractDeterministicExpressions(expr5).getOrElse(Literal(null)), expr6)

    val expr7 = (('col1.int === Literal(1)) && rand(1) < 1) ||
        (('col2.int === Literal(1)) && rand(1) < 1)
    val expr8 = ('col1.int === Literal(1)) || ('col2.int === Literal(1))
    compareExpressions(extractDeterministicExpressions(expr7).getOrElse(Literal(null)), expr8)
  }
}

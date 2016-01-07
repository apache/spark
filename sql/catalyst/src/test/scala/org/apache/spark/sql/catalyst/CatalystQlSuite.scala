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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Subtract, Add, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest

class CatalystQlSuite extends PlanTest {
  val parser = new CatalystQl()

  test("parse expressions") {
    compareExpressions(
      parser.createExpression("prinln('hello', 'world')"),
      UnresolvedFunction(
        "prinln", Literal("hello") :: Literal("world") :: Nil, false))

    compareExpressions(
      parser.createExpression("1 + r.r"),
      Add(Literal(1), UnresolvedAttribute("r.r")))

    compareExpressions(
      parser.createExpression("1 - f('o', o(bar))"),
      Subtract(Literal(1),
        UnresolvedFunction("f",
          Literal("o") ::
          UnresolvedFunction("o", UnresolvedAttribute("bar") :: Nil, false) ::
          Nil, false)))
  }

  test("parse union/except/intersect") {
    parser.createPlan("select * from t1 union all select * from t2")
    parser.createPlan("select * from t1 union distinct select * from t2")
    parser.createPlan("select * from t1 union select * from t2")
    parser.createPlan("select * from t1 except select * from t2")
    parser.createPlan("select * from t1 intersect select * from t2")
  }
}

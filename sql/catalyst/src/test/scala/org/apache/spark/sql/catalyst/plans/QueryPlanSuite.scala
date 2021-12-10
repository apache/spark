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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ListQuery, Literal, NamedExpression, Rand}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.types.IntegerType

class QueryPlanSuite extends SparkFunSuite {

  test("origin remains the same after mapExpressions (SPARK-23823)") {
    CurrentOrigin.setPosition(0, 0)
    val column = AttributeReference("column", IntegerType)(NamedExpression.newExprId)
    val query = DslLogicalPlan(table("table")).select(column)
    CurrentOrigin.reset()

    val mappedQuery = query mapExpressions {
      case _: Expression => Literal(1)
    }

    val mappedOrigin = mappedQuery.expressions.apply(0).origin
    assert(mappedOrigin == Origin.apply(Some(0), Some(0)))
  }

  test("collectInPlanAndSubqueries") {
    val a: NamedExpression = AttributeReference("a", IntegerType)()
    val plan =
      Union(
        Seq(
          Project(
            Seq(a),
            Filter(
              ListQuery(Project(
                Seq(a),
                Filter(
                  ListQuery(Project(
                    Seq(a),
                    UnresolvedRelation(TableIdentifier("t", None))
                  )),
                  UnresolvedRelation(TableIdentifier("t", None))
                )
              )),
              UnresolvedRelation(TableIdentifier("t", None))
            )
          ),
          Project(
            Seq(a),
            Filter(
              ListQuery(Project(
                Seq(a),
                UnresolvedRelation(TableIdentifier("t", None))
              )),
              UnresolvedRelation(TableIdentifier("t", None))
            )
          )
        )
      )

    val countRelationsInPlan = plan.collect({ case _: UnresolvedRelation => 1 }).sum
    val countRelationsInPlanAndSubqueries =
      plan.collectWithSubqueries({ case _: UnresolvedRelation => 1 }).sum

    assert(countRelationsInPlan == 2)
    assert(countRelationsInPlanAndSubqueries == 5)
  }

  test("SPARK-33035: consecutive attribute updates in parent plan nodes") {
    val testRule = new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithNewOutput {
        case p @ Project(projList, _) =>
          // Assigns new `ExprId`s for output references
          val newPlan = p.copy(projectList = projList.map { ne => Alias(ne, ne.name)() })
          val attrMapping = p.output.zip(newPlan.output)
          newPlan -> attrMapping
      }
    }

    val t = LocalRelation('a.int, 'b.int)
    val plan = t.select($"a", $"b").select($"a", $"b").select($"a", $"b").analyze
    assert(testRule(plan).resolved)
  }

  test("SPARK-37199: add a deterministic field to QueryPlan") {
    val a: NamedExpression = AttributeReference("a", IntegerType)()
    val aRand: NamedExpression = Alias(a + Rand(1), "aRand")()
    val deterministicPlan = Project(
      Seq(a),
      Filter(
        ListQuery(Project(
          Seq(a),
          UnresolvedRelation(TableIdentifier("t", None))
        )),
        UnresolvedRelation(TableIdentifier("t", None))
      )
    )
    assert(deterministicPlan.deterministic)

    val nonDeterministicPlan = Project(
      Seq(aRand),
      Filter(
        ListQuery(Project(
          Seq(a),
          UnresolvedRelation(TableIdentifier("t", None))
        )),
        UnresolvedRelation(TableIdentifier("t", None))
      )
    )
    assert(!nonDeterministicPlan.deterministic)
  }
}

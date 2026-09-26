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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias, UnresolvedCTERelation, UnresolvedWith}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, TreeNodeTag}

class HintResolutionRunnerSuite extends SparkFunSuite {

  private val RELATION_TAG = TreeNodeTag[String]("relation-tag")

  /**
   * Renames every [[UnresolvedRelation]] it sees, so that the plans the rules were applied on can
   * be told apart from the ones they were not applied on.
   */
  private object RenameRelations extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
      case unresolvedRelation: UnresolvedRelation
          if !unresolvedRelation.multipartIdentifier.last.endsWith("_renamed") =>
        unresolvedRelation.copy(
          multipartIdentifier = Seq(unresolvedRelation.multipartIdentifier.last + "_renamed")
        )
    }
  }

  private def runner = new HintResolutionRunner(Seq(RenameRelations))

  private def relationNames(plan: LogicalPlan): Seq[String] = {
    val names = Seq.newBuilder[String]
    plan.foreachWithSubqueries { operator =>
      operator match {
        case unresolvedRelation: UnresolvedRelation =>
          names += unresolvedRelation.multipartIdentifier.last
        case unresolvedWith: UnresolvedWith =>
          unresolvedWith.cteRelations.foreach { cteRelation =>
            names ++= relationNames(cteRelation.plan)
          }
        case _ =>
      }
    }
    names.result()
  }

  private def cteRelation(name: String, plan: LogicalPlan): UnresolvedCTERelation =
    UnresolvedCTERelation(name = name, plan = SubqueryAlias(name, plan))

  test("no rules leaves the plan untouched") {
    val plan = UnresolvedRelation(Seq("t")).select($"col1")
    val emptyRunner = new HintResolutionRunner(Seq.empty)

    assert(emptyRunner.resolveWithSubqueries(plan) eq plan)
  }

  test("rules are applied on the main plan") {
    val plan = UnresolvedRelation(Seq("t")).select($"col1")

    assert(relationNames(runner.resolveWithSubqueries(plan)) == Seq("t_renamed"))
  }

  test("rules are applied on CTE definitions") {
    val plan = UnresolvedWith(
      child = UnresolvedRelation(Seq("main")).select($"col1"),
      cteRelations = Seq(cteRelation("cte", UnresolvedRelation(Seq("inner")).select($"col1")))
    )

    assert(
      relationNames(runner.resolveWithSubqueries(plan)).sorted ==
      Seq("inner_renamed", "main_renamed")
    )
  }

  test("rules are applied on nested CTE definitions") {
    val innerWith = UnresolvedWith(
      child = UnresolvedRelation(Seq("innermost")).select($"col1"),
      cteRelations =
        Seq(cteRelation("inner_cte", UnresolvedRelation(Seq("nested")).select($"col1")))
    )
    val plan = UnresolvedWith(
      child = UnresolvedRelation(Seq("main")).select($"col1"),
      cteRelations = Seq(cteRelation("outer_cte", innerWith))
    )

    assert(
      relationNames(runner.resolveWithSubqueries(plan)).sorted ==
      Seq("innermost_renamed", "main_renamed", "nested_renamed")
    )
  }

  test("rules are applied on subquery plans") {
    val subquery = UnresolvedRelation(Seq("sub")).select($"col1")
    val plan = UnresolvedRelation(Seq("main")).select(ScalarSubquery(subquery).as("scalar"))

    assert(
      relationNames(runner.resolveWithSubqueries(plan)).sorted ==
      Seq("main_renamed", "sub_renamed")
    )
  }

  test("rules are applied on a subquery inside a CTE definition") {
    val subquery = UnresolvedRelation(Seq("sub")).select($"col1")
    val cteBody =
      UnresolvedRelation(Seq("inner")).select(ScalarSubquery(subquery).as("scalar"))
    val plan = UnresolvedWith(
      child = UnresolvedRelation(Seq("main")).select($"col1"),
      cteRelations = Seq(cteRelation("cte", cteBody))
    )

    assert(
      relationNames(runner.resolveWithSubqueries(plan)).sorted ==
      Seq("inner_renamed", "main_renamed", "sub_renamed")
    )
  }

  test("CTE definition keeps its alias, origin and tags") {
    val origin = Origin(line = Some(42), startPosition = Some(7))
    val alias = CurrentOrigin.withOrigin(origin) {
      SubqueryAlias("cte", UnresolvedRelation(Seq("inner")).select($"col1"))
    }
    alias.setTagValue(RELATION_TAG, "tagged")

    val plan = UnresolvedWith(
      child = UnresolvedRelation(Seq("main")).select($"col1"),
      cteRelations = Seq(UnresolvedCTERelation(name = "cte", plan = alias))
    )

    val result = runner.resolveWithSubqueries(plan).asInstanceOf[UnresolvedWith]
    val resultAlias = result.cteRelations.head.plan

    assert(resultAlias.identifier == alias.identifier)
    assert(resultAlias.origin == origin)
    assert(resultAlias.getTagValue(RELATION_TAG).contains("tagged"))
    assert(relationNames(resultAlias) == Seq("inner_renamed"))
  }

  test("CTE rules see the definition body and not the SubqueryAlias wrapper") {
    var observedRoots = Seq.empty[Class[_]]

    val observingRule = new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = {
        observedRoots :+= plan.getClass
        plan
      }
    }

    val plan = UnresolvedWith(
      child = UnresolvedRelation(Seq("main")).select($"col1"),
      cteRelations = Seq(cteRelation("cte", UnresolvedRelation(Seq("inner")).select($"col1")))
    )

    new HintResolutionRunner(Seq(observingRule)).resolveWithSubqueries(plan)

    // The rules are handed the body of the definition. The wrapper stays out of their reach,
    // because UnresolvedCTERelation.plan is typed as SubqueryAlias and a rewritten wrapper could
    // not be written back. See the HintResolutionRunner scaladoc.
    assert(observedRoots.contains(classOf[Project]))
    assert(!observedRoots.contains(classOf[SubqueryAlias]))
  }
}

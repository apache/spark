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
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.types._

object MyRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case project: Project if project.getTagValue(LogicalPlan.PLAN_ID_TAG).nonEmpty =>
      val newProject = project.copy(
        projectList = project.projectList :+ UnresolvedAttribute("b")
      )
      newProject.copyTagsFrom(project)
      newProject

    case _ => plan
  }
}


class ResolveDataFrameColumnSuite extends AnalysisTest {

  // Test Resolve DataFrame Column with a rule adding new attribute
  // 'Project ['b]
  // +- 'Project ['i]  (plan id = 0)
  //    +- LocalRelation <empty>, [i#0, b#1, d#2]
  //
  // MyRule should append column b in 'Project ['i]
  test("Resolve missing column") {
    val table = LocalRelation(
      AttributeReference("i", IntegerType)(),
      AttributeReference("b", ByteType)(),
      AttributeReference("d", DoubleType)())

    val u1 = UnresolvedAttribute("i")
    u1.setTagValue[Long](LogicalPlan.PLAN_ID_TAG, 0L)

    val project1 = Project(Seq(u1), table)
    project1.setTagValue[Long](LogicalPlan.PLAN_ID_TAG, 0L)

    val u2 = UnresolvedAttribute("b")
    u2.setTagValue[Long](LogicalPlan.PLAN_ID_TAG, 0L)

    val project2 = Project(Seq(u2), project1)

    val rules = Seq(MyRule)
    val analyzer = new RuleExecutor[LogicalPlan] {
      override val batches = Seq(Batch("Resolution", FixedPoint(2), rules: _*))
    }

    val analyzed = analyzer.execute(project2)
    assert(analyzed.resolved, s"\n$analyzed")
  }
}

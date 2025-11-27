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
    case project: Project =>
      val newProject = project.copy(
        projectList = project.projectList :+ AttributeReference("b", ByteType)()
      )
      newProject.copyTagsFrom(project)
      newProject
  }
}

class ResolveDataFrameColumnSuite extends AnalysisTest {

  // Test Resolve DataFrame Column with a rule adding new attribute
  test("Resolve Post Attached Column") {
    val table = LocalRelation(
      AttributeReference("i", IntegerType)(),
      AttributeReference("b", ByteType)(),
      AttributeReference("d", DoubleType)())

    val project = Project(Seq(AttributeReference("i", IntegerType)()), table)
    project.setTagValue(LogicalPlan.PLAN_ID_TAG, 0)


  }
}

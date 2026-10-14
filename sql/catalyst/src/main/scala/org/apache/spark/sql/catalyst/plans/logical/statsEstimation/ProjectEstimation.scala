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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import org.apache.spark.sql.catalyst.expressions.{AttributeMap, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Project, Statistics}

object ProjectEstimation {
  import EstimationUtils._

  def estimate(project: Project): Option[Statistics] = {
    estimate(project.projectList, project.child.stats)
  }

  def estimate(
      projectList: Seq[NamedExpression],
      childStats: Statistics): Option[Statistics] = {
    childStats.rowCount.map { rowCount =>
      val output = projectList.map(_.toAttribute)
      val aliasStats = EstimationUtils.getAliasStats(
        projectList, childStats.attributeStats, rowCount)

      val outputAttrStats =
        getOutputMap(AttributeMap(childStats.attributeStats.toSeq ++ aliasStats), output)
      childStats.copy(
        sizeInBytes = getOutputSize(output, rowCount, outputAttrStats),
        attributeStats = outputAttrStats)
    }
  }
}

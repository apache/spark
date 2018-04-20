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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{Project, Statistics}

object ProjectEstimation {
  import EstimationUtils._

  def estimate(project: Project): Option[Statistics] = {
    if (rowCountsExist(project.child)) {
      val childStats = project.child.stats
      val inputAttrStats = childStats.attributeStats
      // Match alias with its child's column stat
      val aliasStats = project.expressions.collect {
        case alias @ Alias(attr: Attribute, _) if inputAttrStats.contains(attr) =>
          alias.toAttribute -> inputAttrStats(attr)
      }
      val outputAttrStats =
        getOutputMap(AttributeMap(inputAttrStats.toSeq ++ aliasStats), project.output)
      Some(childStats.copy(
        sizeInBytes = getOutputSize(project.output, childStats.rowCount.get, outputAttrStats),
        attributeStats = outputAttrStats))
    } else {
      None
    }
  }
}

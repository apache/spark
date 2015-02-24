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

package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{UnknownPartitioning, Partitioning}

/**
 * Apply the all of the GroupExpressions to every input row, hence we will get
 * multiple output rows for a input row.
 * @param projections The group of expressions, all of the group expressions should
 *                    output the same schema specified bye the parameter `output`
 * @param output      The output Schema
 * @param child       Child operator
 */
@DeveloperApi
case class Expand(
    projections: Seq[GroupExpression],
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryNode {

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      // TODO Move out projection objects creation and transfer to
      // workers via closure. However we can't assume the Projection
      // is serializable because of the code gen, so we have to
      // create the projections within each of the partition processing.
      val groups = projections.map(ee => newProjection(ee.children, child.output)).toArray

      new Iterator[Row] {
        private[this] var result: Row = _
        private[this] var idx = -1  // -1 means the initial state
        private[this] var input: Row = _

        override final def hasNext = (-1 < idx && idx < groups.length) || iter.hasNext

        override final def next(): Row = {
          if (idx <= 0) {
            // in the initial (-1) or beginning(0) of a new input row, fetch the next input tuple
            input = iter.next()
            idx = 0
          }

          result = groups(idx)(input)
          idx += 1

          if (idx == groups.length && iter.hasNext) {
            idx = 0
          }

          result
        }
      }
    }
  }
}

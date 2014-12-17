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

@DeveloperApi
case class Expand(
    projections: Seq[GroupExpression],
    gid: Attribute,
    child: SparkPlan)
  extends UnaryNode {

  // the output schema is quite same with input, but followed by the Grouping ID
  def output = child.output :+ gid

  // We change the output data partitioning in the analytics function (GroupingSets)
  // The output partitioning does not conform to its child, set it as UNKNOWN
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val input = child.output

      // For each input row, we will project to couple of different rows as
      // output, according to the projections.
      // TODO Move out projection objects creation and transfer to
      // workers via closure. However we can't assume the Projection
      // is serializable because of the code gen, so we have to
      // create the projections within each of the partition.
      val groups = projections.map(ee => newProjection(ee.children, input)).toArray

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

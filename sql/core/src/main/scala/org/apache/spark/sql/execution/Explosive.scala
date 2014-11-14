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
case class Explosive(
    projections: Seq[GroupExpression],
    output: Seq[Attribute],
    child: SparkPlan)(@transient sqlContext: SQLContext)
  extends UnaryNode {

  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def otherCopyArgs = sqlContext :: Nil

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      // TODO InterpretProjection is not Serializable
      val projs = projections.map(ee => newProjection(ee.children, output)).toArray

      new Iterator[Row] {
        private[this] var result: Row = _
        private[this] var idx = -1  // initial value is set as -1
        private[this] var input: Row = _

        override final def hasNext = (-1 < idx && idx < projs.length) || iter.hasNext

        override final def next(): Row = {
          if (idx <= 0) {
            // in the beginning or end of iteration, fetch the next input tuple
            input = iter.next()
            idx = 0
          }

          result = projs(idx)(input)
          idx += 1

          if (idx == projs.length && iter.hasNext) {
            idx = 0
          }

          result
        }
      }
    }
  }
}

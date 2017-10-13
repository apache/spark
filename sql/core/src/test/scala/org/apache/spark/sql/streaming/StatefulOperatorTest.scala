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

package org.apache.spark.sql.streaming

import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.streaming._

trait StatefulOperatorTest {
  /**
   * Check that the output partitioning of a child operator of a Stateful operator satisfies the
   * distribution that we expect for our Stateful operator.
   */
  protected def checkChildOutputHashPartitioning[T <: StatefulOperator](
      sq: StreamingQuery,
      colNames: Seq[String]): Boolean = {
    val attr = sq.asInstanceOf[StreamExecution].lastExecution.analyzed.output
    val partitions = sq.sparkSession.sessionState.conf.numShufflePartitions
    val groupingAttr = attr.filter(a => colNames.contains(a.name))
    checkChildOutputPartitioning(sq, HashPartitioning(groupingAttr, partitions))
  }

  /**
   * Check that the output partitioning of a child operator of a Stateful operator satisfies the
   * distribution that we expect for our Stateful operator.
   */
  protected def checkChildOutputPartitioning[T <: StatefulOperator](
      sq: StreamingQuery,
      expectedPartitioning: Partitioning): Boolean = {
    val operator = sq.asInstanceOf[StreamExecution].lastExecution
      .executedPlan.collect { case p: T => p }
    operator.head.children.forall(
      _.outputPartitioning.numPartitions == expectedPartitioning.numPartitions)
  }
}

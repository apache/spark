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

import org.apache.spark.rdd.{RDD, RecombinationedRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}

/**
 * A operator to redistribute the child operator's RDD partitions.
 * Kinda like ShuffleExchangeExec, but ShuffleExchangeExec make the redistribution
 * on each row of child's output, and PartitionDistributionExec just make the redistribution
 * on child's RDD partition. It provides the capability to reorder, remove, duplicate...
 * any RDD partition level's operation.
 *
 * @param f the function to apply on child RDD partitions. It takes a sequence of partition indexes
 *          as input, and output a new sequence to represent the new partitions combination.
 * @param child Child plan
 */

case class PartitionRecombinationExec(
    f: (Seq[Int] => Seq[Int]),
    targetPartitionNum: Int,
    child: SparkPlan) extends SparkPlan {

  override protected def doExecute(): RDD[InternalRow] = {
    new RecombinationedRDD[InternalRow](child.execute(), f)
  }

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = UnknownPartitioning(targetPartitionNum)
}

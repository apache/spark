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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees.TreePattern.{DISTRIBUTED_SEQUENCE_ID, TreePattern}
import org.apache.spark.sql.types.{DataType, LongType}

/**
 * Returns increasing 64-bit integers consecutive from 0.
 * The generated ID is guaranteed to be increasing consecutive started from 0.
 *
 * @note this expression is dedicated for Pandas API on Spark to use.
 */
case class DistributedSequenceID() extends LeafExpression with Unevaluable with NonSQLExpression {

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    DistributedSequenceID()
  }

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  final override val nodePatterns: Seq[TreePattern] = Seq(DISTRIBUTED_SEQUENCE_ID)

  override def nodeName: String = "distributed_sequence_id"
}

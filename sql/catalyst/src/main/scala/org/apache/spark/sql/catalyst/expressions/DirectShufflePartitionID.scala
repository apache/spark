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

import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType}

/**
 * Expression that takes a partition ID value and passes it through directly for use in
 * shuffle partitioning. This is used with RepartitionByExpression to allow users to
 * directly specify target partition IDs.
 *
 * The child expression must evaluate to an integral type and must not be null.
 * The resulting partition ID must be in the range [0, numPartitions).
 */
case class DirectShufflePartitionID(child: Expression)
  extends UnaryExpression
  with ExpectsInputTypes
  with Unevaluable {

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = IntegerType :: Nil

  override def nullable: Boolean = false

  override val prettyName: String = "direct_shuffle_partition_id"

  override protected def withNewChildInternal(newChild: Expression): DirectShufflePartitionID =
    copy(child = newChild)
}


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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

/**
 * Returns the current date at the start of query evaluation.
 * All calls of current_date within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
case class CurrentDate() extends LeafExpression {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = {
    DateTimeUtils.millisToDays(System.currentTimeMillis())
  }
}

/**
 * Returns the current timestamp at the start of query evaluation.
 * All calls of current_timestamp within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
case class CurrentTimestamp() extends LeafExpression {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = TimestampType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis() * 1000L
  }
}

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
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, NullType}


abstract class SubQueryExpression extends LeafExpression {
  def query: LogicalPlan
  def withNewPlan(plan: LogicalPlan): SubQueryExpression
}

case class ScalarSubQuery(query: LogicalPlan) extends SubQueryExpression with CodegenFallback {
  override lazy val resolved: Boolean = query.resolved
  override def dataType: DataType = {
    if (resolved) {
      query.schema.fields(0).dataType
    } else {
      NullType
    }
  }
  override def nullable: Boolean = true
  override def foldable: Boolean = true

  override def withNewPlan(plan: LogicalPlan): ScalarSubQuery = ScalarSubQuery(plan)

  private lazy val result: Any = {
    // SQLContext
  }
  override def eval(input: InternalRow): Any = {

  }
}

case class ListSubQuery(query: LogicalPlan)
  extends SubQueryExpression with Unevaluable  {
  override lazy val resolved: Boolean = false  // can't be resolved
  override def dataType: DataType = ArrayType(NullType)
  override def nullable: Boolean = true
  override def withNewPlan(plan: LogicalPlan): ListSubQuery = ListSubQuery(plan)
}

case class Exists(query: LogicalPlan) extends SubQueryExpression with Unevaluable {
  override lazy val resolved: Boolean = false  // can't be resolved
  override def dataType: DataType = BooleanType
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): Exists = Exists(plan)
}

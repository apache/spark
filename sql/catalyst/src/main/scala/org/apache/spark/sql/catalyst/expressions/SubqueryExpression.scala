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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Evaluates whether `subquery` result contains `value`. 
 * For example : 'SELECT * FROM src a WHERE a.key in (SELECT b.key FROM src b)'
 * @param value   In the above example 'a.key' is 'value'
 * @param subquery  In the above example 'SELECT b.key FROM src b' is 'subquery'
 */
case class SubqueryExpression(value: Expression, subquery: LogicalPlan) extends Expression {

  type EvaluatedType = Any
  def dataType = value.dataType
  override def foldable = value.foldable
  def nullable = value.nullable
  override def toString = s"SubqueryExpression($value, $subquery)"
  override lazy val resolved = childrenResolved
  def children = value :: Nil
  override def eval(input: Row): Any =
    sys.error(s"SubqueryExpression eval should not be called since it will be converted"
        +" to LeftSemiJoin query")
}

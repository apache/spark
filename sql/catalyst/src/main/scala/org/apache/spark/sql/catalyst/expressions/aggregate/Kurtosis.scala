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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DoubleType

case class Kurtosis(child: Expression) extends CentralMomentAgg(child) {

  override protected val momentOrder = 4

  override val evaluateExpression: Expression = {
    If(EqualTo(count, Literal(0.0)), Literal.create(null, DoubleType),
      If(EqualTo(m2, Literal(0.0)), Literal(Double.NaN),
        count * m4 / (m2 * m2) - Literal(3.0)))
  }

  override def prettyName: String = "kurtosis"
}

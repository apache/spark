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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

/**
 * If the UDF is deterministic and if the children are all literal, we can replace the udf
 * with the output of the udf serialized
 */
object DeterministicLiteralUDF extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan =
    if (!SQLConf.get.deterministicLiteralUdfFoldEnabled) {
      plan
    } else plan transformAllExpressions {
      case udf @ ScalaUDF(_, dataType, children, _, _, _, _, udfDeterministic)
        if udf.deterministic && children.forall(_.isInstanceOf[Literal]) => {
        val res = udf.eval(null)
        Literal(res, dataType)
      }
    }
}

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

package org.apache.spark.sql.catalyst.analysis

import java.util.LinkedHashMap

import org.apache.spark.sql.catalyst.expressions._

object NondeterministicExpressionCollection {
  def getNondeterministicToAttributes(
      expressions: Seq[Expression]): LinkedHashMap[Expression, NamedExpression] = {
    val nonDeterministicToAttributes = new LinkedHashMap[Expression, NamedExpression]
    expressions
      .filterNot(_.deterministic)
      .flatMap { expr =>
        val leafNondeterministic = expr.collect {
          case n: Nondeterministic => n
          case udf: UserDefinedExpression if !udf.deterministic => udf
        }
        leafNondeterministic.distinct.map { e =>
          val ne = e match {
            case n: NamedExpression => n
            case _ => Alias(e, "_nondeterministic")()
          }
          nonDeterministicToAttributes.put(e, ne)
        }
      }
    nonDeterministicToAttributes
  }
}

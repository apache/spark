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

import org.apache.spark.sql.catalyst.expressions.{BooleanLiteral, Expression, IntegerLiteral}
import org.apache.spark.sql.errors.QueryCompilationErrors

private[expressions] object PandasAggregate {
  def expressionToIgnoreNA(e: Expression, source: String): Boolean = e match {
    case BooleanLiteral(ignoreNA) => ignoreNA
    case _ => throw QueryCompilationErrors.invalidIgnoreNAParameter(source, e)
  }

  def expressionToDDOF(e: Expression, source: String): Int = e match {
    case IntegerLiteral(ddof) => ddof
    case _ => throw QueryCompilationErrors.invalidDdofParameter(source, e)
  }
}

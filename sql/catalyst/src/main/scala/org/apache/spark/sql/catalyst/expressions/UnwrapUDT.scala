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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, UserDefinedType}


/**
 * Unwrap UDT data type column into its underlying type.
 */
case class UnwrapUDT(child: Expression) extends UnaryExpression with NonSQLExpression {

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = child.genCode(ctx)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType.isInstanceOf[UserDefinedType[_]]) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"Input type should be UserDefinedType but got ${child.dataType.catalogString}")
    }
  }
  override def dataType: DataType = child.dataType.asInstanceOf[UserDefinedType[_]].sqlType

  override def nullSafeEval(input: Any): Any = input

  override def prettyName: String = "unwrap_udt"

  override protected def withNewChildInternal(newChild: Expression): UnwrapUDT = {
    copy(child = newChild)
  }
}

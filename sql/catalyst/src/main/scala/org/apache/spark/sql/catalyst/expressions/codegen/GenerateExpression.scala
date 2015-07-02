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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression}

import scala.runtime.AbstractFunction1

object GenerateExpression extends CodeGenerator[Expression, InternalRow => Any] {

  override protected def canonicalize(in: Expression): Expression = {
    ExpressionCanonicalizer.execute(in)
  }

  override protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression = {
    BindReferences.bindReference(in, inputSchema)
  }

  override protected def create(expr: Expression): InternalRow => Any = {
    val ctx = newCodeGenContext()
    val eval = expr.gen(ctx)
    val code =
      s"""
         |class SpecificExpression extends
         |  ${classOf[AbstractFunction1[InternalRow, Any]].getName}<${classOf[InternalRow].getName}, Object> {
         |
         |  @Override
         |  public SpecificExpression generate($exprType[] expr) {
         |    return new SpecificExpression(expr);
         |  }
         |
         |  @Override
         |  public Object apply(InternalRow i) {
         |    ${eval.code}
         |    return ${eval.isNull} ? null : ${eval.primitive};
         |  }
         | }
       """.stripMargin
    logDebug(s"Generated expression '$expr':\n$code")
    println(code)
    compile(code).generate(ctx.references.toArray).asInstanceOf[InternalRow => Any]
  }
}

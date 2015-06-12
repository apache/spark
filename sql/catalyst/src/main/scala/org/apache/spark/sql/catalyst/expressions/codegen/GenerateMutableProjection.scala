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

import org.apache.spark.sql.catalyst.expressions._

// MutableProjection is not accessible in Java
abstract class BaseMutableProjection extends MutableProjection {}

/**
 * Generates byte code that produces a [[MutableRow]] object that can update itself based on a new
 * input [[InternalRow]] for a fixed set of [[Expression Expressions]].
 */
object GenerateMutableProjection extends CodeGenerator[Seq[Expression], () => MutableProjection] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(expressions: Seq[Expression]): (() => MutableProjection) = {
    val ctx = newCodeGenContext()
    val projectionCode = expressions.zipWithIndex.map { case (e, i) =>
      val evaluationCode = e.gen(ctx)
      evaluationCode.code +
        s"""
          if(${evaluationCode.isNull})
            mutableRow.setNullAt($i);
          else
            mutableRow.${ctx.setColumn(e.dataType, i, evaluationCode.primitive)};
        """
    }.mkString("\n")
    val code = s"""
      import org.apache.spark.sql.InternalRow;

      public SpecificProjection generate($exprType[] expr) {
        return new SpecificProjection(expr);
      }

      class SpecificProjection extends ${classOf[BaseMutableProjection].getName} {

        private $exprType[] expressions = null;
        private $mutableRowType mutableRow = null;

        public SpecificProjection($exprType[] expr) {
          expressions = expr;
          mutableRow = new $genericMutableRowType(${expressions.size});
        }

        public ${classOf[BaseMutableProjection].getName} target($mutableRowType row) {
          mutableRow = row;
          return this;
        }

        /* Provide immutable access to the last projected row. */
        public InternalRow currentValue() {
          return (InternalRow) mutableRow;
        }

        public Object apply(Object _i) {
          InternalRow i = (InternalRow) _i;
          $projectionCode

          return mutableRow;
        }
      }
    """

    logDebug(s"code for ${expressions.mkString(",")}:\n$code")

    val c = compile(code)
    // fetch the only one method `generate(Expression[])`
    val m = c.getDeclaredMethods()(0)
    () => {
      m.invoke(c.newInstance(), ctx.references.toArray).asInstanceOf[BaseMutableProjection]
    }
  }
}

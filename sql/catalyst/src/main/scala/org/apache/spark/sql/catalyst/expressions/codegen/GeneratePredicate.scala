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

/**
 * Interface for generated predicate
 */
trait Predicate {
  def predict(r: Row): Boolean
}

/**
 * Generates bytecode that evaluates a boolean [[Expression]] on a given input [[Row]].
 */
object GeneratePredicate extends CodeGenerator[Expression, (Row) => Boolean] {
  import scala.reflect.runtime.universe._

  protected def canonicalize(in: Expression): Expression = ExpressionCanonicalizer.execute(in)

  protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression =
    BindReferences.bindReference(in, inputSchema)

  protected def create(predicate: Expression): ((Row) => Boolean) = {
    val ctx = newCodeGenContext()
    val eval = expressionEvaluator(predicate, ctx)
    val code = s"""
      import org.apache.spark.sql.Row;

      public SpecificPredicate generate($exprType[] expr) {
        return new SpecificPredicate(expr);
      }

      class SpecificPredicate implements ${typeOf[Predicate]} {
        private final $exprType[] expressions;
        public SpecificPredicate($exprType[] expr) {
        System.err.println("number of expressions");
          System.err.println(expr.length);
          expressions = expr;
        }

        @Override
        public boolean predict(Row i) {
          ${eval.code}
          return !${eval.nullTerm} && ${eval.primitiveTerm};
        }
      }"""

    logWarning(s"Generated predicate '$predicate':\n$code")

    val c = compile(code)
    val m = c.getDeclaredMethods()(0)
    val p = m.invoke(c.newInstance(), ctx.references.toArray).asInstanceOf[Predicate]
    (r: Row) => p.predict(r)
  }
}

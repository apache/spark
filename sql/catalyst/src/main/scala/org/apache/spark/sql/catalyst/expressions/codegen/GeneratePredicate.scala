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
 * Generates bytecode that evaluates a boolean [[Expression]] on a given input [[InternalRow]].
 */
object GeneratePredicate extends CodeGenerator[Expression, BasePredicate] {

  protected def canonicalize(in: Expression): Expression = ExpressionCanonicalizer.execute(in)

  protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression =
    BindReferences.bindReference(in, inputSchema)

  def generate(expressions: Expression, useSubexprElimination: Boolean): BasePredicate =
    create(canonicalize(expressions), useSubexprElimination)

  protected def create(predicate: Expression): BasePredicate = create(predicate, false)

  protected def create(predicate: Expression, useSubexprElimination: Boolean): BasePredicate = {
    val ctx = newCodeGenContext()

    // Do sub-expression elimination for predicates.
    val eval = ctx.generateExpressions(Seq(predicate), useSubexprElimination).head
    val evalSubexpr = ctx.subexprFunctionsCode

    val codeBody = s"""
      public SpecificPredicate generate(Object[] references) {
        return new SpecificPredicate(references);
      }

      class SpecificPredicate extends ${classOf[BasePredicate].getName} {
        private final Object[] references;
        ${ctx.declareMutableStates()}

        public SpecificPredicate(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        public boolean eval(InternalRow ${ctx.INPUT_ROW}) {
          $evalSubexpr
          ${eval.code}
          return !${eval.isNull} && ${eval.value};
        }

        ${ctx.declareAddedFunctions()}
      }"""

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated predicate '$predicate':\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[BasePredicate]
  }
}

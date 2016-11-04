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
import org.apache.spark.sql.catalyst.expressions._

/**
 * Interface for generated predicate
 */
abstract class Predicate {
  def eval(r: InternalRow): Boolean

  /**
   * Initializes internal states given the current partition index.
   * This is used by nondeterministic expressions to set initial states.
   * The default implementation does nothing.
   */
  def initialize(partitionIndex: Int): Unit = {}
}

/**
 * Generates bytecode that evaluates a boolean [[Expression]] on a given input [[InternalRow]].
 */
object GeneratePredicate extends CodeGenerator[Expression, Predicate] {

  protected def canonicalize(in: Expression): Expression = ExpressionCanonicalizer.execute(in)

  protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression =
    BindReferences.bindReference(in, inputSchema)

  protected def create(predicate: Expression): Predicate = {
    val ctx = newCodeGenContext()
    val eval = predicate.genCode(ctx)

    val codeBody = s"""
      public SpecificPredicate generate(Object[] references) {
        return new SpecificPredicate(references);
      }

      class SpecificPredicate extends ${classOf[Predicate].getName} {
        private final Object[] references;
        ${ctx.declareMutableStates()}

        public SpecificPredicate(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        ${ctx.declareAddedFunctions()}

        public boolean eval(InternalRow ${ctx.INPUT_ROW}) {
          ${eval.code}
          return !${eval.isNull} && ${eval.value};
        }
      }"""

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated predicate '$predicate':\n${CodeFormatter.format(code)}")

    CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[Predicate]
  }
}

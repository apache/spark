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
    // With a subexpression common to the top-level conjuncts, whether several of them share it or
    // one of them repeats it, the conjuncts are generated one by one, and the subexpression is
    // evaluated just before the first conjunct that references it, not before the whole
    // condition, so that a FALSE conjunct before them stops the evaluation, as `And.eval` and the
    // whole-stage codegen of a filter do (SPARK-60332). The conjuncts are evaluated like
    // `And.eval`: a FALSE one stops the evaluation, a NULL one does not, and the condition is TRUE
    // only if all of them are. Otherwise the condition is generated as a whole, as before.
    val severalConjuncts = predicate.isInstanceOf[And]
    val conjunctCodes = if (useSubexprElimination && severalConjuncts) {
      ctx.generateConjunctsWithSubexprCodes(predicate)
    } else {
      None
    }
    val evalCode = conjunctCodes match {
      case Some(codes) =>
        // A field rather than a local variable, since the checks of the conjuncts are split into
        // functions when their code is long, see `splitExpressions`, with a FALSE conjunct
        // returning false from its function and from `eval`.
        val nullConjunct = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "nullConjunct")
        val checks = codes.map { case (subexprs, ev) =>
          s"""
             |$subexprs
             |${ev.code}
             |if (${ev.isNull}) {
             |  $nullConjunct = true;
             |} else if (!${ev.value}) {
             |  return false;
             |}
           """.stripMargin
        }
        val checksCode = ctx.splitExpressions(
          checks,
          "evalConjuncts",
          Seq("InternalRow" -> ctx.INPUT_ROW),
          returnType = "boolean",
          makeSplitFunction = body => s"$body\nreturn true;",
          foldFunctions = calls => calls.map(call => s"if (!$call) return false;").mkString("\n"))
        s"""
           |$nullConjunct = false;
           |$checksCode
           |return !$nullConjunct;
         """.stripMargin
      case None =>
        // With more than one conjunct, no subexpression is eliminated when they were looked at, so
        // the elimination is not run again on them.
        val eliminate = useSubexprElimination && !severalConjuncts
        val eval = ctx.generateExpressions(Seq(predicate), eliminate).head
        s"""
           |${ctx.subexprFunctionsCode}
           |${eval.code}
           |return !${eval.isNull} && ${eval.value};
         """.stripMargin
    }

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
          $evalCode
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

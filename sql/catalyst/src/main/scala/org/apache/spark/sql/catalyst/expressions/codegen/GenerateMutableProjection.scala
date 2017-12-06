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
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp

// MutableProjection is not accessible in Java
abstract class BaseMutableProjection extends MutableProjection

/**
 * Generates byte code that produces a [[InternalRow]] object that can update itself based on a new
 * input [[InternalRow]] for a fixed set of [[Expression Expressions]].
 * It exposes a `target` method, which is used to set the row that will be updated.
 * The internal [[InternalRow]] object created internally is used only when `target` is not used.
 */
object GenerateMutableProjection extends CodeGenerator[Seq[Expression], MutableProjection] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  def generate(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute],
      useSubexprElimination: Boolean): MutableProjection = {
    create(canonicalize(bind(expressions, inputSchema)), useSubexprElimination)
  }

  protected def create(expressions: Seq[Expression]): MutableProjection = {
    create(expressions, false)
  }

  private def create(
      expressions: Seq[Expression],
      useSubexprElimination: Boolean): MutableProjection = {
    val ctx = newCodeGenContext()
    val (validExpr, index) = expressions.zipWithIndex.filter {
      case (NoOp, _) => false
      case _ => true
    }.unzip
    val exprVals = ctx.generateExpressions(validExpr, useSubexprElimination)
    val projectionAndUpdatesCode = exprVals.zip(index).map {
      case (ev, i) =>
        val e = expressions(i)
        val isNull = s"isNull_$i"
        val value = s"value_$i"
        // code for the projection
        val proj = if (e.nullable) {
          s"""
             |${ev.code}
             |final ${ctx.JAVA_BOOLEAN} $isNull = ${ev.isNull};
             |final ${ctx.javaType(e.dataType)} $value = ${ev.value};
           """.stripMargin
        } else {
          s"""
             |${ev.code}
             |final ${ctx.javaType(e.dataType)} $value = ${ev.value};
           """.stripMargin
        }
        // code for updating the mutableRow
        val updateExpr = ExprCode("", isNull, value)
        val update = ctx.updateColumn("mutableRow", e.dataType, i, updateExpr, e.nullable)
        // code which performs the projection and updates the mutableRow
        proj + update
    }

    // Evaluate all the subexpressions.
    val evalSubexpr = ctx.subexprFunctions.mkString("\n")

    val allProjectionsAndUpdates = ctx.splitExpressionsWithCurrentInputs(projectionAndUpdatesCode)

    val codeBody =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificMutableProjection(references);
         |}
         |
         |class SpecificMutableProjection extends ${classOf[BaseMutableProjection].getName} {
         |
         |  private Object[] references;
         |  private InternalRow mutableRow;
         |  ${ctx.declareMutableStates()}
         |
         |  public SpecificMutableProjection(Object[] references) {
         |    this.references = references;
         |    mutableRow = new $genericMutableRowType(${expressions.size});
         |    ${ctx.initMutableStates()}
         |  }
         |
         |  public void initialize(int partitionIndex) {
         |    ${ctx.initPartition()}
         |  }
         |
         |  public ${classOf[BaseMutableProjection].getName} target(InternalRow row) {
         |    mutableRow = row;
         |    return this;
         |  }
         |
         |  /* Provide immutable access to the last projected row. */
         |  public InternalRow currentValue() {
         |    return (InternalRow) mutableRow;
         |  }
         |
         |  public java.lang.Object apply(java.lang.Object _i) {
         |    InternalRow ${ctx.INPUT_ROW} = (InternalRow) _i;
         |    $evalSubexpr
         |    // evaluate all the projections and update mutableRow
         |    $allProjectionsAndUpdates
         |    return mutableRow;
         |  }
         |
         |  ${ctx.declareAddedFunctions()}
         |}
       """.stripMargin

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[MutableProjection]
  }
}

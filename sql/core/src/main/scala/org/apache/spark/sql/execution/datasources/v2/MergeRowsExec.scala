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

package org.apache.spark.sql.execution.datasources.v2

import org.roaringbitmap.longlong.Roaring64Bitmap

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.BasePredicate
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Projection
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral, GeneratePredicate, JavaCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Context, Copy, Delete, Discard, Insert, Instruction, Keep, ROW_ID, Split, Update}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.BooleanType

case class MergeRowsExec(
    isSourceRowPresent: Expression,
    isTargetRowPresent: Expression,
    matchedInstructions: Seq[Instruction],
    notMatchedInstructions: Seq[Instruction],
    notMatchedBySourceInstructions: Seq[Instruction],
    checkCardinality: Boolean,
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numTargetRowsCopied" -> SQLMetrics.createMetric(sparkContext,
      "number of target rows copied unmodified because they did not match any action"),
    "numTargetRowsInserted" -> SQLMetrics.createMetric(sparkContext,
      "number of target rows inserted"),
    "numTargetRowsDeleted" -> SQLMetrics.createMetric(sparkContext,
      "number of target rows deleted"),
    "numTargetRowsUpdated" -> SQLMetrics.createMetric(sparkContext,
      "number of target rows updated"),
    "numTargetRowsMatchedUpdated" -> SQLMetrics.createMetric(sparkContext,
      "number of target rows updated by a matched clause"),
    "numTargetRowsMatchedDeleted" -> SQLMetrics.createMetric(sparkContext,
      "number of target rows deleted by a matched clause"),
    "numTargetRowsNotMatchedBySourceUpdated" -> SQLMetrics.createMetric(sparkContext,
      "number of target rows updated by a not matched by source clause"),
    "numTargetRowsNotMatchedBySourceDeleted" -> SQLMetrics.createMetric(sparkContext,
      "number of target rows deleted by a not matched by source clause"))

  @transient override lazy val producedAttributes: AttributeSet = {
    AttributeSet(output.filterNot(attr => inputSet.contains(attr)))
  }

  @transient
  override lazy val references: AttributeSet = {
    val usedExprs = if (checkCardinality) {
      val rowIdAttr = child.output.find(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdAttr.isDefined, "Cannot find row ID attr")
      rowIdAttr.get +: expressions
    } else {
      expressions
    }
    AttributeSet.fromAttributeSets(usedExprs.map(_.references)) -- producedAttributes
  }

  override def simpleString(maxFields: Int): String = {
    s"MergeRowsExec${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions(processPartition)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    // Save the input variables that were passed to doConsume
    val inputCurrentVars = input

    // code for instruction execution code
    generateInstructionExecutionCode(ctx, inputCurrentVars)
  }


  /**
   * code for cardinality validation
   */
  private def generateCardinalityValidationCode(ctx: CodegenContext, rowIdOrdinal: Int,
                                                input: Seq[ExprCode]): ExprCode = {
    val bitmapClass = classOf[Roaring64Bitmap]
    val rowIdBitmap = ctx.addMutableState(bitmapClass.getName, "matchedRowIds",
      v => s"$v = new ${bitmapClass.getName}();")

    val currentRowId = input(rowIdOrdinal)
    val queryExecutionErrorsClass = QueryExecutionErrors.getClass.getName + ".MODULE$"
    val code =
      code"""
            |${currentRowId.code}
            |if ($rowIdBitmap.contains(${currentRowId.value})) {
            |  throw $queryExecutionErrorsClass.mergeCardinalityViolationError();
            |}
            |$rowIdBitmap.add(${currentRowId.value});
     """.stripMargin
    ExprCode(code, FalseLiteral, JavaCode.variable(rowIdBitmap, bitmapClass))
  }

  /**
   * Generate code for instruction execution based on row presence conditions
   */
  private def generateInstructionExecutionCode(ctx: CodegenContext,
                                               inputExprs: Seq[ExprCode]): String = {

    // code for evaluating src/tgt presence conditions
    val sourcePresentExpr = generatePredicateCode(ctx, isSourceRowPresent, child.output, inputExprs)
    val targetPresentExpr = generatePredicateCode(ctx, isTargetRowPresent, child.output, inputExprs)

    // code for each instruction type
    val matchedInstructionsCode = generateInstructionsCode(ctx, matchedInstructions,
      "matched", inputExprs, sourcePresent = true)
    val notMatchedInstructionsCode = generateInstructionsCode(ctx, notMatchedInstructions,
      "notMatched", inputExprs, sourcePresent = true)
    val notMatchedBySourceInstructionsCode = generateInstructionsCode(ctx,
      notMatchedBySourceInstructions, "notMatchedBySource", inputExprs, sourcePresent = false)

    val cardinalityValidationCode = if (checkCardinality) {
      val rowIdOrdinal = child.output.indexWhere(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdOrdinal != -1, "Cannot find row ID attr")
      generateCardinalityValidationCode(ctx, rowIdOrdinal, inputExprs).code
    } else {
      ""
    }

    s"""
       |${sourcePresentExpr.code}
       |${targetPresentExpr.code}
       |
       |if (${targetPresentExpr.value} && ${sourcePresentExpr.value}) {
       |  $cardinalityValidationCode
       |  $matchedInstructionsCode
       |} else if (${sourcePresentExpr.value}) {
       |  $notMatchedInstructionsCode
       |} else if (${targetPresentExpr.value}) {
       |  $notMatchedBySourceInstructionsCode
       |}
     """.stripMargin
  }

  /**
   * Generate code for executing a sequence of instructions
   */
  private def generateInstructionsCode(ctx: CodegenContext, instructions: Seq[Instruction],
                                       instructionType: String,
                                       inputExprs: Seq[ExprCode],
                                       sourcePresent: Boolean): String = {
    if (instructions.isEmpty) {
      ""
    } else {
      val instructionCodes = instructions.map(instruction =>
        generateSingleInstructionCode(ctx, instruction, inputExprs, sourcePresent))

      s"""
         |${instructionCodes.mkString("\n")}
         |return;
       """.stripMargin
    }
  }

  private def generateSingleInstructionCode(ctx: CodegenContext,
                                            instruction: Instruction,
                                            inputExprs: Seq[ExprCode],
                                            sourcePresent: Boolean): String = {
    instruction match {
      case Keep(context, condition, outputExprs) =>
        val projectionExpr = generateProjectionCode(ctx, outputExprs, inputExprs)
        val code = generatePredicateCode(ctx, condition, child.output, inputExprs)

        // Generate metric updates based on context
        val metricUpdateCode = generateMetricUpdateCode(ctx, context, sourcePresent)

        s"""
           |${code.code}
           |if (${code.value}) {
           |  $metricUpdateCode
           |  ${consume(ctx, projectionExpr)}
           |  return;
           |}
       """.stripMargin

      case Discard(condition) =>
        val code = generatePredicateCode(ctx, condition, child.output, inputExprs)
        val metricUpdateCode = generateDeleteMetricUpdateCode(ctx, sourcePresent)

        s"""
           |${code.code}
           |if (${code.value}) {
           |  $metricUpdateCode
           |  return; // Discar row
           |}
       """.stripMargin

      case Split(condition, outputExprs, otherOutputExprs) =>
        val projectionExpr = generateProjectionCode(ctx, outputExprs, inputExprs)
        val otherProjectionExpr = generateProjectionCode(ctx, otherOutputExprs, inputExprs)
        val code = generatePredicateCode(ctx, condition, child.output, inputExprs)
        val metricUpdateCode = generateUpdateMetricUpdateCode(ctx, sourcePresent)

        s"""
           |${code.code}
           |if (${code.value}) {
           |  $metricUpdateCode
           |  ${consume(ctx, projectionExpr)}
           |  ${consume(ctx, otherProjectionExpr)}
           |  return;
           |}
       """.stripMargin
      case _ =>
        // Codegen not implemented
        throw new SparkUnsupportedOperationException(
          errorClass = "_LEGACY_ERROR_TEMP_3073",
          messageParameters = Map("instruction" -> instruction.toString))
    }
  }

  /**
   * metric update code based on Keep's context
   */
  private def generateMetricUpdateCode(ctx: CodegenContext, context: Context,
                                       sourcePresent: Boolean): String = {
    context match {
      case Copy =>
        val copyMetric = metricTerm(ctx, "numTargetRowsCopied")
        s"$copyMetric.add(1);"

      case Insert =>
        val insertMetric = metricTerm(ctx, "numTargetRowsInserted")
        s"$insertMetric.add(1);"

      case Update =>
        generateUpdateMetricUpdateCode(ctx, sourcePresent)

      case Delete =>
        generateDeleteMetricUpdateCode(ctx, sourcePresent)

      case _ =>
        throw new IllegalArgumentException(s"Unexpected context for KeepExec: $context")
    }
  }

  private def generateUpdateMetricUpdateCode(ctx: CodegenContext,
                                             sourcePresent: Boolean): String = {
    val updateMetric = metricTerm(ctx, "numTargetRowsUpdated")
    if (sourcePresent) {
      val matchedUpdateMetric = metricTerm(ctx, "numTargetRowsMatchedUpdated")

      s"""
         |$updateMetric.add(1);
         |$matchedUpdateMetric.add(1);
       """.stripMargin
    } else {
      val notMatchedBySourceUpdateMetric = metricTerm(ctx, "numTargetRowsNotMatchedBySourceUpdated")

      s"""
         |$updateMetric.add(1);
         |$notMatchedBySourceUpdateMetric.add(1);
       """.stripMargin
    }
  }

  private def generateDeleteMetricUpdateCode(ctx: CodegenContext,
                                             sourcePresent: Boolean): String = {
    val deleteMetric = metricTerm(ctx, "numTargetRowsDeleted")
    if (sourcePresent) {
      val matchedDeleteMetric = metricTerm(ctx, "numTargetRowsMatchedDeleted")

      s"""
         |$deleteMetric.add(1);
         |$matchedDeleteMetric.add(1);
       """.stripMargin
    } else {
      val notMatchedBySourceDeleteMetric = metricTerm(ctx, "numTargetRowsNotMatchedBySourceDeleted")

      s"""
         |$deleteMetric.add(1);
         |$notMatchedBySourceDeleteMetric.add(1);
       """.stripMargin
    }
  }

  /**
   * Helper method to save and restore CodegenContext state for code generation.
   *
   * This is needed because when generating code for expressions, the CodegenContext
   * state (currentVars and INPUT_ROW) gets modified during expression evaluation.
   * This method temporarily sets the context to the input variables from doConsume
   * and restores the original state after the block completes.
   */
  private def withCodegenContext[T](
      ctx: CodegenContext,
      inputCurrentVars: Seq[ExprCode])(block: => T): T = {
    val originalCurrentVars = ctx.currentVars
    val originalInputRow = ctx.INPUT_ROW
    try {
      // Set to the input variables saved in doConsume
      ctx.currentVars = inputCurrentVars
      block
    } finally {
      // Restore original context
      ctx.currentVars = originalCurrentVars
      ctx.INPUT_ROW = originalInputRow
    }
  }

  private def generatePredicateCode(ctx: CodegenContext,
                                    predicate: Expression,
                                    inputAttrs: Seq[Attribute],
                                    inputCurrentVars: Seq[ExprCode]): ExprCode = {
    withCodegenContext(ctx, inputCurrentVars) {
      val boundPredicate = BindReferences.bindReference(predicate, inputAttrs)
      val ev = boundPredicate.genCode(ctx)
      val predicateVar = ctx.freshName("predicateResult")
      val code = code"""
                       |${ev.code}
                       |boolean $predicateVar = !${ev.isNull} && ${ev.value};
                        """.stripMargin
      ExprCode(code, FalseLiteral,
        JavaCode.variable(predicateVar, BooleanType))
    }
  }

  private def generateProjectionCode(ctx: CodegenContext,
                                     outputExprs: Seq[Expression],
                                     inputCurrentVars: Seq[ExprCode]): Seq[ExprCode] = {
    withCodegenContext(ctx, inputCurrentVars) {
      val boundExprs = outputExprs.map(BindReferences.bindReference(_, child.output))
      boundExprs.map(_.genCode(ctx))
    }
  }

  private def processPartition(rowIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
    val isSourceRowPresentPred = createPredicate(isSourceRowPresent)
    val isTargetRowPresentPred = createPredicate(isTargetRowPresent)

    val matchedInstructionExecs = planInstructions(matchedInstructions)
    val notMatchedInstructionExecs = planInstructions(notMatchedInstructions)
    val notMatchedBySourceInstructionExecs = planInstructions(notMatchedBySourceInstructions)

    val cardinalityValidator = if (checkCardinality) {
      val rowIdOrdinal = child.output.indexWhere(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdOrdinal != -1, "Cannot find row ID attr")
      BitmapCardinalityValidator(rowIdOrdinal)
    } else {
      NoopCardinalityValidator
    }

    val mergeIterator = new MergeRowIterator(
      rowIterator, cardinalityValidator, isTargetRowPresentPred, isSourceRowPresentPred,
      matchedInstructionExecs, notMatchedInstructionExecs, notMatchedBySourceInstructionExecs)

    // null indicates a record must be discarded
    mergeIterator.filter(_ != null)
  }

  private def createProjection(exprs: Seq[Expression]): UnsafeProjection = {
    UnsafeProjection.create(exprs, child.output)
  }

  private def createPredicate(expr: Expression): BasePredicate = {
    GeneratePredicate.generate(expr, child.output)
  }

  private def planInstructions(instructions: Seq[Instruction]): Seq[InstructionExec] = {
    instructions.map {
      case Keep(context, cond, output) =>
        KeepExec(context, createPredicate(cond), createProjection(output))

      case Discard(cond) =>
        DiscardExec(createPredicate(cond))

      case Split(cond, output, otherOutput) =>
        SplitExec(createPredicate(cond), createProjection(output), createProjection(otherOutput))

      case other =>
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3073",
          messageParameters = Map("other" -> other.toString))
    }
  }

  sealed trait InstructionExec {
    def condition: BasePredicate
  }

  case class KeepExec(
      context: Context,
      condition: BasePredicate,
      projection: Projection) extends InstructionExec {
    def apply(row: InternalRow): InternalRow = projection.apply(row)
  }

  case class DiscardExec(condition: BasePredicate) extends InstructionExec

  case class SplitExec(
      condition: BasePredicate,
      projection: Projection,
      otherProjection: Projection) extends InstructionExec {
    def projectRow(row: InternalRow): InternalRow = projection.apply(row)
    def projectExtraRow(row: InternalRow): InternalRow = otherProjection.apply(row)
  }

  sealed trait CardinalityValidator {
    def validate(row: InternalRow): Unit
  }

  object NoopCardinalityValidator extends CardinalityValidator {
    def validate(row: InternalRow): Unit = {}
  }

  /**
   * A simple cardinality validator that keeps track of seen row IDs in a roaring bitmap.
   * This validator assumes the target table is never broadcasted or replicated, which guarantees
   * matches for one target row are always co-located in the same partition.
   *
   * IDs are generated by [[org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID]].
   */
  case class BitmapCardinalityValidator(rowIdOrdinal: Int) extends CardinalityValidator {
    // use Roaring64Bitmap as row IDs generated by MonotonicallyIncreasingID are 64-bit integers
    private val matchedRowIds = new Roaring64Bitmap()

    override def validate(row: InternalRow): Unit = {
      val currentRowId = row.getLong(rowIdOrdinal)
      if (matchedRowIds.contains(currentRowId)) {
        throw QueryExecutionErrors.mergeCardinalityViolationError()
      }
      matchedRowIds.add(currentRowId)
    }
  }

  /**
   * An iterator that acts on joined target and source rows and computes deletes, updates and
   * inserts according to provided MERGE instructions.
   *
   * If a particular joined row should be discarded, this iterator returns null.
   */
  class MergeRowIterator(
      private val rowIterator: Iterator[InternalRow],
      private val cardinalityValidator: CardinalityValidator,
      private val isTargetRowPresentPred: BasePredicate,
      private val isSourceRowPresentPred: BasePredicate,
      private val matchedInstructions: Seq[InstructionExec],
      private val notMatchedInstructions: Seq[InstructionExec],
      private val notMatchedBySourceInstructions: Seq[InstructionExec])
    extends Iterator[InternalRow] {

    var cachedExtraRow: InternalRow = _

    override def hasNext: Boolean = cachedExtraRow != null || rowIterator.hasNext

    override def next(): InternalRow = {
      if (cachedExtraRow != null) {
        val extraRow = cachedExtraRow
        cachedExtraRow = null
        return extraRow
      }

      val row = rowIterator.next()

      val isSourceRowPresent = isSourceRowPresentPred.eval(row)
      val isTargetRowPresent = isTargetRowPresentPred.eval(row)

      if (isTargetRowPresent && isSourceRowPresent) {
        cardinalityValidator.validate(row)
        applyInstructions(row, matchedInstructions, sourcePresent = true)
      } else if (isSourceRowPresent) {
        applyInstructions(row, notMatchedInstructions, sourcePresent = true)
      } else if (isTargetRowPresent) {
        applyInstructions(row, notMatchedBySourceInstructions)
      } else {
        null
      }
    }

    private def applyInstructions(
        row: InternalRow,
        instructions: Seq[InstructionExec],
        sourcePresent: Boolean = false): InternalRow = {

      for (instruction <- instructions) {
        if (instruction.condition.eval(row)) {
          instruction match {
            case keep: KeepExec =>
              keep.context match {
                case Copy => incrementCopyMetric()
                case Update => incrementUpdateMetric(sourcePresent)
                case Insert => incrementInsertMetric()
                case Delete => incrementDeleteMetric(sourcePresent)
                case _ => throw new IllegalArgumentException(
                  s"Unexpected context for KeepExec: ${keep.context}")
              }
              return keep.apply(row)

            case _: DiscardExec =>
              incrementDeleteMetric(sourcePresent)
              return null

            case split: SplitExec =>
              incrementUpdateMetric(sourcePresent)
              cachedExtraRow = split.projectExtraRow(row)
              return split.projectRow(row)
          }
        }
      }

      null
    }
  }

  // For group based merge, copy is inserted if row matches no other case
  private def incrementCopyMetric(): Unit = longMetric("numTargetRowsCopied") += 1

  private def incrementInsertMetric(): Unit = longMetric("numTargetRowsInserted") += 1

  private def incrementDeleteMetric(sourcePresent: Boolean): Unit = {
    longMetric("numTargetRowsDeleted") += 1
    if (sourcePresent) {
      longMetric("numTargetRowsMatchedDeleted") += 1
    } else {
      longMetric("numTargetRowsNotMatchedBySourceDeleted") += 1
    }
  }

  private def incrementUpdateMetric(sourcePresent: Boolean): Unit = {
    longMetric("numTargetRowsUpdated") += 1
    if (sourcePresent) {
      longMetric("numTargetRowsMatchedUpdated") += 1
    } else {
      longMetric("numTargetRowsNotMatchedBySourceUpdated") += 1
    }
  }
}

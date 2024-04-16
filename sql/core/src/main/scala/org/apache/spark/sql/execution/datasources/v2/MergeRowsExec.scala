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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.BasePredicate
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Projection
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Discard, Instruction, Keep, ROW_ID, Split}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode

case class MergeRowsExec(
    isSourceRowPresent: Expression,
    isTargetRowPresent: Expression,
    matchedInstructions: Seq[Instruction],
    notMatchedInstructions: Seq[Instruction],
    notMatchedBySourceInstructions: Seq[Instruction],
    checkCardinality: Boolean,
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {

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
      case Keep(cond, output) =>
        KeepExec(createPredicate(cond), createProjection(output))

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

  case class KeepExec(condition: BasePredicate, projection: Projection) extends InstructionExec {
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
        applyInstructions(row, matchedInstructions)
      } else if (isSourceRowPresent) {
        applyInstructions(row, notMatchedInstructions)
      } else if (isTargetRowPresent) {
        applyInstructions(row, notMatchedBySourceInstructions)
      } else {
        null
      }
    }

    private def applyInstructions(
        row: InternalRow,
        instructions: Seq[InstructionExec]): InternalRow = {

      for (instruction <- instructions) {
        if (instruction.condition.eval(row)) {
          instruction match {
            case keep: KeepExec =>
              return keep.apply(row)

            case _: DiscardExec =>
              return null

            case split: SplitExec =>
              cachedExtraRow = split.projectExtraRow(row)
              return split.projectRow(row)
          }
        }
      }

      null
    }
  }
}

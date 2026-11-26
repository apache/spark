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

package org.apache.spark.sql.execution

import java.time.{ZoneId, ZoneOffset}

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSet, BindReferences, BoundReference, Cast, Expression, GenericInternalRow, JoinedRow, Literal, Multiply, NamedExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.plans.logical.BinBy
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, MathUtils, TimestampFormatter}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DoubleType

/**
 * Physical node for the `BIN BY` relation operator. For each input row it emits one output row per
 * bin overlapping `[rangeStart, rangeEnd)`, scaling the DISTRIBUTE UNIFORM columns by the overlap
 * fraction and appending `bin_start`, `bin_end`, `bin_distribute_ratio`.
 *
 * Bin boundaries lie on the grid `bin_start(k) =
 * [[DateTimeUtils.timeBucketFromIndexDTInterval]](width, k, origin, zone)`. Sub-day widths use UTC
 * microsecond arithmetic, multi-day widths use civil-time arithmetic in the session zone (UTC for
 * TIMESTAMP_NTZ). DISTRIBUTE UNIFORM columns are FLOAT or DOUBLE only and scale by multiplication.
 */
case class BinByExec(
    binWidthMicros: Long,
    originMicros: Long,
    rangeStart: Attribute,
    rangeEnd: Attribute,
    distributeColumns: Seq[Attribute],
    scaledDistributeColumns: Seq[Attribute],
    appendedAttributes: Seq[Attribute],
    timeZoneId: Option[String],
    child: SparkPlan)
  extends UnaryExecNode with CodegenSupport with PartitioningPreservingUnaryExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private val distributeReplacements: AttributeMap[Attribute] =
    AttributeMap(distributeColumns.zip(scaledDistributeColumns))

  private val numAppended: Int = appendedAttributes.length
  assert(numAppended == 3,
    s"BinBy appends exactly 3 columns (bin_start, bin_end, bin_distribute_ratio), got $numAppended")

  override def output: Seq[Attribute] =
    child.output.map(a => distributeReplacements.getOrElse(a, a)) ++ appendedAttributes

  override def producedAttributes: AttributeSet =
    AttributeSet(scaledDistributeColumns ++ appendedAttributes)

  override protected def stringArgs: Iterator[Any] = {
    BinBy.explainStringArgs(
      rangeStart = rangeStart,
      rangeEnd = rangeEnd,
      binWidthMicros = binWidthMicros,
      originMicros = originMicros,
      distributeColumns = distributeColumns,
      scaledDistributeColumns = scaledDistributeColumns,
      appendedAttributes = appendedAttributes,
      timeZoneId = timeZoneId)
  }

  // The trait keeps a child partitioning only when its keys are in the output: a partitioning keyed
  // on a pass-through column survives; one keyed on a scaled DISTRIBUTE column (fresh ExprId) does
  // not, because that column is absent from the output.
  override protected def outputExpressions: Seq[NamedExpression] = output

  override protected def withNewChildInternal(newChild: SparkPlan): BinByExec =
    copy(child = newChild)

  override def supportCodegen: Boolean = conf.getConf(SQLConf.ENABLE_BIN_BY_CODEGEN)

  // BIN BY starts its generated stage and reads the child rows through an iterator.
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.execute() :: Nil

  // The loop returns after each output row that reaches the stage output, so the row needs no copy.
  override def needCopyResult: Boolean = false

  // The bin loop reads the stage's input, so it stops at a LIMIT fused above it.
  override protected def canCheckLimitNotReached: Boolean = true

  /**
   * Generates the bin loop. The current input row and the state of its next bin are fields, so the
   * loop can return after any bin and continue with the next bin on the next call.
   */
  override protected def doProduce(ctx: CodegenContext): String = {
    val input = ctx.addMutableState(
      javaType = "scala.collection.Iterator",
      variableName = "input",
      initFunc = v => s"$v = inputs[0];",
      forceInline = true
    )
    val row = ctx.addMutableState("InternalRow", "binByRow", forceInline = true)
    val hasMoreBins =
      ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "hasMoreBins", forceInline = true)
    val nullRange = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "nullRange", forceInline = true)
    val start = ctx.addMutableState(CodeGenerator.JAVA_LONG, "rangeStart", forceInline = true)
    val end = ctx.addMutableState(CodeGenerator.JAVA_LONG, "rangeEnd", forceInline = true)
    val total = ctx.addMutableState(CodeGenerator.JAVA_LONG, "total", forceInline = true)
    val index = ctx.addMutableState(CodeGenerator.JAVA_LONG, "binIndex", forceInline = true)
    val nextStart = ctx.addMutableState(CodeGenerator.JAVA_LONG, "nextStart", forceInline = true)
    val binsSinceKillCheck =
      ctx.addMutableState(CodeGenerator.JAVA_INT, "binsSinceKillCheck", forceInline = true)
    val rsIdx = bindOrdinal(rangeStart)
    val reIdx = bindOrdinal(rangeEnd)
    val zone = timeZoneId.map(DateTimeUtils.getZoneId).getOrElse(ZoneOffset.UTC)
    val zoneRef = ctx.addReferenceObj(
      objName = "zoneId", obj = zone, className = classOf[ZoneId].getName)
    val formatter = ctx.addReferenceObj(
      objName = "formatter",
      obj = TimestampFormatter.getFractionFormatter(zone),
      className = classOf[TimestampFormatter].getName
    )
    val taskContext = ctx.addMutableState(
      javaType = "TaskContext",
      variableName = "taskContext",
      initFunc = v => s"$v = TaskContext.get();",
      forceInline = true
    )
    // The loop checks for a task kill every this many bins, across input rows.
    val killCheckInterval = 1024
    val dateTimeUtils = DateTimeUtils.getClass.getName.stripSuffix("$")
    val errors = QueryExecutionErrors.getClass.getName.stripSuffix("$")
    val bucket = ctx.freshName("bucket")
    val binStart = ctx.freshName("binStart")
    val binEnd = ctx.freshName("binEnd")
    val ratio = ctx.freshName("ratio")
    val numOutput = metricTerm(ctx, "numOutputRows")

    val appended = Seq(binStart, binEnd, ratio).zip(appendedAttributes).map {
      case (value, attr) =>
        ExprCode(JavaCode.isNullGlobal(nullRange), JavaCode.variable(value, attr.dataType))
    }
    ctx.INPUT_ROW = row
    ctx.currentVars = Seq.fill(child.output.length)(null) ++ appended
    val outputVars = buildOutputExpressions(childLen = child.output.length, ratioNullable = true)
      .map(_.genCode(ctx))

    // Set the next bin before `consume`, because the stop check after it can return.
    s"""
       |while ($limitNotReachedCond ($hasMoreBins || $input.hasNext())) {
       |  if (!$hasMoreBins) {
       |    $row = (InternalRow) $input.next();
       |    $nullRange = $row.isNullAt($rsIdx) || $row.isNullAt($reIdx);
       |    if (!$nullRange) {
       |      $start = $row.getLong($rsIdx);
       |      $end = $row.getLong($reIdx);
       |      if ($start > $end) {
       |        throw (RuntimeException) $errors.binByInvalidRangeError(
       |          $formatter.format($start), $formatter.format($end));
       |      }
       |      try {
       |        $total = java.lang.Math.subtractExact($end, $start);
       |        scala.Tuple2 $bucket = $dateTimeUtils.timeBucketFromTimestampDTInterval(
       |          ${binWidthMicros}L, $start, ${originMicros}L, $zoneRef);
       |        $index = $bucket._1$$mcJ$$sp();
       |        $nextStart = $bucket._2$$mcJ$$sp();
       |      } catch (java.lang.ArithmeticException e) {
       |        throw $errors.binByOverflowError();
       |      }
       |    }
       |  }
       |  if (++$binsSinceKillCheck == $killCheckInterval) {
       |    $binsSinceKillCheck = 0;
       |    $taskContext.killTaskIfInterrupted();
       |  }
       |  long $binStart = $nextStart;
       |  long $binEnd = 0L;
       |  double $ratio = 0.0D;
       |  if (!$nullRange) {
       |    try {
       |      $index = java.lang.Math.addExact($index, 1L);
       |      $binEnd = $dateTimeUtils.timeBucketFromIndexDTInterval(
       |        ${binWidthMicros}L, $index, ${originMicros}L, $zoneRef);
       |    } catch (java.lang.ArithmeticException e) {
       |      throw $errors.binByOverflowError();
       |    }
       |    $ratio = $total == 0L ? 1.0D :
       |      (double) (java.lang.Math.min($end, $binEnd) -
       |        java.lang.Math.max($start, $binStart)) / (double) $total;
       |    $nextStart = $binEnd;
       |  }
       |  $hasMoreBins = !$nullRange && $total != 0L && $nextStart < $end;
       |  $numOutput.add(1);
       |  ${consume(ctx, outputVars)}
       |  $shouldStopCheckCode
       |}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val width = binWidthMicros
    val origin = originMicros
    val zone = timeZoneId.map(DateTimeUtils.getZoneId).getOrElse(ZoneOffset.UTC)
    val numOutputRows = longMetric("numOutputRows")

    val rsIdx = bindOrdinal(rangeStart)
    val reIdx = bindOrdinal(rangeEnd)
    val childLen = child.output.length

    // Bound over JoinedRow(childRow, appendedRow); reused for every sub-row.
    val projExprs = buildOutputExpressions(childLen)
    // Bound over the child row alone, for the null-range path.
    val nullProjExprs = buildNullRangeExpressions()

    child.execute().mapPartitionsInternal { rows =>
      val proj = UnsafeProjection.create(projExprs)
      val nullProj = UnsafeProjection.create(nullProjExprs)
      val appended = new GenericInternalRow(numAppended)
      val joined = new JoinedRow
      val fmt = TimestampFormatter.getFractionFormatter(zone)

      def withBinByOverflow[T](f: => T): T = {
        try {
          f
        } catch {
          case _: ArithmeticException =>
            throw QueryExecutionErrors.binByOverflowError()
        }
      }

      rows.flatMap { row =>
        joined.withLeft(row)
        val rowIter: Iterator[InternalRow] =
          if (row.isNullAt(rsIdx) || row.isNullAt(reIdx)) {
            Iterator.single(nullProj(row))
          } else {
            val rs = row.getLong(rsIdx)
            val re = row.getLong(reIdx)
            if (rs > re) {
              throw QueryExecutionErrors.binByInvalidRangeError(fmt.format(rs), fmt.format(re))
            } else if (rs == re) {
              val (k, binStart) = withBinByOverflow {
                DateTimeUtils.timeBucketFromTimestampDTInterval(width, rs, origin, zone)
              }
              val binEnd = withBinByOverflow {
                val nextK = MathUtils.addExact(k, 1L)
                DateTimeUtils.timeBucketFromIndexDTInterval(width, nextK, origin, zone)
              }
              appended.update(0, binStart)
              appended.update(1, binEnd)
              appended.update(2, 1.0d)
              Iterator.single(proj(joined.withRight(appended)))
            } else {
              val total = withBinByOverflow(MathUtils.subtractExact(re, rs))
              new Iterator[InternalRow] {
                // Advance the bucket index and read each boundary off the grid, rather than
                // walking one bin end to the next start.
                private var (k, curStart) = withBinByOverflow {
                  DateTimeUtils.timeBucketFromTimestampDTInterval(width, rs, origin, zone)
                }

                override def hasNext: Boolean = curStart < re

                override def next(): InternalRow = {
                  val (nextK, curEnd) = withBinByOverflow {
                    val index = MathUtils.addExact(k, 1L)
                    val end =
                      DateTimeUtils.timeBucketFromIndexDTInterval(width, index, origin, zone)
                    (index, end)
                  }
                  val overlap = math.min(re, curEnd) - math.max(rs, curStart)
                  appended.update(0, curStart)
                  appended.update(1, curEnd)
                  appended.update(2, overlap.toDouble / total.toDouble)
                  k = nextK
                  curStart = curEnd
                  proj(joined.withRight(appended))
                }
              }
            }
          }
        rowIter.map { r =>
          numOutputRows += 1
          r
        }
      }
    }
  }

  private def bindOrdinal(a: Attribute): Int =
    (BindReferences.bindReference(a, child.output): Expression) match {
      case b: BoundReference => b.ordinal
      case _ =>
        throw SparkException.internalError(
          s"BinByExec attribute ${a.name}#${a.exprId.id} is not bound in child output " +
            s"${child.output.mkString("[", ", ", "]")}")
    }

  /**
   * Output projection over `JoinedRow(childRow, appendedRow)`: DISTRIBUTE columns scaled by the
   * ratio, other columns passed through. Codegen sets `ratioNullable`, because a NULL range there
   * goes through this projection with a NULL ratio.
   */
  private def buildOutputExpressions(
      childLen: Int,
      ratioNullable: Boolean = false): Seq[Expression] = {
    // bin_distribute_ratio is the last appended column.
    val ratioRef = BoundReference(childLen + numAppended - 1, DoubleType, ratioNullable)

    val forwarded = child.output.zipWithIndex.map { case (attr, ordinal) =>
      val ref = BoundReference(ordinal, attr.dataType, attr.nullable)
      // Match by attribute, as `output` does, so every copy of a DISTRIBUTE column is scaled.
      if (distributeReplacements.contains(attr)) {
        // Cast to double, multiply, narrow back. These expressions bind at execution time, so the
        // analyzer's operand coercion never runs; a bare Multiply(FloatType, DoubleType) fails.
        Cast(Multiply(Cast(ref, DoubleType), ratioRef), attr.dataType)
      } else {
        ref
      }
    }

    val appendedRefs = appendedAttributes.zipWithIndex.map { case (attr, i) =>
      BoundReference(childLen + i, attr.dataType, attr.nullable)
    }

    forwarded ++ appendedRefs
  }

  /**
   * Null-range projection over the child row: DISTRIBUTE and appended columns are NULL (no valid
   * bin), other columns passed through.
   */
  private def buildNullRangeExpressions(): Seq[Expression] = {
    val forwarded = child.output.zipWithIndex.map { case (attr, ordinal) =>
      if (distributeReplacements.contains(attr)) {
        Literal.create(null, attr.dataType)
      } else {
        BoundReference(ordinal, attr.dataType, attr.nullable)
      }
    }
    val nullAppended = appendedAttributes.map { attr =>
      Literal.create(null, attr.dataType)
    }
    forwarded ++ nullAppended
  }
}

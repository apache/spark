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

package org.apache.spark.sql.execution.joins

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.{AsOfJoinDirection, Backward, Forward, Inner, LeftOuter,
  Nearest}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class SortMergeAsOfJoinScannerSuite extends SparkFunSuite with LocalSparkContext {

  test("nearest scan stops at the first eligible row after the left key") {
    sc = new SparkContext("local", "SortMergeAsOfJoinScannerSuite", new SparkConf(false))
    val (matchedKeys, evaluations) = sc.parallelize(Seq(0), 1)
      .mapPartitions { _ =>
        val leftKey = AttributeReference("leftKey", IntegerType)()
        val rightKey = AttributeReference("rightKey", IntegerType)()
        val count = new AtomicInteger()
        val scanner = new SortMergeAsOfJoinScanner(
          leftIter = Iterator(InternalRow(98)),
          rightIter = (0 to 200 by 10).iterator.map(i => InternalRow(i)),
          leftOutput = Seq(leftKey),
          rightOutput = Seq(rightKey),
          leftKeys = Nil,
          rightKeys = Nil,
          leftSortExprs = Seq(leftKey),
          rightSortExprs = Seq(rightKey),
          asOfCondition = CountedEvaluation(Literal.TrueLiteral, count),
          joinType = Inner,
          residualCondition = None,
          numOutputRows = new SQLMetric("sum"),
          spillSize = new SQLMetric("sum"),
          direction = Nearest,
          inMemoryThreshold = 256,
          sizeInBytesSpillThreshold = Long.MaxValue,
          spillThreshold = 256,
          useCursorScan = false,
          numAsOfCursorTasks = new SQLMetric("sum"))
        try {
          Iterator.single((scanner.iterator.map(_.getInt(1)).toList, count.get()))
        } finally {
          scanner.close()
        }
      }.collect().head

    // The scan reads the right rows 0 to 100 and stops at 100, the first row after 98.
    assert(matchedKeys == List(100))
    assert(evaluations == 11, s"evaluated the as-of condition $evaluations times")
  }

  test("cursor scan evaluates the as-of condition twice per left row") {
    val leftKey = AttributeReference("leftKey", IntegerType)()
    val rightKey = AttributeReference("rightKey", IntegerType)()
    val leftValues = 5 to 95 by 10
    val rightValues = 0 to 100 by 10
    Seq(
      (Backward, GreaterThanOrEqual(leftKey, rightKey), (0 to 90 by 10).toList),
      (Forward, LessThanOrEqual(leftKey, rightKey), (10 to 100 by 10).toList)
    ).foreach { case (direction, condition, expected) =>
      withClue(s"$direction: ") {
        val (bufferedKeys, _, bufferedTasks) = runScan(
          leftValues, rightValues, leftKey, rightKey, condition, direction, useCursorScan = false)
        assert(bufferedKeys == expected)
        assert(bufferedTasks == 0)

        // Each left row passes one new right row and fails one, so 10 left rows need 20
        // evaluations. A scan that restarts at the first right row needs 65.
        val (cursorKeys, evaluations, cursorTasks) = runScan(
          leftValues, rightValues, leftKey, rightKey, condition, direction, useCursorScan = true)
        assert(cursorKeys == expected)
        assert(evaluations == 20, s"evaluated the as-of condition $evaluations times")
        assert(cursorTasks == 1)
      }
    }
  }

  test("cursor scan keeps its best right row while it reads the next right rows") {
    // Tags grow and shrink, so the reused best-row buffer has to resize and drop older bytes.
    val longTag = "a tag that is longer than the tags around it"
    val rightRows = Seq(10 -> "a", 20 -> longTag, 30 -> "bb", 40 -> null, 50 -> "c" * 30)
    val leftValues = Seq(5, 15, 15, 25, 35, 45, 55)
    Seq(
      // Backward keeps its best row while it reads the first failing row after it.
      (Backward, Seq(None, Some(10 -> "a"), Some(10 -> "a"), Some(20 -> longTag),
        Some(30 -> "bb"), Some(40 -> null), Some(50 -> "c" * 30))),
      // Forward reads past its best row, then reuses it for the second left row 15.
      (Forward, Seq(Some(10 -> "a"), Some(20 -> longTag), Some(20 -> longTag), Some(30 -> "bb"),
        Some(40 -> null), Some(50 -> "c" * 30), None))
    ).foreach { case (direction, expected) =>
      Seq(false, true).foreach { useCursorScan =>
        withClue(s"$direction, cursor scan $useCursorScan: ") {
          assert(runTagScan(leftValues, rightRows, direction, useCursorScan) == expected)
        }
      }
    }
  }

  test("cursor scan accepts each comparison of the sort keys in the join direction") {
    val l = AttributeReference("l", IntegerType)()
    val r = AttributeReference("r", IntegerType)()
    Seq(
      Backward -> GreaterThanOrEqual(l, r),
      Backward -> GreaterThan(l, r),
      Backward -> LessThanOrEqual(r, l),
      Backward -> LessThan(r, l),
      Forward -> LessThanOrEqual(l, r),
      Forward -> LessThan(l, r),
      Forward -> GreaterThanOrEqual(r, l),
      Forward -> GreaterThan(r, l)
    ).foreach { case (direction, condition) =>
      assert(supportsCursorScan(direction, condition, None, Seq(l), Seq(r)),
        s"$direction $condition")
    }

    // An operand may be any expression, as long as the side sorts on that same expression.
    val longL = Cast(l, LongType)
    val longR = AttributeReference("r", LongType)()
    assert(supportsCursorScan(
      Backward, GreaterThanOrEqual(longL, longR), None, Seq(longL), Seq(longR)))
  }

  test("cursor scan accepts scalar sort keys except TIME and nanosecond timestamps") {
    def supportsType(dataType: DataType): Boolean = {
      val l = AttributeReference("l", dataType)()
      val r = AttributeReference("r", dataType)()
      supportsCursorScan(Backward, GreaterThanOrEqual(l, r), None, Seq(l), Seq(r))
    }
    Seq(
      BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      DecimalType(38, 18), StringType, StringType("UTF8_LCASE"), BinaryType, NullType, DateType,
      TimestampType, TimestampNTZType, YearMonthIntervalType(), DayTimeIntervalType()
    ).foreach(dataType => assert(supportsType(dataType), dataType))
    Seq(
      StructType(Seq(StructField("a", IntegerType))), ArrayType(IntegerType), TimeType(),
      TimestampLTZNanosType(9), TimestampNTZNanosType(9)
    ).foreach(dataType => assert(!supportsType(dataType), dataType))
  }

  test("cursor scan rejects conditions that need the buffered scan") {
    val l = AttributeReference("l", IntegerType)()
    val r = AttributeReference("r", IntegerType)()
    val l2 = AttributeReference("l2", IntegerType)()
    val r2 = AttributeReference("r2", IntegerType)()
    val longR = AttributeReference("r", LongType)()
    val lcaseL = AttributeReference("l", StringType("UTF8_LCASE"))()
    val stringR = AttributeReference("r", StringType)()
    val backward = GreaterThanOrEqual(l, r)
    Seq(
      "a residual condition" ->
        supportsCursorScan(Backward, backward, Some(LessThan(l2, r2)), Seq(l), Seq(r)),
      "a nearest join" ->
        supportsCursorScan(Nearest, backward, None, Seq(l), Seq(r)),
      "a backward join with a forward comparison" ->
        supportsCursorScan(Backward, LessThanOrEqual(l, r), None, Seq(l), Seq(r)),
      "a forward join with a backward comparison" ->
        supportsCursorScan(Forward, GreaterThanOrEqual(l, r), None, Seq(l), Seq(r)),
      "swapped as-of columns" ->
        supportsCursorScan(Backward, GreaterThanOrEqual(r, l), None, Seq(l), Seq(r)),
      "a tolerance" ->
        supportsCursorScan(Backward, And(backward, GreaterThanOrEqual(r, Subtract(l, Literal(1)))),
          None, Seq(l), Seq(r)),
      "a constant operand" ->
        supportsCursorScan(Backward, GreaterThanOrEqual(l, Literal(1)), None, Seq(l), Seq(r)),
      // The left side sorts on l, but -l falls as l grows, so a cursor would walk the wrong way.
      "an operand that its side does not sort on" ->
        supportsCursorScan(Backward, GreaterThanOrEqual(UnaryMinus(l), r), None, Seq(l), Seq(r)),
      "two sort keys per side" ->
        supportsCursorScan(Backward, backward, None, Seq(l, l2), Seq(r, r2)),
      "sort keys of different types" ->
        supportsCursorScan(Backward, GreaterThanOrEqual(l, longR), None, Seq(l), Seq(longR)),
      "sort keys of different collations" ->
        supportsCursorScan(Backward, GreaterThanOrEqual(lcaseL, stringR), None,
          Seq(lcaseL), Seq(stringR))
    ).foreach { case (name, supported) => assert(!supported, name) }
  }

  private def supportsCursorScan(
      direction: AsOfJoinDirection,
      asOfCondition: Expression,
      residualCondition: Option[Expression],
      leftSortExprs: Seq[Expression],
      rightSortExprs: Seq[Expression]): Boolean =
    SortMergeAsOfJoinScanner.supportsCursorScan(
      direction, asOfCondition, residualCondition, leftSortExprs, rightSortExprs)

  /**
   * Runs one scan with no equi-keys in a task. Returns the right key that each left row matched,
   * how many times the scan evaluated the as-of condition, and the cursor scan task count.
   */
  private def runScan(
      leftValues: Seq[Int],
      rightValues: Seq[Int],
      leftKey: AttributeReference,
      rightKey: AttributeReference,
      asOfCondition: Expression,
      direction: AsOfJoinDirection,
      useCursorScan: Boolean): (List[Int], Int, Long) = {
    if (sc == null) {
      sc = new SparkContext("local", "SortMergeAsOfJoinScannerSuite", new SparkConf(false))
    }
    sc.parallelize(Seq(0), 1)
      .mapPartitions { _ =>
        val count = new AtomicInteger()
        val numAsOfCursorTasks = new SQLMetric("sum")
        val scanner = new SortMergeAsOfJoinScanner(
          leftIter = leftValues.iterator.map(value => InternalRow(value)),
          rightIter = rightValues.iterator.map(value => InternalRow(value)),
          leftOutput = Seq(leftKey),
          rightOutput = Seq(rightKey),
          leftKeys = Nil,
          rightKeys = Nil,
          leftSortExprs = Seq(leftKey),
          rightSortExprs = Seq(rightKey),
          asOfCondition = CountedEvaluation(asOfCondition, count),
          joinType = Inner,
          residualCondition = None,
          numOutputRows = new SQLMetric("sum"),
          spillSize = new SQLMetric("sum"),
          direction = direction,
          inMemoryThreshold = 256,
          sizeInBytesSpillThreshold = Long.MaxValue,
          spillThreshold = 256,
          useCursorScan = useCursorScan,
          numAsOfCursorTasks = numAsOfCursorTasks)
        try {
          val matchedKeys = scanner.iterator.map(_.getInt(1)).toList
          Iterator.single((matchedKeys, count.get(), numAsOfCursorTasks.value))
        } finally {
          scanner.close()
        }
      }.collect().head
  }

  /**
   * Runs a LEFT OUTER scan of INT left keys against right rows of (INT key, STRING tag), and
   * returns the matched right row of each left row. The right input reuses one row object, as
   * many operators do, so the scan has to copy each row it keeps.
   */
  private def runTagScan(
      leftValues: Seq[Int],
      rightRows: Seq[(Int, String)],
      direction: AsOfJoinDirection,
      useCursorScan: Boolean): Seq[Option[(Int, String)]] = {
    if (sc == null) {
      sc = new SparkContext("local", "SortMergeAsOfJoinScannerSuite", new SparkConf(false))
    }
    sc.parallelize(Seq(0), 1)
      .mapPartitions { _ =>
        val leftKey = AttributeReference("leftKey", IntegerType)()
        val rightKey = AttributeReference("rightKey", IntegerType)()
        val tag = AttributeReference("tag", StringType)()
        val reusedRow = new GenericInternalRow(2)
        val scanner = new SortMergeAsOfJoinScanner(
          leftIter = leftValues.iterator.map(value => InternalRow(value)),
          rightIter = rightRows.iterator.map { case (key, value) =>
            reusedRow.update(0, key)
            reusedRow.update(1, UTF8String.fromString(value))
            reusedRow
          },
          leftOutput = Seq(leftKey),
          rightOutput = Seq(rightKey, tag),
          leftKeys = Nil,
          rightKeys = Nil,
          leftSortExprs = Seq(leftKey),
          rightSortExprs = Seq(rightKey),
          asOfCondition = direction match {
            case Backward => GreaterThanOrEqual(leftKey, rightKey)
            case _ => LessThanOrEqual(leftKey, rightKey)
          },
          joinType = LeftOuter,
          residualCondition = None,
          numOutputRows = new SQLMetric("sum"),
          spillSize = new SQLMetric("sum"),
          direction = direction,
          inMemoryThreshold = 256,
          sizeInBytesSpillThreshold = Long.MaxValue,
          spillThreshold = 256,
          useCursorScan = useCursorScan,
          numAsOfCursorTasks = new SQLMetric("sum"))
        try {
          val matches = scanner.iterator.map { row =>
            val tagValue = if (row.isNullAt(2)) null else row.getString(2)
            if (row.isNullAt(1)) None else Some(row.getInt(1) -> tagValue)
          }.toList
          Iterator.single(matches)
        } finally {
          scanner.close()
        }
      }.collect().head
  }
}

private[joins] case class CountedEvaluation(child: Expression, counter: AtomicInteger)
    extends UnaryExpression with CodegenFallback {
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = child.dataType
  override lazy val resolved: Boolean = child.resolved

  override def eval(input: InternalRow): Any = {
    counter.incrementAndGet()
    child.eval(input)
  }

  override protected def withNewChildInternal(newChild: Expression): CountedEvaluation =
    copy(child = newChild)
}

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

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicInteger

import com.sun.management.UnixOperatingSystemMXBean

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException,
  SparkFunSuite}
import org.apache.spark.internal.config.UNSAFE_SORTER_SPILL_READ_AHEAD_ENABLED
import org.apache.spark.sql.YearUDT
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.plans.{AsOfJoinDirection, Backward, Forward, Inner, LeftOuter,
  Nearest}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class SortMergeAsOfJoinScannerSuite extends SparkFunSuite with LocalSparkContext {
  import SortMergeAsOfJoinScannerSuite._

  private val factoryModes =
    Seq(CodegenObjectFactoryMode.CODEGEN_ONLY, CodegenObjectFactoryMode.NO_CODEGEN)

  test("only backward and forward joins have a scan strategy") {
    assert(AsOfScanStrategy.of(Backward).contains(AsOfScanStrategy.LastMatch))
    assert(AsOfScanStrategy.of(Forward).contains(AsOfScanStrategy.FirstMatch))
    assert(AsOfScanStrategy.of(Nearest).isEmpty)
    assert(AsOfScanStrategy(Backward) == AsOfScanStrategy.LastMatch)
    assert(AsOfScanStrategy(Forward) == AsOfScanStrategy.FirstMatch)
    checkError(
      exception = intercept[SparkException](AsOfScanStrategy(Nearest)),
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "The Nearest AS-OF join has no ordered scan"))
  }

  test("nearest join has no cursor scan") {
    val leftKey = AttributeReference("leftKey", IntegerType)()
    val rightKey = AttributeReference("rightKey", IntegerType)()
    // A scanner built by hand must fail, not run the cursor of another direction. It fails when
    // it is built, so also with no rows, where the cursor would not check any right row.
    Seq((Seq(5), Seq(1, 9)), (Seq(5), Seq.empty[Int]), (Seq.empty[Int], Seq.empty[Int])).foreach {
      case (leftValues, rightValues) =>
        val error = intercept[SparkException] {
          runScan(leftValues, rightValues, leftKey, rightKey, Literal.TrueLiteral, Nearest,
            useCursorScan = true)
        }
        checkError(
          exception = error.getCause.asInstanceOf[SparkException],
          condition = "INTERNAL_ERROR",
          parameters = Map("message" -> "The Nearest AS-OF join has no cursor scan"))
    }
  }

  test("nearest scan stops at the first eligible row after the left key") {
    sc = new SparkContext("local", "SortMergeAsOfJoinScannerSuite", new SparkConf(false))
    val (matchedKeys, evaluations) = sc.parallelize(Seq(0), 1)
      .mapPartitions { _ =>
        val leftKey = AttributeReference("leftKey", IntegerType)()
        val rightKey = AttributeReference("rightKey", IntegerType)()
        val count = new AtomicInteger()
        val scanner = newScanner(leftKey, rightKey, Seq(98), 0 to 200 by 10,
          asOfCondition = CountedEvaluation(Literal.TrueLiteral, count), direction = Nearest)
        Iterator.single((matchedRightKeys(scanner), count.get()))
      }.collect().head

    // The scan reads the right rows 0 to 100 and stops at 100, the first row after 98.
    assert(matchedKeys == List(100))
    assert(evaluations == 11, s"evaluated the as-of condition $evaluations times")
  }

  gridTest("residual condition skips a repeated part behind a FALSE guard")(factoryModes) { mode =>
    sc = new SparkContext("local", "SortMergeAsOfJoinScannerSuite", new SparkConf(false))
    val (matchedKeys, evaluations) = sc.parallelize(Seq(0), 1)
      .mapPartitions { _ =>
        val leftKey = AttributeReference("leftKey", IntegerType)()
        val rightKey = AttributeReference("rightKey", IntegerType)()
        val count = new AtomicInteger()
        val residual = falseGuardBeforeRepeatedPart(leftKey, CountedEvaluation(rightKey, count))
        withFactoryMode(mode) {
          val scanner = newScanner(leftKey, rightKey, Seq(10), 0 until 10,
            asOfCondition = GreaterThanOrEqual(leftKey, rightKey),
            residualCondition = Some(residual))
          Iterator.single((matchedRightKeys(scanner), count.get()))
        }
      }.collect().head

    // All 10 right rows pass the as-of condition and then fail the guard.
    assert(matchedKeys.isEmpty)
    assert(evaluations == 0, s"evaluated the repeated part $evaluations times")
  }

  gridTest("as-of condition skips a repeated part behind a FALSE guard")(factoryModes) { mode =>
    sc = new SparkContext("local", "SortMergeAsOfJoinScannerSuite", new SparkConf(false))
    val (matchedKeys, evaluations) = sc.parallelize(Seq(0), 1)
      .mapPartitions { _ =>
        val leftKey = AttributeReference("leftKey", IntegerType)()
        val rightKey = AttributeReference("rightKey", IntegerType)()
        val count = new AtomicInteger()
        val guard = falseGuardBeforeRepeatedPart(leftKey, CountedEvaluation(rightKey, count))
        withFactoryMode(mode) {
          val scanner = newScanner(leftKey, rightKey, Seq(10), 0 until 10,
            asOfCondition = GreaterThanOrEqual(If(guard, Literal(-1), leftKey), rightKey))
          Iterator.single((matchedRightKeys(scanner), count.get()))
        }
      }.collect().head

    // The guard is FALSE, so the IF compares the left key 10, and the closest right row is 9.
    assert(matchedKeys == List(9))
    assert(evaluations == 0, s"evaluated the repeated part $evaluations times")
  }

  test("buffered scan closes the spill readers of a group it leaves early") {
    assume(openFileCount().isDefined, "needs a JVM that counts its open files")
    // Without read-ahead, each spill reader holds one open file and no extra thread.
    sc = new SparkContext("local", "SortMergeAsOfJoinScannerSuite",
      new SparkConf(false).set(UNSAFE_SORTER_SPILL_READ_AHEAD_ENABLED, false))
    // One task per direction, since a task keeps each spill reader's buffer until it ends.
    Seq(Backward, Forward, Nearest).foreach { direction =>
      withClue(s"$direction: ") {
        val (matchedKeys, openedFiles) = sc.parallelize(Seq(0), 1)
          .mapPartitions(_ => Iterator.single(spilledScan(direction)))
          .collect().head
        // Each left key k matches the right key k.
        assert(matchedKeys == spilledScanLeftKeys.toList)
        // Every left row leaves the scan early, with the later spill files of the group still
        // unread. Readers left open would keep those files open until the task ended.
        assert(openedFiles < spilledScanLeftKeys.size / 2,
          s"the scan left $openedFiles more files open")
      }
    }
  }

  test("conditions are built only when a scan checks a right row") {
    sc = new SparkContext("local", "SortMergeAsOfJoinScannerSuite", new SparkConf(false))
    val compiled = sc.parallelize(Seq(0), 1)
      .mapPartitions { _ =>
        val leftKey = AttributeReference("leftKey", IntegerType)()
        val rightKey = AttributeReference("rightKey", IntegerType)()
        val failing = GreaterThanOrEqual(FailsToCompile(leftKey), rightKey)
        // Whether the scan compiled `failing`, as its as-of or as its residual condition.
        def compiles(left: Seq[Int], right: Seq[Int], asOf: Expression,
            residual: Option[Expression]): Boolean = {
          try {
            matchedRightKeys(newScanner(leftKey, rightKey, left, right, asOf, residual))
            false
          } catch {
            case _: FailsToCompileException => true
          }
        }
        val sides = Seq((Seq.empty[Int], Seq(1)), (Seq(1), Seq.empty[Int]), (Seq(1), Seq(1)))
        withFactoryMode(CodegenObjectFactoryMode.CODEGEN_ONLY) {
          Iterator.single(sides.map { case (left, right) =>
            (compiles(left, right, failing, None),
              compiles(left, right, Literal.TrueLiteral, Some(failing)))
          })
        }
      }.collect().head

    // An empty left or right side checks no right row, so neither condition is compiled.
    assert(compiled == Seq((false, false), (false, false), (true, true)))
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

  test("cursor scan accepts each comparison of the sort keys in the join direction, left " +
      "side first") {
    val l = AttributeReference("l", IntegerType)()
    val r = AttributeReference("r", IntegerType)()
    Seq(
      Backward -> GreaterThanOrEqual(l, r),
      Backward -> GreaterThan(l, r),
      Forward -> LessThanOrEqual(l, r),
      Forward -> LessThan(l, r)
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

  test("cursor scan accepts scalar sort keys except TIME, nanosecond timestamps and " +
      "user-defined types") {
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
      TimestampLTZNanosType(9), TimestampNTZNanosType(9), new YearUDT
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
      // A NULL right key would end these before the left key, which the cursor evaluates first.
      "a backward condition with the right side first" ->
        supportsCursorScan(Backward, LessThanOrEqual(r, l), None, Seq(l), Seq(r)),
      "a strict backward condition with the right side first" ->
        supportsCursorScan(Backward, LessThan(r, l), None, Seq(l), Seq(r)),
      "a forward condition with the right side first" ->
        supportsCursorScan(Forward, GreaterThanOrEqual(r, l), None, Seq(l), Seq(r)),
      "a strict forward condition with the right side first" ->
        supportsCursorScan(Forward, GreaterThan(r, l), None, Seq(l), Seq(r)),
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

private object SortMergeAsOfJoinScannerSuite {

  /** A buffered scanner over one INT column per side, with no equi-keys. */
  def newScanner(
      leftKey: Attribute,
      rightKey: Attribute,
      leftKeys: Seq[Int],
      rightKeys: Seq[Int],
      asOfCondition: Expression,
      residualCondition: Option[Expression] = None,
      direction: AsOfJoinDirection = Backward,
      inMemoryThreshold: Int = 256,
      spillThreshold: Int = 256): SortMergeAsOfJoinScanner = {
    new SortMergeAsOfJoinScanner(
      leftIter = leftKeys.iterator.map(k => InternalRow(k)),
      rightIter = rightKeys.iterator.map(k => InternalRow(k)),
      leftOutput = Seq(leftKey),
      rightOutput = Seq(rightKey),
      leftKeys = Nil,
      rightKeys = Nil,
      leftSortExprs = Seq(leftKey),
      rightSortExprs = Seq(rightKey),
      asOfCondition = asOfCondition,
      joinType = Inner,
      residualCondition = residualCondition,
      numOutputRows = new SQLMetric("sum"),
      spillSize = new SQLMetric("sum"),
      direction = direction,
      inMemoryThreshold = inMemoryThreshold,
      sizeInBytesSpillThreshold = Long.MaxValue,
      spillThreshold = spillThreshold,
      useCursorScan = false,
      numAsOfCursorTasks = new SQLMetric("sum"))
  }

  /** Left keys of `spilledScan`: each one is near the start of the right keys. */
  val spilledScanLeftKeys: Seq[Int] = 1 to 16

  /** The number of files this JVM has open, if it can tell. */
  def openFileCount(): Option[Long] = ManagementFactory.getOperatingSystemMXBean match {
    case unix: UnixOperatingSystemMXBean => Some(unix.getOpenFileDescriptorCount)
    case _ => None
  }

  /**
   * Runs a buffered scan in `direction` over a right group of 50 rows that spills every 10 rows.
   * Returns the right key of each output row, and how many more files the JVM has open after the
   * scan than before it, while the task still runs.
   */
  def spilledScan(direction: AsOfJoinDirection): (List[Int], Long) = {
    val leftKey = AttributeReference("leftKey", IntegerType)()
    val rightKey = AttributeReference("rightKey", IntegerType)()
    val asOfCondition = direction match {
      case Backward => GreaterThanOrEqual(leftKey, rightKey)
      case Forward => LessThanOrEqual(leftKey, rightKey)
      case _ => Literal.TrueLiteral
    }
    val scanner = newScanner(leftKey, rightKey, spilledScanLeftKeys, 0 until 50, asOfCondition,
      direction = direction, inMemoryThreshold = 1, spillThreshold = 10)
    val before = openFileCount().get
    try {
      val matchedKeys = scanner.iterator.map(_.getInt(1)).toList
      (matchedKeys, openFileCount().get - before)
    } finally {
      scanner.close()
    }
  }

  /** Runs the scan to the end and returns the right key of each output row. */
  def matchedRightKeys(scanner: SortMergeAsOfJoinScanner): List[Int] = {
    try {
      scanner.iterator.map(_.getInt(1)).toList
    } finally {
      scanner.close()
    }
  }

  /** `leftKey < 0 AND part > 0 AND part < 100`: the guard is FALSE for the left keys used here. */
  def falseGuardBeforeRepeatedPart(leftKey: Expression, part: Expression): Expression =
    And(And(LessThan(leftKey, Literal(0)), GreaterThan(part, Literal(0))),
      LessThan(part, Literal(100)))

  /** Runs `f` with subexpression elimination on and the given codegen factory mode. */
  def withFactoryMode[T](mode: CodegenObjectFactoryMode.Value)(f: => T): T = {
    val conf = new SQLConf
    conf.setConfString(SQLConf.CODEGEN_FACTORY_MODE.key, mode.toString)
    conf.setConfString(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key, "true")
    SQLConf.withExistingConf(conf)(f)
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

private[joins] class FailsToCompileException extends RuntimeException("compiled")

/** Evaluates its child, but throws when compiled, so a test can see when a condition is built. */
private[joins] case class FailsToCompile(child: Expression) extends UnaryExpression {
  override def dataType: DataType = child.dataType
  override protected def nullSafeEval(input: Any): Any = input

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new FailsToCompileException

  override protected def withNewChildInternal(newChild: Expression): FailsToCompile =
    copy(child = newChild)
}

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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.plans.{AsOfJoinDirection, Backward, Inner, Nearest}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType}

class SortMergeAsOfJoinScannerSuite extends SparkFunSuite with LocalSparkContext {
  import SortMergeAsOfJoinScannerSuite._

  private val factoryModes =
    Seq(CodegenObjectFactoryMode.CODEGEN_ONLY, CodegenObjectFactoryMode.NO_CODEGEN)

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
}

private object SortMergeAsOfJoinScannerSuite {

  /** A scanner over one INT column per side, with no equi-keys. */
  def newScanner(
      leftKey: Attribute,
      rightKey: Attribute,
      leftKeys: Seq[Int],
      rightKeys: Seq[Int],
      asOfCondition: Expression,
      residualCondition: Option[Expression] = None,
      direction: AsOfJoinDirection = Backward): SortMergeAsOfJoinScanner = {
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
      inMemoryThreshold = 256,
      sizeInBytesSpillThreshold = Long.MaxValue,
      spillThreshold = 256)
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

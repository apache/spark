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

package org.apache.spark.sql.execution.window

import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite, TaskContext}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, MutableProjection, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{DeclarativeAggregate, Max, Min}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * Property-based tests for [[WindowSegmentTree]] using ScalaCheck.
 *
 * Uses the row-by-row naive aggregate as parallel oracle; every property
 * ultimately reduces to `segtree(input) == naive(input)`.
 *
 * Follows the catalyst convention established by `ExpressionEvalHelper`:
 * mixes in [[ScalaCheckDrivenPropertyChecks]] from `scalatestplus.scalacheck`.
 *
 * Runtime note: tests wrap `forAll` inside a single `withTaskContext`, so all
 * generated cases share one `SparkContext` / `TaskMemoryManager`. Do NOT
 * invert: wrapping `withTaskContext` inside `forAll` would cold-start a
 * SparkContext per case.
 */
class WindowSegmentTreePropertySuite extends SparkFunSuite
    with LocalSparkContext with ScalaCheckDrivenPropertyChecks {

  // ---- ScalaCheck config: fixed seed for CI reproducibility ----

  // Quick tier: 100 cases for P1, 10 cases for P2 (see pbt-design.md §6).
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100, minSize = 0, sizeRange = 100)

  // ---- test harness (mirrors WindowSegmentTreeSuite) ----

  private def newMutableProjection
      : (Seq[Expression], Seq[Attribute]) => MutableProjection =
    (exprs, attrs) => GenerateMutableProjection.generate(exprs, attrs)

  private def withTaskContext[T](body: => T): T = {
    sc = new SparkContext("local", "property-based-test", new SparkConf(false))
    try {
      val taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
      TaskContext.setTaskContext(taskContext)
      try body finally TaskContext.unset()
    } finally {
      if (sc != null) {
        sc.stop()
        sc = null
      }
    }
  }

  // ---- Oracle ----

  sealed trait AggKind
  case object AggMin extends AggKind
  case object AggMax extends AggKind

  /**
   * Naive oracle over [lo, hi). Empty frame =&gt; None. null inputs are skipped
   * (Spark Min/Max semantics).
   */
  private def naiveAgg[T](
      values: IndexedSeq[Option[T]],
      lo: Int, hi: Int,
      kind: AggKind)(implicit ord: Ordering[T]): Option[T] = {
    if (lo >= hi) None
    else {
      var result: Option[T] = None
      var i = lo
      while (i < hi) {
        values(i) match {
          case Some(v) =>
            result = result match {
              case None => Some(v)
              case Some(cur) => kind match {
                case AggMin => if (ord.lt(v, cur)) Some(v) else result
                case AggMax => if (ord.gt(v, cur)) Some(v) else result
              }
            }
          case None => // skip null
        }
        i += 1
      }
      result
    }
  }

  // ---- Input case ADT ----

  private case class PbtCase(
      values: IndexedSeq[Option[Long]],
      dataType: DataType,   // IntegerType or LongType
      agg: AggKind,
      blockSize: Int,
      fanout: Int) {
    def n: Int = values.length
  }

  // ---- Generators ----

  /** Weighted N: bias toward small boundaries (0,1,2 never hit under uniform). */
  private val genN: Gen[Int] = Gen.frequency(
    (2, Gen.choose(0, 4)),
    (4, Gen.choose(5, 128)),
    (4, Gen.choose(129, 5000))
  )

  /** 20% null, 80% bounded long value (safe for Int cast too). */
  private def genValue(dt: DataType): Gen[Option[Long]] = {
    val bounded: Gen[Long] = dt match {
      case IntegerType => Gen.choose(Int.MinValue.toLong, Int.MaxValue.toLong)
      case LongType => Gen.choose(Long.MinValue, Long.MaxValue)
      case _ => throw new IllegalArgumentException(s"unsupported: $dt")
    }
    Gen.frequency(
      (1, Gen.const(None: Option[Long])),
      (4, bounded.map(Some(_)))
    )
  }

  private val genBlockSize: Gen[Int] =
    Gen.oneOf(1, 16, 32, 64, 256, 1024, 4096, 65536)

  private val genFanout: Gen[Int] = Gen.oneOf(2, 3, 4, 8, 16)

  private val genAgg: Gen[AggKind] = Gen.oneOf(AggMin, AggMax)

  private val genType: Gen[DataType] = Gen.oneOf(IntegerType, LongType)

  private val genCase: Gen[PbtCase] = for {
    dt <- genType
    n <- genN
    values <- Gen.listOfN(n, genValue(dt)).map(_.toIndexedSeq)
    agg <- genAgg
    blockSize <- genBlockSize
    fanout <- genFanout
  } yield PbtCase(values, dt, agg, blockSize, fanout)

  /** Frame [lo, hi) within [0, n]. */
  private val genFrame: PbtCase => Gen[(Int, Int)] = { c =>
    val n = c.n
    for {
      a <- Gen.choose(0, n)
      b <- Gen.choose(0, n)
    } yield if (a <= b) (a, b) else (b, a)
  }

  // ---- Tree build/query glue ----

  private def buildAttr(dt: DataType): AttributeReference =
    AttributeReference("v", dt, nullable = true)()

  private def buildTreeFor(c: PbtCase): (WindowSegmentTree, AttributeReference) = {
    val attr = buildAttr(c.dataType)
    val schema: Seq[Attribute] = Seq(attr)
    val agg: DeclarativeAggregate = c.agg match {
      case AggMin => Min(attr)
      case AggMax => Max(attr)
    }
    val tree = new WindowSegmentTree(
      Array(agg), schema, newMutableProjection,
      c.fanout, c.blockSize, maxCachedBlocks = None,
      taskMemoryManager = TaskContext.get().taskMemoryManager())
    val rows = c.values.iterator.map { opt =>
      val r = new GenericInternalRow(1)
      opt match {
        case Some(v) => c.dataType match {
          case IntegerType => r.update(0, v.toInt)
          case LongType => r.update(0, v)
          case _ => throw new IllegalStateException
        }
        case None => r.setNullAt(0)
      }
      r.asInstanceOf[InternalRow]
    }
    tree.build(rows)
    (tree, attr)
  }

  private def queryResult(
      tree: WindowSegmentTree, dt: DataType, lo: Int, hi: Int): Option[Long] = {
    val out = new SpecificInternalRow(Seq[DataType](dt))
    tree.query(lo, hi, out)
    if (out.isNullAt(0)) None
    else dt match {
      case IntegerType => Some(out.getInt(0).toLong)
      case LongType => Some(out.getLong(0))
      case _ => throw new IllegalStateException
    }
  }

  // ---- Properties (placeholder, filled in Commit 2/3) ----

  test("skeleton: generator produces buildable input") {
    withTaskContext {
      // Smoke: 5 random cases build + trivial full-range query matches oracle.
      forAll(genCase) { c =>
        whenever(c.n > 0) {
          val (tree, _) = buildTreeFor(c)
          try {
            val expected = naiveAgg(c.values, 0, c.n, c.agg)(longOrdering)
            val actual = queryResult(tree, c.dataType, 0, c.n)
            assert(actual == expected,
              s"smoke mismatch: n=${c.n} agg=${c.agg} blockSize=${c.blockSize} " +
                s"fanout=${c.fanout} dt=${c.dataType}")
          } finally tree.close()
        }
      }
    }
  }

  private val longOrdering: Ordering[Long] = Ordering.Long
}

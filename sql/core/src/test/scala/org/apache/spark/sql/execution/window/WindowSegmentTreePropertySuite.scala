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
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Count, DeclarativeAggregate, Max, Min, StddevPop, StddevSamp, Sum}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType}

/**
 * Property-based tests for [[WindowSegmentTree]] using ScalaCheck. Row-by-row
 * naive aggregate is the parallel oracle: every property reduces to
 * `segtree(input) == naive(input)`.
 * Follows `ExpressionEvalHelper` convention (mixes in
 * [[ScalaCheckDrivenPropertyChecks]]). All generated cases share one
 * `SparkContext`/`TaskMemoryManager` by wrapping `forAll` inside a single
 * `withTaskContext`; do NOT invert.
 */
class WindowSegmentTreePropertySuite extends SparkFunSuite
    with LocalSparkContext with ScalaCheckDrivenPropertyChecks {

  // ScalaCheck config: deterministic seed (ScalaCheck 1.14+ default).
  // Quick tier: 100 cases for P1, 10 cases for P2 (see class doc).
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100, minSize = 0, sizeRange = 100)

  // test harness (mirrors WindowSegmentTreeSuite)

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

  // Oracle
  sealed trait AggKind
  case object AggMin extends AggKind
  case object AggMax extends AggKind
  case object AggSum extends AggKind
  case object AggCount extends AggKind

  /**
   * Naive oracle over [lo, hi). Empty frame =&gt; None for Min/Max/Sum
   * (Spark returns NULL); for Count returns Some(0). null inputs are
   * skipped (Spark Min/Max/Sum/Count semantics).
   */
  private def naiveAgg(
      values: IndexedSeq[Option[Long]],
      lo: Int, hi: Int,
      kind: AggKind): Option[Long] = kind match {
    case AggMin | AggMax =>
      if (lo >= hi) None
      else {
        var result: Option[Long] = None
        var i = lo
        while (i < hi) {
          values(i) match {
            case Some(v) =>
              result = result match {
                case None => Some(v)
                case Some(cur) => kind match {
                  case AggMin => if (v < cur) Some(v) else result
                  case AggMax => if (v > cur) Some(v) else result
                  case _ => result
                }
              }
            case None =>
          }
          i += 1
        }
        result
      }
    case AggSum =>
      // Spark Sum returns NULL when all inputs are null OR frame empty.
      var sawNonNull = false
      var s = 0L
      var i = lo
      while (i < hi) {
        values(i) match {
          case Some(v) => sawNonNull = true; s += v
          case None =>
        }
        i += 1
      }
      if (sawNonNull) Some(s) else None
    case AggCount =>
      // Spark COUNT(col) over empty frame returns 0 (never null).
      var c = 0L
      var i = lo
      while (i < hi) {
        if (values(i).isDefined) c += 1
        i += 1
      }
      Some(c)
  }

  // Input case ADT
  private case class PbtCase(
      values: IndexedSeq[Option[Long]],
      dataType: DataType,   // IntegerType or LongType
      agg: AggKind,
      blockSize: Int,
      fanout: Int) {
    def n: Int = values.length
  }

  // Generators
  /** Weighted N: bias toward small boundaries (0,1,2 never hit under uniform). */
  private val genN: Gen[Int] = Gen.frequency(
    (2, Gen.choose(0, 4)),
    (4, Gen.choose(5, 128)),
    (4, Gen.choose(129, 5000))
  )

  /**
   * 20% null, 80% bounded. For (AggSum, LongType) range shrunk to +/-1e12
   * so worst-case sum (1e12 x 5000 = 5e15) stays inside Long bounds.
   */
  private def genValue(dt: DataType, agg: AggKind): Gen[Option[Long]] = {
    val bounded: Gen[Long] = (dt, agg) match {
      case (IntegerType, _) =>
        Gen.choose(Int.MinValue.toLong, Int.MaxValue.toLong)
      case (LongType, AggSum) =>
        Gen.choose(-1000000000000L, 1000000000000L) // +/-1e12
      case (LongType, _) =>
        Gen.choose(Long.MinValue, Long.MaxValue)
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

  private val genAgg: Gen[AggKind] =
    Gen.oneOf(AggMin, AggMax, AggSum, AggCount)

  private val genType: Gen[DataType] = Gen.oneOf(IntegerType, LongType)

  private val genCase: Gen[PbtCase] = for {
    dt <- genType
    agg <- genAgg
    n <- genN
    values <- Gen.listOfN(n, genValue(dt, agg)).map(_.toIndexedSeq)
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

  // Tree build/query glue
  private def buildAttr(dt: DataType): AttributeReference =
    AttributeReference("v", dt, nullable = true)()

  /** Output type of the aggregate (segtree's queryInto materializes this). */
  private def aggOutputType(c: PbtCase): DataType = c.agg match {
    case AggMin | AggMax => c.dataType        // same as input
    case AggSum | AggCount => LongType        // Sum(Int|Long)->Long; Count->Long
  }

  private def buildTreeFor(c: PbtCase): (WindowSegmentTree, AttributeReference) = {
    val attr = buildAttr(c.dataType)
    val schema: Seq[Attribute] = Seq(attr)
    val agg: DeclarativeAggregate = c.agg match {
      case AggMin => Min(attr)
      case AggMax => Max(attr)
      case AggSum => Sum(attr)
      case AggCount => Count(Seq(attr))
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
    SegmentTreeWindowTestHelper.buildTreeFromIter(tree, rows, schema)
    (tree, attr)
  }

  private def queryResult(
      tree: WindowSegmentTree, outDt: DataType, lo: Int, hi: Int): Option[Long] = {
    val out = new SpecificInternalRow(Seq[DataType](outDt))
    tree.query(lo, hi, out)
    if (out.isNullAt(0)) None
    else outDt match {
      case IntegerType => Some(out.getInt(0).toLong)
      case LongType => Some(out.getLong(0))
      case _ => throw new IllegalStateException
    }
  }

  // Properties
  /**
   * P1 Equivalence: segtree query result equals naive aggregate over random
   * frames. Probes multiple frames per case (empty / degenerate / full /
   * random) to amplify block/fanout edge coverage.
   */
  test("P1 equivalence: segtree query equals naive aggregate over random frames") {
    withTaskContext {
      forAll(genCase) { c =>
        val (tree, _) = buildTreeFor(c)
        val outDt = aggOutputType(c)
        try {
          val frames = boundaryFrames(c.n) ++ randomFrames(c.n, k = 8, seed = c.n.toLong)
          frames.foreach { case (lo, hi) =>
            val expected = naiveAgg(c.values, lo, hi, c.agg)
            val actual = queryResult(tree, outDt, lo, hi)
            assert(actual == expected,
              s"P1 mismatch: n=${c.n} agg=${c.agg} blockSize=${c.blockSize} " +
                s"fanout=${c.fanout} dt=${c.dataType} frame=[$lo,$hi) " +
                s"expected=$expected actual=$actual")
          }
        } finally tree.close()
      }
    }
  }

  /** Boundary frames: empty, full, head, tail, single-element. */
  private def boundaryFrames(n: Int): Seq[(Int, Int)] = {
    if (n == 0) Seq((0, 0))
    else Seq(
      (0, 0),            // empty at start
      (n, n),            // empty at end
      (0, n),            // full
      (0, 1),            // single head
      (n - 1, n),        // single tail
      (n / 2, n / 2),    // empty middle
      (0, n / 2),        // prefix
      (n / 2, n)         // suffix
    ).distinct
  }

  /** Deterministic random frames; seed tied to case to aid shrink repro. */
  private def randomFrames(n: Int, k: Int, seed: Long): Seq[(Int, Int)] = {
    if (n == 0) return Nil
    val rnd = new scala.util.Random(seed)
    Seq.fill(k) {
      val a = rnd.nextInt(n + 1)
      val b = rnd.nextInt(n + 1)
      if (a <= b) (a, b) else (b, a)
    }
  }

  /**
   * P2 Determinism (10-case smoke): rebuild + requery yields identical
   * results. Segtree has no RNG/hash iteration; acts as low-cost regression
   * sentinel against future non-deterministic refactors.
   */
  test("P2 determinism: rebuild + requery yields identical results") {
    withTaskContext {
      forAll(genCase, minSuccessful(10)) { c =>
        val frames = boundaryFrames(c.n) ++ randomFrames(c.n, k = 2, seed = 0xD2L)
        val run1 = runFrames(c, frames)
        val run2 = runFrames(c, frames)
        assert(run1 == run2,
          s"P2 mismatch: n=${c.n} agg=${c.agg} blockSize=${c.blockSize} " +
            s"fanout=${c.fanout} dt=${c.dataType} run1=$run1 run2=$run2")
      }
    }
  }

  private def runFrames(c: PbtCase, frames: Seq[(Int, Int)]): Seq[Option[Long]] = {
    val (tree, _) = buildTreeFor(c)
    val outDt = aggOutputType(c)
    try {
      frames.map { case (lo, hi) => queryResult(tree, outDt, lo, hi) }
    } finally tree.close()
  }

  // Floating-point family (Avg / StddevSamp / StddevPop).
  // Welford merge order in segtree differs from row-by-row SlidingWindowFrame,
  // so equality is NOT bit-exact; compared with relative tol 1e-9 + absolute
  // floor for near-zero (e.g. constant-input stddev). Kept separate to avoid
  // muddying the integer-family exact-equality contract.

  sealed trait FpAggKind
  case object FpAvg extends FpAggKind
  case object FpStddevSamp extends FpAggKind
  case object FpStddevPop extends FpAggKind

  private case class FpCase(
      values: IndexedSeq[Option[Double]],
      agg: FpAggKind,
      blockSize: Int,
      fanout: Int) {
    def n: Int = values.length
  }

  /**
   * 20% null, 80% bounded gaussian-ish double. Range chosen so partial
   * sums stay well within Double precision over n <= 5000.
   */
  private val genFpValue: Gen[Option[Double]] = {
    val bounded: Gen[Double] = Gen.choose(-1e6, 1e6)
    Gen.frequency(
      (1, Gen.const(None: Option[Double])),
      (4, bounded.map(Some(_)))
    )
  }

  private val genFpAgg: Gen[FpAggKind] =
    Gen.oneOf(FpAvg, FpStddevSamp, FpStddevPop)

  private val genFpCase: Gen[FpCase] = for {
    agg <- genFpAgg
    n <- genN
    values <- Gen.listOfN(n, genFpValue).map(_.toIndexedSeq)
    blockSize <- genBlockSize
    fanout <- genFanout
  } yield FpCase(values, agg, blockSize, fanout)

  /**
   * Naive oracle for FP aggregates over [lo, hi). Returns None when Spark
   * would emit NULL: empty frame, all-null frame, and (for StddevSamp)
   * count &lt; 2.
   */
  private def naiveFpAgg(
      values: IndexedSeq[Option[Double]],
      lo: Int, hi: Int,
      kind: FpAggKind): Option[Double] = {
    val nonNull = (lo until hi).flatMap(values).toArray
    val n = nonNull.length
    if (n == 0) return None
    val mean = nonNull.sum / n
    kind match {
      case FpAvg => Some(mean)
      case FpStddevSamp =>
        if (n < 2) None
        else {
          var sq = 0.0
          var i = 0
          while (i < n) { val d = nonNull(i) - mean; sq += d * d; i += 1 }
          Some(math.sqrt(sq / (n - 1)))
        }
      case FpStddevPop =>
        var sq = 0.0
        var i = 0
        while (i < n) { val d = nonNull(i) - mean; sq += d * d; i += 1 }
        Some(math.sqrt(sq / n))
    }
  }

  /** Build tree + matching AggregateProcessor so we can `queryInto` to evaluate. */
  private def buildFpTreeFor(
      c: FpCase): (WindowSegmentTree, AggregateProcessor) = {
    val attr = AttributeReference("v", DoubleType, nullable = true)()
    val schema: Seq[Attribute] = Seq(attr)
    val agg: DeclarativeAggregate = c.agg match {
      case FpAvg => Average(attr)
      case FpStddevSamp => StddevSamp(attr)
      case FpStddevPop => StddevPop(attr)
    }
    val tree = new WindowSegmentTree(
      Array(agg), schema, newMutableProjection,
      c.fanout, c.blockSize, maxCachedBlocks = None,
      taskMemoryManager = TaskContext.get().taskMemoryManager())
    val rows = c.values.iterator.map { opt =>
      val r = new GenericInternalRow(1)
      opt match {
        case Some(v) => r.update(0, v)
        case None => r.setNullAt(0)
      }
      r.asInstanceOf[InternalRow]
    }
    SegmentTreeWindowTestHelper.buildTreeFromIter(tree, rows, schema)
    val processor = AggregateProcessor(
      Array[Expression](agg),
      ordinal = 0,
      inputAttributes = schema,
      newMutableProjection = newMutableProjection,
      filters = Array[Option[Expression]](None))
    (tree, processor)
  }

  private def queryFpResult(
      tree: WindowSegmentTree,
      processor: AggregateProcessor,
      lo: Int, hi: Int): Option[Double] = {
    val out = new SpecificInternalRow(Seq[DataType](DoubleType))
    tree.queryInto(lo, hi, processor, out)
    if (out.isNullAt(0)) None else Some(out.getDouble(0))
  }

  private def fpClose(actual: Option[Double], expected: Option[Double]): Boolean =
    (actual, expected) match {
      case (None, None) => true
      case (Some(a), Some(e)) =>
        if (java.lang.Double.isNaN(a) && java.lang.Double.isNaN(e)) true
        else {
          val absDiff = math.abs(a - e)
          val denom = math.max(math.abs(e), 1.0)  // absolute floor for near-zero
          absDiff / denom < 1e-9
        }
      case _ => false
    }

  /**
   * P4 Equivalence (FP family): segtree avg/stddev within 1e-9 relative
   * tolerance of naive oracle. Smaller minSuccessful (50) than P1 because
   * Welford-merge is heavier than Min/Max/Sum/Count.
   */
  test("P4 fp equivalence: segtree avg/stddev within 1e-9 of naive oracle") {
    withTaskContext {
      forAll(genFpCase, minSuccessful(50)) { c =>
        val (tree, processor) = buildFpTreeFor(c)
        try {
          val frames = boundaryFrames(c.n) ++ randomFrames(c.n, k = 6, seed = c.n.toLong)
          frames.foreach { case (lo, hi) =>
            val expected = naiveFpAgg(c.values, lo, hi, c.agg)
            val actual = queryFpResult(tree, processor, lo, hi)
            assert(fpClose(actual, expected),
              s"P4 fp mismatch: n=${c.n} agg=${c.agg} blockSize=${c.blockSize} " +
                s"fanout=${c.fanout} frame=[$lo,$hi) " +
                s"expected=$expected actual=$actual")
          }
        } finally tree.close()
      }
    }
  }
}

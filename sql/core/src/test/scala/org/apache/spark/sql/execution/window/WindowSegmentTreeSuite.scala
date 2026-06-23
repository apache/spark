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

import scala.util.Random

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkException, SparkFunSuite, TaskContext}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, MutableProjection, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{DeclarativeAggregate, Max, Min, StddevSamp, Sum}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType}

class WindowSegmentTreeSuite extends SparkFunSuite with LocalSparkContext {

  // test harness
  private val inputAttr: AttributeReference =
    AttributeReference("v", IntegerType, nullable = true)()
  private val inputSchema: Seq[Attribute] = Seq(inputAttr)

  private def newMutableProjection
      : (Seq[Expression], Seq[Attribute]) => MutableProjection =
    (exprs, attrs) => GenerateMutableProjection.generate(exprs, attrs)

  private def minAgg: DeclarativeAggregate = Min(inputAttr)

  private def withTaskContext[T](body: => T): T = {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
    TaskContext.setTaskContext(taskContext)
    try body finally {
      TaskContext.unset()
    }
  }

  private def buildTree(
      values: Seq[Int],
      aggs: Array[DeclarativeAggregate] = Array(minAgg),
      fanout: Int = WindowSegmentTree.DefaultFanout,
      blockSize: Int = WindowSegmentTree.DefaultBlockSize,
      maxCachedBlocks: Option[Int] = None): WindowSegmentTree = {
    val tree = new WindowSegmentTree(
      aggs, inputSchema, newMutableProjection, fanout, blockSize, maxCachedBlocks,
      taskMemoryManager = TaskContext.get().taskMemoryManager())
    val rows = values.iterator.map { v =>
      val r = new GenericInternalRow(1)
      r.update(0, v)
      r.asInstanceOf[InternalRow]
    }
    SegmentTreeWindowTestHelper.buildTreeFromIter(tree, rows, inputSchema)
    tree
  }

  /** Naive oracle: MIN over values[lo, hi). Returns Int box or null. */
  private def naiveMin(values: Seq[Int], lo: Int, hi: Int): Any = {
    if (lo >= hi) null
    else {
      var m = values(lo)
      var i = lo + 1
      while (i < hi) { if (values(i) < m) m = values(i); i += 1 }
      m.asInstanceOf[java.lang.Integer]
    }
  }

  private def newOutBuffer(): SpecificInternalRow =
    new SpecificInternalRow(Seq[org.apache.spark.sql.types.DataType](IntegerType))

  private def queryMin(tree: WindowSegmentTree, lo: Int, hi: Int): Any = {
    val out = newOutBuffer()
    tree.query(lo, hi, out)
    if (out.isNullAt(0)) null else out.getInt(0)
  }

  test("build and single-point query returns identity; full scan matches naive") {
    withTaskContext {
      val values = Seq(5, 2, 9, 1, 7, 3, 4, 8, 6, 0)
      val tree = buildTree(values, fanout = 4, blockSize = 1024)
      try {
        // single-point queries -> identity (null for MIN)
        for (i <- 0 to values.length) {
          assert(queryMin(tree, i, i) == null, s"identity at i=$i")
        }
        // full scan
        assert(queryMin(tree, 0, values.length) == naiveMin(values, 0, values.length))
      } finally tree.close()
    }
  }

  test("single-block: range query matches naive baseline for random ranges") {
    withTaskContext {
      val rnd = new Random(0xC0FFEE)
      val values = Seq.fill(100)(rnd.nextInt(1000))
      val tree = buildTree(values, fanout = 8, blockSize = 1024)
      try {
        for (_ <- 0 until 20) {
          val a = rnd.nextInt(values.length + 1)
          val b = rnd.nextInt(values.length + 1)
          val (lo, hi) = (math.min(a, b), math.max(a, b))
          assert(queryMin(tree, lo, hi) == naiveMin(values, lo, hi),
            s"mismatch at [$lo, $hi)")
        }
      } finally tree.close()
    }
  }

  test("fanout boundaries: sizes {1, F, F+1, F*F} for fanout in {2,4,8,16}") {
    withTaskContext {
      val rnd = new Random(42)
      for (fanout <- Seq(2, 4, 8, 16)) {
        val sizes = Seq(1, fanout, fanout + 1, fanout * fanout).distinct
        for (n <- sizes) {
          val values = Seq.fill(n)(rnd.nextInt(10000))
          val tree = buildTree(values, fanout = fanout, blockSize = 1024 * 1024)
          try {
            // Exhaustive ranges for small n.
            for (lo <- 0 to n; hi <- lo to n) {
              assert(queryMin(tree, lo, hi) == naiveMin(values, lo, hi),
                s"fanout=$fanout n=$n [$lo,$hi)")
            }
            // Level-size invariant: ceil(prev / fanout)
            if (n > 0) {
              val numLevels = tree.peekLevelCount(0)
              var prev = n
              for (l <- 1 until numLevels) {
                val expected = math.ceil(prev.toDouble / fanout).toInt
                val actual = tree.peekLevelSize(0, l)
                assert(actual == expected,
                  s"level-size wrong at fanout=$fanout n=$n level=$l: $actual vs $expected")
                prev = expected
              }
            }
          } finally tree.close()
        }
      }
    }
  }

  test("identity at empty range query(k, k)") {
    withTaskContext {
      val values = (1 to 50).reverse
      val tree = buildTree(values, fanout = 4, blockSize = 16)
      try {
        for (k <- Seq(0, values.length / 2, values.length)) {
          assert(queryMin(tree, k, k) == null, s"identity at k=$k")
        }
      } finally tree.close()
    }
  }

  test("block boundary correctness: cross-block vs single-block baseline") {
    withTaskContext {
      val rnd = new Random(123)
      val values = Seq.fill(100)(rnd.nextInt(10000))
      val treeBlocked = buildTree(values, fanout = 4, blockSize = 16)
      val treeBaseline = buildTree(values, fanout = 4, blockSize = 1024)
      try {
        assert(treeBlocked.peekBlockCount == (100 + 16 - 1) / 16)
        for (lo <- 0 to 100; hi <- lo to 100) {
          assert(queryMin(treeBlocked, lo, hi) == queryMin(treeBaseline, lo, hi),
            s"cross-block mismatch at [$lo, $hi)")
        }
      } finally {
        treeBlocked.close()
        treeBaseline.close()
      }
    }
  }

  test("LRU stability: same queries in different orders produce same results") {
    withTaskContext {
      val rnd = new Random(777)
      val values = Seq.fill(100)(rnd.nextInt(10000))
      val tree = buildTree(
        values, fanout = 4, blockSize = 16, maxCachedBlocks = Some(2))
      try {
        val queries = Seq.fill(30) {
          val a = rnd.nextInt(101)
          val b = rnd.nextInt(101)
          (math.min(a, b), math.max(a, b))
        }
        val results1 = queries.map { case (lo, hi) => queryMin(tree, lo, hi) }
        val reordered = rnd.shuffle(queries.zipWithIndex)
        val results2 = Array.fill[Any](queries.length)(null)
        reordered.foreach { case ((lo, hi), idx) =>
          results2(idx) = queryMin(tree, lo, hi)
        }
        for (i <- queries.indices) {
          assert(results1(i) == results2(i),
            s"LRU instability at query $i ${queries(i)}: ${results1(i)} vs ${results2(i)}")
        }
        // Also cross-check against naive oracle.
        for (i <- queries.indices) {
          val (lo, hi) = queries(i)
          assert(results1(i) == naiveMin(values, lo, hi))
        }
      } finally tree.close()
    }
  }

  test("cross-block: range query matches naive baseline for random ranges") {
    withTaskContext {
      val rnd = new Random(0xBEEF)
      val values = Seq.fill(100)(rnd.nextInt(1000))
      val tree = buildTree(values, fanout = 4, blockSize = 8)
      try {
        assert(tree.peekBlockCount == (100 + 8 - 1) / 8)
        for (_ <- 0 until 50) {
          val a = rnd.nextInt(values.length + 1)
          val b = rnd.nextInt(values.length + 1)
          val (lo, hi) = (math.min(a, b), math.max(a, b))
          assert(queryMin(tree, lo, hi) == naiveMin(values, lo, hi),
            s"mismatch at [$lo, $hi)")
        }
      } finally tree.close()
    }
  }

  test("cross-block: multi-block level-size invariant") {
    withTaskContext {
      val rnd = new Random(31337)
      val fanout = 4
      val blockSize = 8
      val numRows = 50 // > blockSize -> multiple blocks
      val values = Seq.fill(numRows)(rnd.nextInt(10000))
      val tree = buildTree(values, fanout = fanout, blockSize = blockSize)
      try {
        val numBlocks = tree.peekBlockCount
        assert(numBlocks > 1, s"expected >1 block, got $numBlocks")
        for (b <- 0 until numBlocks) {
          val blockStart = b * blockSize
          val blockRows = math.min(blockSize, numRows - blockStart)
          val numLevels = tree.peekLevelCount(b)
          var prev = blockRows
          for (l <- 1 until numLevels) {
            val expected = math.ceil(prev.toDouble / fanout).toInt
            val actual = tree.peekLevelSize(b, l)
            assert(actual == expected,
              s"block=$b level=$l: $actual vs $expected")
            prev = expected
          }
        }
        // Correctness cross-check.
        for (lo <- 0 to numRows; hi <- lo to numRows) {
          assert(queryMin(tree, lo, hi) == naiveMin(values, lo, hi),
            s"cross-block MIN mismatch at [$lo, $hi)")
        }
      } finally tree.close()
    }
  }

  // D8 multi-aggregate
  test("D8 multi-aggregate: MIN + MAX + SUM on the same tree") {
    withTaskContext {
      val rnd = new Random(2024)
      val numRows = 50
      val values = Seq.fill(numRows)(rnd.nextInt(1000))
      val aggs: Array[DeclarativeAggregate] =
        Array(Min(inputAttr), Max(inputAttr), Sum(inputAttr))
      val tree = buildTree(values, aggs = aggs, fanout = 4, blockSize = 8)
      // Output schema: MIN(int), MAX(int), SUM(long).
      // Sum on IntegerType widens the buffer slot to LongType.
      val outTypes: Seq[DataType] = Seq(IntegerType, IntegerType, LongType)
      def queryAll(lo: Int, hi: Int): (Any, Any, Any) = {
        val out = new SpecificInternalRow(outTypes)
        tree.query(lo, hi, out)
        val mn = if (out.isNullAt(0)) null else out.getInt(0)
        val mx = if (out.isNullAt(1)) null else out.getInt(1)
        val sm = if (out.isNullAt(2)) null else out.getLong(2)
        (mn, mx, sm)
      }
      def naiveMax(vs: Seq[Int], lo: Int, hi: Int): Any =
        if (lo >= hi) null else vs.slice(lo, hi).max
      def naiveSum(vs: Seq[Int], lo: Int, hi: Int): Any =
        if (lo >= hi) null else vs.slice(lo, hi).map(_.toLong).sum
      try {
        for (_ <- 0 until 20) {
          val a = rnd.nextInt(numRows + 1)
          val b = rnd.nextInt(numRows + 1)
          val (lo, hi) = (math.min(a, b), math.max(a, b))
          val (mn, mx, sm) = queryAll(lo, hi)
          assert(mn == naiveMin(values, lo, hi), s"MIN at [$lo,$hi)")
          assert(mx == naiveMax(values, lo, hi), s"MAX at [$lo,$hi)")
          assert(sm == naiveSum(values, lo, hi), s"SUM at [$lo,$hi)")
        }
      } finally tree.close()
    }
  }

  // D9 spill coverage
  test("D9 spill path: low thresholds still produce correct range aggregates") {
    withTaskContext {
      val rnd = new Random(909)
      val numRows = 60
      val values = Seq.fill(numRows)(rnd.nextInt(1000))
      val tree = new WindowSegmentTree(
        Array(minAgg), inputSchema, newMutableProjection,
        fanout = 4, blockSize = 8, maxCachedBlocks = Some(2),
        taskMemoryManager = TaskContext.get().taskMemoryManager())
      val rows = values.iterator.map { v =>
        val r = new GenericInternalRow(1); r.update(0, v); r.asInstanceOf[InternalRow]
      }
      SegmentTreeWindowTestHelper.buildTreeFromIter(
        tree, rows, inputSchema, inMemoryThreshold = 4, spillThreshold = 8)
      try {
        for (_ <- 0 until 40) {
          val a = rnd.nextInt(numRows + 1)
          val b = rnd.nextInt(numRows + 1)
          val (lo, hi) = (math.min(a, b), math.max(a, b))
          assert(queryMin(tree, lo, hi) == naiveMin(values, lo, hi),
            s"spill-path mismatch at [$lo, $hi)")
        }
      } finally tree.close()
    }
  }

  test("D10 rebuild: second build replaces state; failed build preserves prior state") {
    withTaskContext {
      val v1 = Seq(5, 1, 9, 3, 7, 2, 8, 4, 6, 0)
      val v2 = Seq(100, 200, 50, 400, 25, 600, 12, 800)
      val v3 = Seq(10, 20, 30, 40, 50, 60, 70, 80, 90, 5)
      val tree = new WindowSegmentTree(
        Array(minAgg), inputSchema, newMutableProjection,
        fanout = 4, blockSize = 4,
        taskMemoryManager = TaskContext.get().taskMemoryManager())

      def iterOf(vs: Seq[Int]): Iterator[InternalRow] = vs.iterator.map { v =>
        val r = new GenericInternalRow(1); r.update(0, v); r.asInstanceOf[InternalRow]
      }

      try {
        SegmentTreeWindowTestHelper.buildTreeFromIter(tree, iterOf(v1), inputSchema)
        assert(queryMin(tree, 0, v1.length) == v1.min)
        SegmentTreeWindowTestHelper.buildTreeFromIter(tree, iterOf(v2), inputSchema)
        assert(tree.size == v2.length)
        for (lo <- 0 to v2.length; hi <- lo to v2.length) {
          assert(queryMin(tree, lo, hi) == naiveMin(v2, lo, hi),
            s"post-rebuild mismatch at [$lo, $hi)")
        }

        // Now simulate a failing build midway through; prior state must remain queryable.
        val boomIter: Iterator[InternalRow] = new Iterator[InternalRow] {
          private var emitted = 0
          override def hasNext: Boolean = true
          override def next(): InternalRow = {
            if (emitted >= 3) throw new RuntimeException("boom")
            emitted += 1
            val r = new GenericInternalRow(1); r.update(0, -1); r.asInstanceOf[InternalRow]
          }
        }
        intercept[RuntimeException](
          SegmentTreeWindowTestHelper.buildTreeFromIter(tree, boomIter, inputSchema))
        // Prior v2 state intact.
        assert(tree.size == v2.length)
        assert(queryMin(tree, 0, v2.length) == v2.min)

        // Build v3 successfully after the failure.
        SegmentTreeWindowTestHelper.buildTreeFromIter(tree, iterOf(v3), inputSchema)
        assert(tree.size == v3.length)
        for (lo <- 0 to v3.length; hi <- lo to v3.length) {
          assert(queryMin(tree, lo, hi) == naiveMin(v3, lo, hi),
            s"post-recovery mismatch at [$lo, $hi)")
        }
      } finally tree.close()
    }
  }

  test("D11 error paths: invalid ctor args and invalid query ranges") {
    withTaskContext {
      // Constructor validation.
      intercept[IllegalArgumentException] {
        new WindowSegmentTree(Array(minAgg), inputSchema, newMutableProjection, fanout = 1)
      }
      intercept[IllegalArgumentException] {
        new WindowSegmentTree(Array(minAgg), inputSchema, newMutableProjection, blockSize = 0)
      }
      intercept[IllegalArgumentException] {
        new WindowSegmentTree(
          Array(minAgg), inputSchema, newMutableProjection, maxCachedBlocks = Some(0))
      }
      intercept[IllegalArgumentException] {
        new WindowSegmentTree(
          Array(minAgg), inputSchema, newMutableProjection, maxCachedBlocks = Some(-1))
      }

      // Query range validation.
      val values = Seq(3, 1, 4, 1, 5, 9, 2, 6, 5, 3)
      val tree = buildTree(values, fanout = 4, blockSize = 4)
      try {
        // Pre-fill outBuffer with a sentinel, then assert bounds check leaves it alone.
        def sentinelBuf(): SpecificInternalRow = {
          val b = newOutBuffer()
          b.setInt(0, 0x5EEDED)
          b
        }
        val sz = tree.size
        for ((lo, hi) <- Seq((-1, 5), (0, sz + 1), (5, 3))) {
          val out = sentinelBuf()
          val ex = intercept[SparkException](tree.query(lo, hi, out))
          assert(ex.getCondition == "INTERNAL_ERROR")
          assert(!out.isNullAt(0) && out.getInt(0) == 0x5EEDED,
            s"outBuffer mutated by invalid query [$lo,$hi)")
        }
      } finally tree.close()
    }
  }

  // STDDEV_SAMP regression: minimal repro for the digest mismatch in GHA run
  // 24599916378 (WindowBenchmark Section A). MIN/MAX/SUM/COUNT/AVG pass
  // digest parity; STDDEV_SAMP is the first multi-buffer agg combining
  // CentralMomentAgg's Welford merge (n, avg, m2) with a non-trivial
  // `evaluateExpression`. Exercises both merge (`query`) and evaluate
  // (`queryInto` + `AggregateProcessor`) paths.
  test("STDDEV_SAMP: segtree matches naive oracle on random doubles, W=21") {
    withTaskContext {
      val rnd = new Random(0x57DDEFL) // fixed seed
      val n = 100
      val values: Array[Double] = Array.fill(n)(rnd.nextGaussian() * 1000.0 + 50.0)
      val doubleAttr = AttributeReference("v", DoubleType, nullable = true)()
      val schema: Seq[Attribute] = Seq(doubleAttr)
      val agg: DeclarativeAggregate = StddevSamp(doubleAttr)

      // Use a block size that forces cross-block merges at W=21: blockSize=8.
      val tree = new WindowSegmentTree(
        Array(agg), schema, newMutableProjection,
        fanout = 4, blockSize = 8, maxCachedBlocks = None,
        taskMemoryManager = TaskContext.get().taskMemoryManager())
      try {
        val rows = values.iterator.map { v =>
          val r = new GenericInternalRow(1)
          r.update(0, v)
          r.asInstanceOf[InternalRow]
        }
        SegmentTreeWindowTestHelper.buildTreeFromIter(tree, rows, schema)

        // AggregateProcessor identical to the Frame's, so `queryInto`
        // exercises evaluateExpression (sqrt(m2/(n-1)) with n=1 / div-by-0
        // guards in StddevSamp).
        val processor = AggregateProcessor(
          Array[Expression](agg),
          ordinal = 0,
          inputAttributes = schema,
          newMutableProjection = newMutableProjection,
          filters = Array[Option[Expression]](None))

        val out = new SpecificInternalRow(Seq[DataType](DoubleType))

        val halfW = 10 // W=21
        var i = 0
        var maxRelErr = 0.0
        while (i < n) {
          val lo = math.max(0, i - halfW)
          val hi = math.min(n, i + halfW + 1)
          tree.queryInto(lo, hi, processor, out)
          val actual = if (out.isNullAt(0)) Double.NaN else out.getDouble(0)
          val expected = naiveStddevSamp(values, lo, hi)
          // NaN handling: legacy `nullOnDivideByZero=true` -> null when n<=1.
          if (java.lang.Double.isNaN(expected)) {
            assert(out.isNullAt(0),
              s"expected null at i=$i [$lo,$hi), got $actual")
          } else {
            assert(!out.isNullAt(0),
              s"expected $expected at i=$i [$lo,$hi), got null")
            val denom = math.max(math.abs(expected), 1e-12)
            val rel = math.abs(actual - expected) / denom
            if (rel > maxRelErr) maxRelErr = rel
            assert(rel < 1e-9,
              s"STDDEV_SAMP mismatch at i=$i [$lo,$hi): " +
                s"expected=$expected actual=$actual relErr=$rel")
          }
          i += 1
        }
      } finally tree.close()
    }
  }

  /** Naive STDDEV_SAMP over [lo, hi); returns NaN for n < 2 to signal null. */
  private def naiveStddevSamp(values: Array[Double], lo: Int, hi: Int): Double = {
    val n = hi - lo
    if (n < 2) return Double.NaN
    var sum = 0.0
    var i = lo
    while (i < hi) { sum += values(i); i += 1 }
    val mean = sum / n
    var sq = 0.0
    i = lo
    while (i < hi) { val d = values(i) - mean; sq += d * d; i += 1 }
    math.sqrt(sq / (n - 1))
  }

  test("D12 block-aligned cross-block boundaries") {
    withTaskContext {
      val rnd = new Random(12)
      val numRows = 50
      val blockSize = 10
      val values = Seq.fill(numRows)(rnd.nextInt(10000))
      val tree = buildTree(values, fanout = 4, blockSize = blockSize)
      try {
        for ((lo, hi) <- Seq((0, 20), (10, 40), (20, 50), (0, 50))) {
          assert(queryMin(tree, lo, hi) == naiveMin(values, lo, hi),
            s"aligned mismatch at [$lo,$hi)")
        }
      } finally tree.close()
    }
  }
}

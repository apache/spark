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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite, TaskContext}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, MutableProjection, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{DeclarativeAggregate, Min}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.types.IntegerType

class WindowSegmentTreeSuite extends SparkFunSuite with LocalSparkContext {

  // ---- test harness ----

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
      fanout: Int = WindowSegmentTree.DEFAULT_FANOUT,
      blockSize: Int = WindowSegmentTree.DEFAULT_BLOCK_SIZE,
      maxCachedBlocks: Int = -1): WindowSegmentTree = {
    val tree = new WindowSegmentTree(
      aggs, inputSchema, newMutableProjection, fanout, blockSize, maxCachedBlocks)
    val rows = values.iterator.map { v =>
      val r = new GenericInternalRow(1)
      r.update(0, v)
      r.asInstanceOf[InternalRow]
    }
    tree.build(rows, values.length)
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
        // single-point queries → identity (null for MIN)
        for (i <- 0 to values.length) {
          assert(queryMin(tree, i, i) == null, s"identity at i=$i")
        }
        // full scan
        assert(queryMin(tree, 0, values.length) == naiveMin(values, 0, values.length))
      } finally tree.close()
    }
  }

    test("range query matches naive baseline for random ranges") {
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
        values, fanout = 4, blockSize = 16, maxCachedBlocks = 2)
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
}

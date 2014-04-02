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

package org.apache.spark.mllib.linalg.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors

/**
 * Represents a matrix in coordinate list format.
 *
 * @param entries matrix entries
 * @param m number of rows (default: -1L, which means unknown)
 * @param n number of column (default: -1L, which means unknown)
 */
class CoordinateRDDMatrix(
    val entries: RDD[RDDMatrixEntry],
    m: Long = -1L,
    n: Long = -1L) extends RDDMatrix {

  private var _m = m
  private var _n = n

  /** Gets or computes the number of columns. */
  override def numCols(): Long = {
    if (_n < 0) {
      computeSize()
    }
    _n
  }

  /** Gets or computes the number of rows. */
  override def numRows(): Long = {
    if (_m < 0) {
      computeSize()
    }
    _m
  }

  private def computeSize() {
    val (m1, n1) = entries.map(entry => (entry.i, entry.j)).reduce { case ((i1, j1), (i2, j2)) =>
      (math.max(i1, i2), math.max(j1, j2))
    }
    // There may be empty columns at the very right and empty rows at the very bottom.
    _m = math.max(_m, m1 + 1L)
    _n = math.max(_n, n1 + 1L)
  }

  def toIndexedRowRDDMatrix(): IndexedRowRDDMatrix = {
    val n = numCols().toInt
    val indexedRows = entries.map(entry => (entry.i, (entry.j.toInt, entry.value)))
      .groupByKey()
      .map { case (i, vectorEntries) =>
      IndexedRDDMatrixRow(i, Vectors.sparse(n, vectorEntries))
    }
    new IndexedRowRDDMatrix(indexedRows, numRows(), numCols())
  }
}

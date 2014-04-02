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

/**
 * Represents a row-oriented RDDMatrix with indexed rows.
 *
 * @param rows indexed rows of this matrix
 * @param m number of rows, where a negative number means unknown
 * @param n number of cols, where a negative number means unknown
 */
class IndexedRowRDDMatrix(
    val rows: RDD[IndexedRDDMatrixRow],
    m: Long = -1L,
    n: Long = -1L) extends RDDMatrix {

  private var _m = m
  private var _n = n

  /** Gets or computes the number of columns. */
  override def numCols(): Long = {
    if (_n < 0) {
      _n = rows.first().vector.size
    }
    _n
  }

  override def numRows(): Long = {
    if (_m < 0) {
      _m = rows.map(_.index).reduce(math.max) + 1
    }
    _m
  }

  /** Drops row indices and converts this matrix to a RowRDDMatrix. */
  def toRowRDDMatrix(): RowRDDMatrix = {
    new RowRDDMatrix(rows.map(_.vector), -1, _n)
  }
}

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

package org.apache.spark.ml.linalg

import scala.collection.mutable.ArrayBuffer

/** Matrix rendering adapted from Breeze 2.1.0, preserving its layout and truncation rules. */
private[linalg] object MatrixFormatter {

  def format(matrix: Matrix): String = {
    // Breeze uses a fixed 80 x 80 terminal and reserves three lines for sparse matrices.
    val maxLines = if (matrix.isInstanceOf[SparseMatrix]) 77 else 80
    format(matrix, maxLines, 80)
  }

  def format(matrix: Matrix, maxLines: Int, maxLineWidth: Int): String = matrix match {
    case sparse: SparseMatrix => formatSparse(sparse, maxLines)
    case dense: DenseMatrix => formatDense(dense, maxLines, maxLineWidth)
  }

  private def formatSparse(matrix: SparseMatrix, maxLines: Int): String = {
    // The previous Breeze transpose also reordered entries and removed explicit zeros.
    val columns = if (matrix.isTransposed) matrix.toSparseColMajor else matrix
    val result = new StringBuilder("%d x %d CSCMatrix".format(matrix.numRows, matrix.numCols))
    val limit = math.min(columns.values.length, maxLines - 1)
    var col = 0
    var index = 0
    while (index < limit) {
      while (index >= columns.colPtrs(col + 1)) {
        col += 1
      }
      result.append('\n').append("(%d,%d) ".format(columns.rowIndices(index), col))
        .append(columns.values(index))
      index += 1
    }
    result.toString()
  }

  private def formatDense(matrix: DenseMatrix, maxLines: Int, maxLineWidth: Int): String = {
    val rows = matrix.numRows
    val cols = matrix.numCols
    val showRows = if (rows > maxLines) maxLines - 1 else rows
    val widths = new ArrayBuffer[Int]
    var totalWidth = 0
    var col = 0
    while (col < cols && totalWidth < maxLineWidth) {
      var width = 0
      var row = 0
      while (row < showRows) {
        width = math.max(width, matrix(row, col).toString.length + 2)
        row += 1
      }
      widths += width
      totalWidth += width
      col += 1
    }

    if (widths.length < cols) {
      // Reserve space for the column truncation marker, including its padding.
      val markerWidth = cols.toString.length + 12
      while (totalWidth + markerWidth >= maxLineWidth) {
        if (widths.isEmpty) {
          return "%d x %d matrix".format(rows, cols)
        }
        totalWidth -= widths.remove(widths.length - 1)
      }
    }

    val result = new StringBuilder
    var row = 0
    while (row < showRows) {
      col = 0
      while (col < widths.length) {
        val cell = matrix(row, col).toString
        result.append(cell).append(" " * (widths(col) - cell.length))
        if (col == widths.length - 1) {
          if (col < cols - 1) {
            result.append("...")
            if (row == 0) {
              result.append(s" ($cols total)")
            }
          }
          if (row + 1 < showRows) {
            result.append('\n')
          }
        }
        col += 1
      }
      row += 1
    }
    if (rows > showRows) {
      result.append(s"\n... ($rows total)")
    }
    result.toString()
  }
}

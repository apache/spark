// scalastyle:off license
/*
 Copyright 2012 David Hall

 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
// scalastyle:on license

package org.apache.spark.ml.linalg

import scala.util.hashing.MurmurHash3

/**
 * Equality, sparse hashing, and traversal adapted from Matrix.scala, CSCMatrix.scala, and
 * Tensor.scala in Breeze 2.1.0 to use Spark matrix storage.
 */
private[linalg] object MatrixUtils {

  def checkDimensions(matrix: Matrix): Unit = {
    require(matrix.numRows >= 0 && matrix.numCols >= 0,
      s"Matrix dimensions must be nonnegative: ${matrix.numRows} x ${matrix.numCols}")
  }

  private def columnMajor(matrix: SparseMatrix): SparseMatrix = {
    checkDimensions(matrix)
    // Breeze's transpose normalizes the storage, unlike its ordinary CSC wrapper.
    if (matrix.isTransposed) matrix.toSparseColMajor else matrix
  }

  def equals(left: Matrix, other: Any): Boolean = other match {
    case right: Matrix =>
      checkDimensions(left)
      checkDimensions(right)
      if (left.numRows != right.numRows || left.numCols != right.numCols) {
        return false
      }
      (left, right) match {
        case (leftSparse: SparseMatrix, rightSparse: SparseMatrix) =>
          val leftEntries = new SparseEntries(leftSparse)
          val rightEntries = new SparseEntries(rightSparse)
          while (leftEntries.hasNext && rightEntries.hasNext) {
            leftEntries.next()
            rightEntries.next()
            // Known Breeze quirk retained for compatibility: a trailing explicit zero is
            // compared by coordinate. This can make sparse equality non-transitive.
            while (leftEntries.value == 0.0 && leftEntries.hasNext) {
              leftEntries.next()
            }
            while (rightEntries.value == 0.0 && rightEntries.hasNext) {
              rightEntries.next()
            }
            if (leftEntries.row != rightEntries.row || leftEntries.col != rightEntries.col ||
                leftEntries.value != rightEntries.value) {
              return false
            }
          }
          remainingZeros(leftEntries) && remainingZeros(rightEntries)
        case (leftSparse: SparseMatrix, _) => equalsByElement(columnMajor(leftSparse), right)
        case (_, rightSparse: SparseMatrix) => equalsByElement(left, columnMajor(rightSparse))
        case _ => equalsByElement(left, right)
      }
    case _ => false
  }

  private def equalsByElement(left: Matrix, right: Matrix): Boolean = {
    var col = 0
    while (col < left.numCols) {
      var row = 0
      while (row < left.numRows) {
        if (left(row, col) != right(row, col)) {
          return false
        }
        row += 1
      }
      col += 1
    }
    true
  }

  private def remainingZeros(entries: SparseEntries): Boolean = {
    while (entries.hasNext) {
      entries.next()
      if (entries.value != 0.0) {
        return false
      }
    }
    true
  }

  def hashCode(matrix: SparseMatrix): Int = {
    val data = columnMajor(matrix).values
    var hash = 43
    var i = 0
    while (i < data.length) {
      val valueHash = data(i).##
      if (valueHash != 0) {
        hash = MurmurHash3.mix(hash, valueHash)
      }
      i += 1
    }
    hash
  }

  /** Traverses column ranges without allocating per-column iterators or per-entry tuples. */
  final class SparseEntries(matrix: SparseMatrix) {
    private val columns = columnMajor(matrix)
    private var nextCol = 0
    private var nextIndex = columns.colPtrs(0)
    private var currentCol = -1
    private var currentIndex = -1

    def hasNext: Boolean = {
      while (nextCol < columns.numCols && nextIndex >= columns.colPtrs(nextCol + 1)) {
        nextCol += 1
        if (nextCol < columns.numCols) {
          nextIndex = columns.colPtrs(nextCol)
        }
      }
      nextCol < columns.numCols
    }

    def next(): Unit = {
      currentCol = nextCol
      currentIndex = nextIndex
      nextIndex += 1
    }

    def row: Int = columns.rowIndices(currentIndex)
    def col: Int = currentCol
    def value: Double = columns.values(currentIndex)
  }
}

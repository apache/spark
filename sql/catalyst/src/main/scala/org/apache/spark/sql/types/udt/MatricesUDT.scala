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
package org.apache.spark.sql.types.udt

import scala.collection.mutable.{ArrayBuilder => MArrayBuilder}

import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}

import org.apache.spark.ml.linalg._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, UserDefinedType}

 /**
  * User Defined Transformations for Matrix
  *
  */

class MatricesUDT extends UserDefinedType[Matrix] {
  /** Underlying storage type for this UDT */
  override def sqlType: DataType = {
    ArrayType(ArrayType(DoubleType, containsNull = false), containsNull = false)
  }

  /**
    * Convert the user type to a SQL datum
    */
  override def serialize(matrix: Matrix): Any = {
    new GenericArrayData(matrix.toArray)
  }

  /**
    * Class object for the UserType
    */
  override def userClass: Class[Matrix] = classOf[Matrix]

  /** Convert a SQL datum to the user type */
  override def deserialize(datum: Any): Matrix = {
    datum match {
      case (numRows: Int, numCols: Int, entries: Iterable[(Int, Int, Double)]) =>
        toMatrix(numRows, numCols, entries)
      case (breeze: BM[Double]) => toMatrix(breeze)
    }
  }

    /**
     * Creates a Matrix instance from a breeze matrix.
     * @param breeze a breeze matrix
     * @return a Matrix instance
     */
  private def toMatrix(breeze: BM[Double]): Matrix = breeze match {
    case dm: BDM[Double] =>
      new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
    case sm: BSM[Double] =>
      // Spark-11507. work around breeze issue 479.
      val mat = if (sm.colPtrs.last != sm.data.length) {
        val matCopy = sm.copy
        matCopy.compact()
        matCopy
      } else {
        sm
      }
      // There is no isTranspose flag for sparse matrices in Breeze
      new SparseMatrix(mat.rows, mat.cols, mat.colPtrs, mat.rowIndices, mat.data)
    case _ =>
      throw new UnsupportedOperationException(
        s"Do not support conversion from type ${breeze.getClass.getName}.")
  }

   /**
     * Generate a `SparseMatrix` from Coordinate List (COO) format. Input must be an array of
     * (i, j, value) tuples. Entries that have duplicate values of i and j are
     * added together. Tuples where value is equal to zero will be omitted.
     * @param numRows number of rows of the matrix
     * @param numCols number of columns of the matrix
     * @param entries Array of (i, j, value) tuples
     * @return The corresponding `SparseMatrix`
     */
  private def toMatrix(numRows: Int,
                       numCols: Int,
                       entries: Iterable[(Int, Int, Double)]): Matrix = {
    val sortedEntries = entries.toSeq.sortBy(v => (v._2, v._1))
    val numEntries = sortedEntries.size
    if (sortedEntries.nonEmpty) {
      // Since the entries are sorted by column index, we only need to check the first and the last.
      for (col <- Seq(sortedEntries.head._2, sortedEntries.last._2)) {
        require(col >= 0 && col < numCols, s"Column index out of range [0, $numCols): $col.")
      }
    }
    val colPtrs = new Array[Int](numCols + 1)
    val rowIndices = MArrayBuilder.make[Int]
    rowIndices.sizeHint(numEntries)
    val values = MArrayBuilder.make[Double]
    values.sizeHint(numEntries)
    var nnz = 0
    var prevCol = 0
    var prevRow = -1
    var prevVal = 0.0
    // Append a dummy entry to include the last one at the end of the loop.
    (sortedEntries.view :+(numRows, numCols, 1.0)).foreach { case (i, j, v) =>
      if (v != 0) {
        if (i == prevRow && j == prevCol) {
          prevVal += v
        } else {
          if (prevVal != 0) {
            require(prevRow >= 0 && prevRow < numRows,
              s"Row index out of range [0, $numRows): $prevRow.")
            nnz += 1
            rowIndices += prevRow
            values += prevVal
          }
          prevRow = i
          prevVal = v
          while (prevCol < j) {
            colPtrs(prevCol + 1) = nnz
            prevCol += 1
          }
        }
      }
    }
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices.result(), values.result())
  }
}

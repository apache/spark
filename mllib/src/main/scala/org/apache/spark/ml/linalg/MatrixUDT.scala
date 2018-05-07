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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.types._

/**
 * User-defined type for [[Matrix]] in `mllib-local` which allows easy interaction with SQL
 * via [[org.apache.spark.sql.Dataset]].
 */
private[spark] class MatrixUDT extends UserDefinedType[Matrix] {

  override def sqlType: StructType = {
    // type: 0 = sparse, 1 = dense
    // the dense matrix is built by numRows, numCols, values and isTransposed, all of which are
    // set as not nullable, except values since in the future, support for binary matrices might
    // be added for which values are not needed.
    // the sparse matrix needs colPtrs and rowIndices, which are set as
    // null, while building the dense matrix.
    StructType(Seq(
      StructField("type", ByteType, nullable = false),
      StructField("numRows", IntegerType, nullable = false),
      StructField("numCols", IntegerType, nullable = false),
      StructField("colPtrs", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("rowIndices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true),
      StructField("isTransposed", BooleanType, nullable = false)
      ))
  }

  override def writeRow(obj: Matrix): Row = {
    obj match {
      case sm: SparseMatrix =>
        Row(0.toByte, sm.numRows, sm.numCols, sm.colPtrs, sm.rowIndices, sm.values, sm.isTransposed)

      case dm: DenseMatrix =>
        Row(1.toByte, dm.numRows, dm.numCols, null, null, dm.values, dm.isTransposed)
    }
  }

  override def readRow(row: Row): Matrix = {
    require(row.length == 7,
      s"MatrixUDT.deserialize given row with length ${row.length} but requires length == 7")
    val tpe = row.getByte(0)
    val numRows = row.getInt(1)
    val numCols = row.getInt(2)
    val values = row.getSeq[Double](5).toArray
    val isTransposed = row.getBoolean(6)
    tpe match {
      case 0 =>
        val colPtrs = row.getSeq[Int](3).toArray
        val rowIndices = row.getSeq[Int](4).toArray
        new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
      case 1 =>
        new DenseMatrix(numRows, numCols, values, isTransposed)
    }
  }

  override def userClass: Class[Matrix] = classOf[Matrix]

  override def equals(o: Any): Boolean = {
    o match {
      case v: MatrixUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[MatrixUDT].getName.hashCode()

  override def typeName: String = "matrix"

  override def pyUDT: String = "pyspark.ml.linalg.MatrixUDT"

  private[spark] override def asNullable: MatrixUDT = this
}

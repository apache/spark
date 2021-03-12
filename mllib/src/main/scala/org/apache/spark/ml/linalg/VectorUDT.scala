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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.types._

/**
 * User-defined type for [[Vector]] in [[mllib-local]] which allows easy interaction with SQL
 * via [[org.apache.spark.sql.Dataset]].
 */
private[spark] class VectorUDT extends UserDefinedType[Vector] {

  override final def sqlType: StructType = _sqlType

  override def serialize(obj: Vector): InternalRow = {
    obj match {
      case SparseVector(size, indices, values) =>
        val row = new GenericInternalRow(4)
        row.setByte(0, 0)
        row.setInt(1, size)
        row.update(2, UnsafeArrayData.fromPrimitiveArray(indices))
        row.update(3, UnsafeArrayData.fromPrimitiveArray(values))
        row
      case DenseVector(values) =>
        val row = new GenericInternalRow(4)
        row.setByte(0, 1)
        row.setNullAt(1)
        row.setNullAt(2)
        row.update(3, UnsafeArrayData.fromPrimitiveArray(values))
        row
    }
  }

  override def deserialize(datum: Any): Vector = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 4,
          s"VectorUDT.deserialize given row with length ${row.numFields} but requires length == 4")
        val tpe = row.getByte(0)
        tpe match {
          case 0 =>
            val size = row.getInt(1)
            val indices = row.getArray(2).toIntArray()
            val values = row.getArray(3).toDoubleArray()
            new SparseVector(size, indices, values)
          case 1 =>
            val values = row.getArray(3).toDoubleArray()
            new DenseVector(values)
        }
    }
  }

  override def pyUDT: String = "pyspark.ml.linalg.VectorUDT"

  override def userClass: Class[Vector] = classOf[Vector]

  override def equals(o: Any): Boolean = {
    o match {
      case v: VectorUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[VectorUDT].getName.hashCode()

  override def typeName: String = "vector"

  private[spark] override def asNullable: VectorUDT = this

  private[this] val _sqlType = {
    // type: 0 = sparse, 1 = dense
    // We only use "values" for dense vectors, and "size", "indices", and "values" for sparse
    // vectors. The "values" field is nullable because we might want to add binary vectors later,
    // which uses "size" and "indices", but not "values".
    StructType(Seq(
      StructField("type", ByteType, nullable = false),
      StructField("size", IntegerType, nullable = true),
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true)))
  }
}

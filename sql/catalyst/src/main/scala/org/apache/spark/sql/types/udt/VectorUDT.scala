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

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.json4s.{DefaultFormats, JValue}

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

/**
 * User Defined Transformations for Vectors
 *
 * Note: Currently Supports deserialization of Json and BEEze.
 */
class VectorUDT extends UserDefinedType[Vector] {
  /** Underlying storage type for this UDT */
  override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)

  /**
   * Convert the user type to a SQL datum
   */
  override def serialize(vector: Vector): ArrayData = {
    new GenericArrayData(vector match {
      case denseVector: DenseVector => denseVector.values
      case sparseVector: SparseVector => sparseVector.toDense.values
    })
  }

  /**
   * Class object for the UserType
   */
  override def userClass: Class[Vector] = classOf[Vector]

  /** Convert a SQL datum to the user type */
  override def deserialize(datum: Any): Vector = {
    implicit val formats = DefaultFormats
    datum match {
      case jValue: JValue => (jValue \ "type").extract[Int] match {
        case 0 => // sparse
          val size = (jValue \ "size").extract[Int]
          val indices = (jValue \ "indices").extract[Seq[Int]].toArray
          val values = (jValue \ "values").extract[Seq[Double]].toArray
          Vectors.sparse(size, indices, values)
        case 1 => // dense
          val values = (jValue \ "values").extract[Seq[Double]].toArray
          Vectors.dense(values)
        case _ =>
          throw new IllegalArgumentException(s"Cannot parse $json into a vector.")
      }

      case breezeVector: BV[Double] => breezeVector match {
        case v: BDV[Double] =>
          if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
            new DenseVector(v.data)
          } else {
            new DenseVector(v.toArray) // Can't use underlying array directly, so make a new one
          }
        case v: BSV[Double] =>
          if (v.index.length == v.used) {
            new SparseVector(v.length, v.index, v.data)
          } else {
            new SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
          }
        case _ =>
          sys.error("Unsupported Breeze vector type: " + datum.getClass.getName)
      }
    }
  }
}

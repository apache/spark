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

package org.apache.spark.sql.test

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{DataType, DoubleType, SQLUserDefinedType, StructField, StructType, UserDefinedType}

/**
 * An example class to demonstrate UDT with StructType in Scala and Python
 *
 * @param xmin x-coordinate of the top-left of the box.
 * @param ymin y-coordinate of the top-left of the box.
 * @param xmax x-coordinate of the bottom-right of the box.
 * @param ymax y-coordinate of the bottom-right of the box.
 */
@SQLUserDefinedType(udt = classOf[ExampleBoxUDT])
private[sql] class ExampleBox(val xmin: Double,
                              val ymin: Double,
                              val xmax: Double,
                              val ymax: Double) extends Serializable {

  override def hashCode(): Int = {
    var hash = 13
    hash = hash * 31 + xmin.hashCode()
    hash = hash * 31 + ymin.hashCode()
    hash = hash * 31 + xmax.hashCode()
    hash = hash * 31 + ymax.hashCode()
    hash
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: ExampleBox => xmin == that.xmin && ymin == that.ymin &&
        xmax == that.xmax && ymax == that.ymax
      case _ => false
    }
}

/**
 * User-defined type for [[ExampleBox]].
 */
private[sql] class ExampleBoxUDT extends UserDefinedType[ExampleBox] {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false)
    )
  )

  override def pyUDT: String = "pyspark.testing.sqlutils.ExampleBoxUDT"

  override def serialize(box: ExampleBox): InternalRow = {
    val row = new GenericInternalRow(4)
    row.setDouble(0, box.xmin)
    row.setDouble(1, box.ymin)
    row.setDouble(2, box.xmax)
    row.setDouble(3, box.ymax)
    row
  }

  override def deserialize(datum: Any): ExampleBox = {
    datum match {
      case row: InternalRow =>
        val xmin = row.getDouble(0)
        val ymin = row.getDouble(1)
        val xmax = row.getDouble(2)
        val ymax = row.getDouble(3)
        new ExampleBox(xmin, ymin, xmax, ymax)
    }
  }

  override def userClass: Class[ExampleBox] = classOf[ExampleBox]

  private[spark] override def asNullable: ExampleBoxUDT = this
}

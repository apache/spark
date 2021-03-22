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

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{DataType, DoubleType, SQLUserDefinedType, StructField, StructType, TimestampType, UserDefinedType}


/**
 * An example class to demonstrate UDT in Scala, Java, and Python.
 * @param x x coordinate
 * @param y y coordinate
 * @param ts timestamp
 */
@SQLUserDefinedType(udt = classOf[ExamplePointUDT])
private[sql] class ExamplePointWithTime(val x: Double, val y: Double, val ts: Timestamp)
  extends Serializable {

  override def hashCode(): Int = {
    var hash = 13
    hash = hash * 31 + x.hashCode()
    hash = hash * 31 + y.hashCode()
    hash = hash * 31 + ts.hashCode()
    hash
  }

  override def equals(other: Any): Boolean = other match {
    case that: ExamplePointWithTime =>
      this.x == that.x && this.y == that.y && this.ts == that.ts
    case _ => false
  }

  override def toString(): String = s"($x, $y, ${ts.toString})"
}

/**
 * User-defined type for [[ExamplePoint]].
 */
private[sql] class ExamplePointWithTimeUDT extends UserDefinedType[ExamplePointWithTime] {

  override def sqlType: DataType = StructType(Array(
    StructField("x", DoubleType, nullable = false),
    StructField("y", DoubleType, nullable = true),
    StructField("ts", TimestampType, nullable = false)
  ))

  override def pyUDT: String = "pyspark.testing.sqlutils.ExamplePointWithTimeUDT"

  override def serialize(p: ExamplePointWithTime): GenericArrayData = {
    val output = new Array[Any](3)
    output(0) = p.x
    output(1) = p.y
    output(2) = p.ts
    new GenericArrayData(output)
  }

  override def deserialize(datum: Any): ExamplePointWithTime = {
    datum match {
      case values: ArrayData =>
        new ExamplePointWithTime(
          values.getDouble(0),
          values.getDouble(1),
          values.get(2, TimestampType).asInstanceOf[Timestamp]
        )
    }
  }

  override def userClass: Class[ExamplePointWithTime] = classOf[ExamplePointWithTime]

  private[spark] override def asNullable: ExamplePointWithTimeUDT = this
}

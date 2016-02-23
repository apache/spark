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

import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayData}
import org.apache.spark.sql.types._

/**
 * An example class to demonstrate UDT in Scala, Java, and Python.
 * @param x x coordinate
 * @param y y coordinate
 */
@SQLUserDefinedType(udt = classOf[ExamplePointUDT])
private[sql] class ExamplePoint(val x: Double, val y: Double) extends Serializable {
  override def equals(other: Any): Boolean = other match {
    case that: ExamplePoint => this.x == that.x && this.y == that.y
    case _ => false
  }
}

/**
 * User-defined type for [[ExamplePoint]].
 */
private[sql] class ExamplePointUDT extends UserDefinedType[ExamplePoint] {

  override def sqlType: DataType = ArrayType(DoubleType, false)

  override def pyUDT: String = "pyspark.sql.tests.ExamplePointUDT"

  override def serialize(obj: Any): GenericArrayData = {
    obj match {
      case p: ExamplePoint =>
        val output = new Array[Any](2)
        output(0) = p.x
        output(1) = p.y
        new GenericArrayData(output)
    }
  }

  override def deserialize(datum: Any): ExamplePoint = {
    datum match {
      case values: ArrayData =>
        new ExamplePoint(values.getDouble(0), values.getDouble(1))
    }
  }

  override def userClass: Class[ExamplePoint] = classOf[ExamplePoint]

  private[spark] override def asNullable: ExamplePointUDT = this
}

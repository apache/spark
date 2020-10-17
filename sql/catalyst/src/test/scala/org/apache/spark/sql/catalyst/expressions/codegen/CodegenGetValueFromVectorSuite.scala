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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{DataTypes, LongType, StructField, StructType, UserDefinedType}

/**
 * A test suite that makes sure code generation handles expression internally states correctly.
 */
class CodegenGetValueFromVectorSuite extends SparkFunSuite {

  test("getValueFromVector should use the primitive value") {
    val result = CodeGenerator.getValueFromVector("vector", DataTypes.LongType, "0")
    assert(result == "vector.getLong(0)")
  }

  test("getValueFromVector should use the StructType") {
    val structType = StructType(Seq(
      StructField("str", DataTypes.StringType),
      StructField("num", DataTypes.LongType)))
    val result = CodeGenerator.getValueFromVector("vector", structType, "0")
    assert(result == "vector.getStruct(0)")
  }

  test("getValueFromVector should use the UDT sqlType") {
    val udtType = new LongWrapperUDT
    val result = CodeGenerator.getValueFromVector("vector", udtType, "0")
    assert(result == "vector.getStruct(0)")
  }

}

case class LongWrapper(value: Long)

private[spark] class LongWrapperUDT extends UserDefinedType[LongWrapper] {

  override final def sqlType: StructType = _sqlType

  override def serialize(obj: LongWrapper): InternalRow = {
    val row = new GenericInternalRow(1)
    row.setLong(0, obj.value)
    row
   }

  override def deserialize(datum: Any): LongWrapper = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 1,
          s"deserialize given row with length ${row.numFields} but requires length == 1")
        val tpe = row.getByte(0)
        LongWrapper(row.getLong(0))
    }
  }

  override def pyUDT: String = "should.not.be.used"

  override def userClass: Class[LongWrapper] = classOf[LongWrapper]

  override def equals(o: Any): Boolean = {
    o match {
      case lw: LongWrapperUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[LongWrapperUDT].getName.hashCode()

  override def typeName: String = "longWrapper"

  private[spark] override def asNullable: LongWrapperUDT = this

  private[this] val _sqlType = {
    StructType(Seq(StructField("value", LongType)))
  }
}

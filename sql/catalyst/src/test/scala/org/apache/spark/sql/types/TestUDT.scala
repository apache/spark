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

package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}


// Wrapped in an object to check Scala compatibility. See SPARK-13929
object TestUDT {

  @SQLUserDefinedType(udt = classOf[MyDenseVectorUDT])
  private[sql] class MyDenseVector(val data: Array[Double]) extends Serializable {
    override def hashCode(): Int = java.util.Arrays.hashCode(data)

    override def equals(other: Any): Boolean = other match {
      case v: MyDenseVector => java.util.Arrays.equals(this.data, v.data)
      case _ => false
    }

    override def toString: String = data.mkString("(", ", ", ")")
  }

  private[sql] class MyDenseVectorUDT extends UserDefinedType[MyDenseVector] {

    override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)

    override def serialize(features: MyDenseVector): ArrayData = {
      new GenericArrayData(features.data.map(_.asInstanceOf[Any]))
    }

    override def deserialize(datum: Any): MyDenseVector = {
      datum match {
        case data: ArrayData =>
          new MyDenseVector(data.toDoubleArray())
      }
    }

    override def userClass: Class[MyDenseVector] = classOf[MyDenseVector]

    private[spark] override def asNullable: MyDenseVectorUDT = this

    override def hashCode(): Int = getClass.hashCode()

    override def equals(other: Any): Boolean = other.isInstanceOf[MyDenseVectorUDT]
  }
}

// object and classes to test SPARK-19311

// Trait/Interface for base type
sealed trait IExampleBaseType extends Serializable {
  def field: Int
}

// Trait/Interface for derived type
sealed trait IExampleSubType extends IExampleBaseType

// a base class
class ExampleBaseClass(override val field: Int) extends IExampleBaseType

// a derived class
class ExampleSubClass(override val field: Int)
  extends ExampleBaseClass(field) with IExampleSubType

// UDT for base class
class ExampleBaseTypeUDT extends UserDefinedType[IExampleBaseType] {

  override def sqlType: StructType = {
    StructType(Seq(
      StructField("intfield", IntegerType, nullable = false)))
  }

  override def serialize(obj: IExampleBaseType): InternalRow = {
    val row = new GenericInternalRow(1)
    row.setInt(0, obj.field)
    row
  }

  override def deserialize(datum: Any): IExampleBaseType = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 1,
          "ExampleBaseTypeUDT requires row with length == 1")
        val field = row.getInt(0)
        new ExampleBaseClass(field)
    }
  }

  override def userClass: Class[IExampleBaseType] = classOf[IExampleBaseType]
}

// UDT for derived class
private[spark] class ExampleSubTypeUDT extends UserDefinedType[IExampleSubType] {

  override def sqlType: StructType = {
    StructType(Seq(
      StructField("intfield", IntegerType, nullable = false)))
  }

  override def serialize(obj: IExampleSubType): InternalRow = {
    val row = new GenericInternalRow(1)
    row.setInt(0, obj.field)
    row
  }

  override def deserialize(datum: Any): IExampleSubType = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 1,
          "ExampleSubTypeUDT requires row with length == 1")
        val field = row.getInt(0)
        new ExampleSubClass(field)
    }
  }

  override def userClass: Class[IExampleSubType] = classOf[IExampleSubType]
}

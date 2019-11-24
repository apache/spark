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

import java.util.GregorianCalendar
import javax.xml.datatype.{DatatypeFactory, XMLGregorianCalendar}

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

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

  private[sql] class MyXMLGregorianCalendarUDT extends UserDefinedType[XMLGregorianCalendar] {
    override def sqlType: DataType = TimestampType

    override def serialize(obj: XMLGregorianCalendar): Any =
      obj.toGregorianCalendar.getTimeInMillis * 1000

    override def deserialize(datum: Any): XMLGregorianCalendar = {
      val calendar = new GregorianCalendar
      calendar.setTimeInMillis(datum.asInstanceOf[Long])
      DatatypeFactory.newInstance.newXMLGregorianCalendar(calendar)
    }

    override def userClass: Class[XMLGregorianCalendar] = classOf[XMLGregorianCalendar]

    // By setting this to a timestamp, we lose the information about the udt
    override private[sql] def jsonValue: JValue = "timestamp"
  }
}

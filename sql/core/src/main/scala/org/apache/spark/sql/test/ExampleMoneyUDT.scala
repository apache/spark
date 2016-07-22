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

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

/**
 * An example class to demonstrate UDT in Scala, Java, and Python.
 * @param value x coordinate
 */
@SQLUserDefinedType(udt = classOf[ExampleMoneyUDT])
private[sql] class ExampleMoney(val value: Double) extends Serializable {

  override def hashCode(): Int = 31 * (31 * value.hashCode())

  override def equals(other: Any): Boolean = other match {
    case that: ExampleMoney => this.value == that.value
    case _ => false
  }
}

/**
 * User-defined type for [[ExampleMoney]].
 */
private[sql] class ExampleMoneyUDT extends UserDefinedType[ExampleMoney] {

  override def sqlType: DataType = DoubleType

  override def pyUDT: String = "pyspark.sql.tests.ExampleMoneyUDT"

  override def serialize(p: ExampleMoney): Double = {
    p.value
  }

  override def deserialize(datum: Any): ExampleMoney = {
    datum match {
      case value: Double =>
        new ExampleMoney(value)
    }
  }

  override def userClass: Class[ExampleMoney] = classOf[ExampleMoney]

  private[spark] override def asNullable: ExampleMoneyUDT = this
}

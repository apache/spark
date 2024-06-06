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

package org.apache.spark.sql

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.types._

private[sql] class TestUserClass {
}

private[sql] class TestUserClass2 {
}

private[sql] class TestUserClass3 {
}

private[sql] class NonUserDefinedType {
}

private[sql] class TestUserClassUDT extends UserDefinedType[TestUserClass] {

  override def sqlType: DataType = IntegerType
  override def serialize(input: TestUserClass): Int = 1

  override def deserialize(datum: Any): TestUserClass = new TestUserClass

  override def userClass: Class[TestUserClass] = classOf[TestUserClass]

  private[spark] override def asNullable: TestUserClassUDT = this

  override def hashCode(): Int = classOf[TestUserClassUDT].getName.hashCode()

  override def equals(other: Any): Boolean = other match {
    case _: TestUserClassUDT => true
    case _ => false
  }
}

class UDTRegistrationSuite extends SparkFunSuite {

  test("register non-UserDefinedType") {
    UDTRegistration.register(classOf[TestUserClass].getName,
      "org.apache.spark.sql.NonUserDefinedType")
    intercept[SparkException] {
      UDTRegistration.getUDTFor(classOf[TestUserClass].getName)
    }
  }

  test("default UDTs") {
    val userClasses = Seq(
    "org.apache.spark.ml.linalg.Vector",
    "org.apache.spark.ml.linalg.DenseVector",
    "org.apache.spark.ml.linalg.SparseVector",
    "org.apache.spark.ml.linalg.Matrix",
    "org.apache.spark.ml.linalg.DenseMatrix",
    "org.apache.spark.ml.linalg.SparseMatrix")
    userClasses.foreach { c =>
      assert(UDTRegistration.exists(c))
    }
  }

  test("query registered user class") {
    UDTRegistration.register(classOf[TestUserClass2].getName, classOf[TestUserClassUDT].getName)
    assert(UDTRegistration.exists(classOf[TestUserClass2].getName))
    assert(
      classOf[UserDefinedType[_]].isAssignableFrom((
        UDTRegistration.getUDTFor(classOf[TestUserClass2].getName).get)))
  }

  test("query unregistered user class") {
    assert(!UDTRegistration.exists(classOf[TestUserClass3].getName))
    assert(UDTRegistration.getUDTFor(classOf[TestUserClass3].getName).isEmpty)
  }
}

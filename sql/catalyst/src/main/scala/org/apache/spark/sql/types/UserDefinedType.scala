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

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

import org.apache.spark.annotation.DeveloperApi

/**
 * ::DeveloperApi::
 * The data type for User Defined Types (UDTs).
 *
 * This interface allows a user to make their own classes more interoperable with SparkSQL;
 * e.g., by creating a [[UserDefinedType]] for a class X, it becomes possible to create
 * a `DataFrame` which has class X in the schema.
 *
 * For SparkSQL to recognize UDTs, the UDT must be annotated with
 * [[SQLUserDefinedType]].
 *
 * The conversion via `serialize` occurs when instantiating a `DataFrame` from another RDD.
 * The conversion via `deserialize` occurs when reading from a `DataFrame`.
 */
@DeveloperApi
abstract class UserDefinedType[UserType] extends DataType with Serializable {

  /** Underlying storage type for this UDT */
  def sqlType: DataType

  /** Paired Python UDT class, if exists. */
  def pyUDT: String = null

  /** Serialized Python UDT class, if exists. */
  def serializedPyClass: String = null

  /**
   * Convert the user type to a SQL datum
   *
   * TODO: Can we make this take obj: UserType?  The issue is in
   *       CatalystTypeConverters.convertToCatalyst, where we need to convert Any to UserType.
   */
  def serialize(obj: Any): Any

  /** Convert a SQL datum to the user type */
  def deserialize(datum: Any): UserType

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("class" -> this.getClass.getName) ~
      ("pyClass" -> pyUDT) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  /**
   * Class object for the UserType
   */
  def userClass: java.lang.Class[UserType]

  /**
   * The default size of a value of the UserDefinedType is 4096 bytes.
   */
  override def defaultSize: Int = 4096

  /**
   * For UDT, asNullable will not change the nullability of its internal sqlType and just returns
   * itself.
   */
  override private[spark] def asNullable: UserDefinedType[UserType] = this

  override private[sql] def acceptsType(dataType: DataType) =
    this.getClass == dataType.getClass

  override def equals(other: Any): Boolean = other match {
    case that: UserDefinedType[_] => this.acceptsType(that)
    case _ => false
  }
}

/**
 * ::DeveloperApi::
 * The user defined type in Python.
 *
 * Note: This can only be accessed via Python UDF, or accessed as serialized object.
 */
private[sql] class PythonUserDefinedType(
    val sqlType: DataType,
    override val pyUDT: String,
    override val serializedPyClass: String) extends UserDefinedType[Any] {

  /* The serialization is handled by UDT class in Python */
  override def serialize(obj: Any): Any = obj
  override def deserialize(datam: Any): Any = datam

  /* There is no Java class for Python UDT */
  override def userClass: java.lang.Class[Any] = null

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("pyClass" -> pyUDT) ~
      ("serializedClass" -> serializedPyClass) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  override def equals(other: Any): Boolean = other match {
    case that: PythonUserDefinedType => this.pyUDT.equals(that.pyUDT)
    case _ => false
  }
}

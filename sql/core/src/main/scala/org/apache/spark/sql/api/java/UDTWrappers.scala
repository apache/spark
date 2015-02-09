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

package org.apache.spark.sql.api.java

import org.apache.spark.sql.catalyst.types.{UserDefinedType => ScalaUserDefinedType}
import org.apache.spark.sql.{DataType => ScalaDataType}
import org.apache.spark.sql.types.util.DataTypeConversions

/**
 * Scala wrapper for a Java UserDefinedType
 */
private[sql] class JavaToScalaUDTWrapper[UserType](val javaUDT: UserDefinedType[UserType])
  extends ScalaUserDefinedType[UserType] with Serializable {

  /** Underlying storage type for this UDT */
  val sqlType: ScalaDataType = DataTypeConversions.asScalaDataType(javaUDT.sqlType())

  /** Convert the user type to a SQL datum */
  def serialize(obj: Any): Any = javaUDT.serialize(obj)

  /** Convert a SQL datum to the user type */
  def deserialize(datum: Any): UserType = javaUDT.deserialize(datum)

  val userClass: java.lang.Class[UserType] = javaUDT.userClass()
}

/**
 * Java wrapper for a Scala UserDefinedType
 */
private[sql] class ScalaToJavaUDTWrapper[UserType](val scalaUDT: ScalaUserDefinedType[UserType])
  extends UserDefinedType[UserType] with Serializable {

  /** Underlying storage type for this UDT */
  val sqlType: DataType = DataTypeConversions.asJavaDataType(scalaUDT.sqlType)

  /** Convert the user type to a SQL datum */
  def serialize(obj: Any): java.lang.Object = scalaUDT.serialize(obj).asInstanceOf[java.lang.Object]

  /** Convert a SQL datum to the user type */
  def deserialize(datum: Any): UserType = scalaUDT.deserialize(datum)

  val userClass: java.lang.Class[UserType] = scalaUDT.userClass
}

private[sql] object UDTWrappers {

  def wrapAsScala(udtType: UserDefinedType[_]): ScalaUserDefinedType[_] = {
    udtType match {
      case t: ScalaToJavaUDTWrapper[_] => t.scalaUDT
      case _ => new JavaToScalaUDTWrapper(udtType)
    }
  }

  def wrapAsJava(udtType: ScalaUserDefinedType[_]): UserDefinedType[_] = {
    udtType match {
      case t: JavaToScalaUDTWrapper[_] => t.javaUDT
      case _ => new ScalaToJavaUDTWrapper(udtType)
    }
  }
}

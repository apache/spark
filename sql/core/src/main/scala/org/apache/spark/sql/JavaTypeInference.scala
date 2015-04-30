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

import java.beans.Introspector
import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator, Map => JMap}

import com.google.common.reflect.TypeToken

import org.apache.spark.sql.types._

import scala.language.existentials

/**
 * Type-inference utilities for POJOs and Java collections.
 */
private [sql] object JavaTypeInference {

  private val iterableType = TypeToken.of(classOf[JIterable[_]])
  private val mapType = TypeToken.of(classOf[JMap[_, _]])
  private val iteratorReturnType = classOf[JIterable[_]].getMethod("iterator").getGenericReturnType
  private val nextReturnType = classOf[JIterator[_]].getMethod("next").getGenericReturnType
  private val keySetReturnType = classOf[JMap[_, _]].getMethod("keySet").getGenericReturnType
  private val valuesReturnType = classOf[JMap[_, _]].getMethod("values").getGenericReturnType

  /**
   * Infers the corresponding SQL data type of a Java type.
   * @param typeToken Java type
   * @return (SQL data type, nullable)
   */
  private [sql] def inferDataType(typeToken: TypeToken[_]): (DataType, Boolean) = {
    // TODO: All of this could probably be moved to Catalyst as it is mostly not Spark specific.
    typeToken.getRawType match {
      case c: Class[_] if c.isAnnotationPresent(classOf[SQLUserDefinedType]) =>
        (c.getAnnotation(classOf[SQLUserDefinedType]).udt().newInstance(), true)

      case c: Class[_] if c == classOf[java.lang.String] => (StringType, true)
      case c: Class[_] if c == java.lang.Short.TYPE => (ShortType, false)
      case c: Class[_] if c == java.lang.Integer.TYPE => (IntegerType, false)
      case c: Class[_] if c == java.lang.Long.TYPE => (LongType, false)
      case c: Class[_] if c == java.lang.Double.TYPE => (DoubleType, false)
      case c: Class[_] if c == java.lang.Byte.TYPE => (ByteType, false)
      case c: Class[_] if c == java.lang.Float.TYPE => (FloatType, false)
      case c: Class[_] if c == java.lang.Boolean.TYPE => (BooleanType, false)

      case c: Class[_] if c == classOf[java.lang.Short] => (ShortType, true)
      case c: Class[_] if c == classOf[java.lang.Integer] => (IntegerType, true)
      case c: Class[_] if c == classOf[java.lang.Long] => (LongType, true)
      case c: Class[_] if c == classOf[java.lang.Double] => (DoubleType, true)
      case c: Class[_] if c == classOf[java.lang.Byte] => (ByteType, true)
      case c: Class[_] if c == classOf[java.lang.Float] => (FloatType, true)
      case c: Class[_] if c == classOf[java.lang.Boolean] => (BooleanType, true)

      case c: Class[_] if c == classOf[java.math.BigDecimal] => (DecimalType(), true)
      case c: Class[_] if c == classOf[java.sql.Date] => (DateType, true)
      case c: Class[_] if c == classOf[java.sql.Timestamp] => (TimestampType, true)

      case _ if typeToken.isArray =>
        val (dataType, nullable) = inferDataType(typeToken.getComponentType)
        (ArrayType(dataType, nullable), true)

      case _ if iterableType.isAssignableFrom(typeToken) =>
        val (dataType, nullable) = inferDataType(elementType(typeToken))
        (ArrayType(dataType, nullable), true)

      case _ if mapType.isAssignableFrom(typeToken) =>
        val typeToken2 = typeToken.asInstanceOf[TypeToken[_ <: JMap[_, _]]]
        val mapSupertype = typeToken2.getSupertype(classOf[JMap[_, _]])
        val keyType = elementType(mapSupertype.resolveType(keySetReturnType))
        val valueType = elementType(mapSupertype.resolveType(valuesReturnType))
        val (keyDataType, _) = inferDataType(keyType)
        val (valueDataType, nullable) = inferDataType(valueType)
        (MapType(keyDataType, valueDataType, nullable), true)

      case _ =>
        val beanInfo = Introspector.getBeanInfo(typeToken.getRawType)
        val properties = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
        val fields = properties.map { property =>
          val returnType = typeToken.method(property.getReadMethod).getReturnType
          val (dataType, nullable) = inferDataType(returnType)
          new StructField(property.getName, dataType, nullable)
        }
        (new StructType(fields), true)
    }
  }

  private def elementType(typeToken: TypeToken[_]): TypeToken[_] = {
    val typeToken2 = typeToken.asInstanceOf[TypeToken[_ <: JIterable[_]]]
    val iterableSupertype = typeToken2.getSupertype(classOf[JIterable[_]])
    val iteratorType = iterableSupertype.resolveType(iteratorReturnType)
    val itemType = iteratorType.resolveType(nextReturnType)
    itemType
  }
}

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

package org.apache.spark.sql.catalyst

import java.beans.{PropertyDescriptor, Introspector}
import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator, Map => JMap, List => JList}

import scala.language.existentials

import com.google.common.reflect.TypeToken

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayBasedMapData, DateTimeUtils}
import org.apache.spark.unsafe.types.UTF8String


/**
 * Type-inference utilities for POJOs and Java collections.
 */
object JavaTypeInference {

  private val iterableType = TypeToken.of(classOf[JIterable[_]])
  private val mapType = TypeToken.of(classOf[JMap[_, _]])
  private val listType = TypeToken.of(classOf[JList[_]])
  private val iteratorReturnType = classOf[JIterable[_]].getMethod("iterator").getGenericReturnType
  private val nextReturnType = classOf[JIterator[_]].getMethod("next").getGenericReturnType
  private val keySetReturnType = classOf[JMap[_, _]].getMethod("keySet").getGenericReturnType
  private val valuesReturnType = classOf[JMap[_, _]].getMethod("values").getGenericReturnType

  /**
   * Infers the corresponding SQL data type of a JavaBean class.
   * @param beanClass Java type
   * @return (SQL data type, nullable)
   */
  def inferDataType(beanClass: Class[_]): (DataType, Boolean) = {
    inferDataType(TypeToken.of(beanClass))
  }

  /**
   * Infers the corresponding SQL data type of a Java type.
   * @param typeToken Java type
   * @return (SQL data type, nullable)
   */
  private def inferDataType(typeToken: TypeToken[_]): (DataType, Boolean) = {
    typeToken.getRawType match {
      case c: Class[_] if c.isAnnotationPresent(classOf[SQLUserDefinedType]) =>
        (c.getAnnotation(classOf[SQLUserDefinedType]).udt().newInstance(), true)

      case c: Class[_] if c == classOf[java.lang.String] => (StringType, true)
      case c: Class[_] if c == classOf[Array[Byte]] => (BinaryType, true)

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

      case c: Class[_] if c == classOf[java.math.BigDecimal] => (DecimalType.SYSTEM_DEFAULT, true)
      case c: Class[_] if c == classOf[java.sql.Date] => (DateType, true)
      case c: Class[_] if c == classOf[java.sql.Timestamp] => (TimestampType, true)

      case _ if typeToken.isArray =>
        val (dataType, nullable) = inferDataType(typeToken.getComponentType)
        (ArrayType(dataType, nullable), true)

      case _ if iterableType.isAssignableFrom(typeToken) =>
        val (dataType, nullable) = inferDataType(elementType(typeToken))
        (ArrayType(dataType, nullable), true)

      case _ if mapType.isAssignableFrom(typeToken) =>
        val (keyType, valueType) = mapKeyValueType(typeToken)
        val (keyDataType, _) = inferDataType(keyType)
        val (valueDataType, nullable) = inferDataType(valueType)
        (MapType(keyDataType, valueDataType, nullable), true)

      case _ =>
        // TODO: we should only collect properties that have getter and setter. However, some tests
        // pass in scala case class as java bean class which doesn't have getter and setter.
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

  private def getJavaBeanProperties(beanClass: Class[_]): Array[PropertyDescriptor] = {
    val beanInfo = Introspector.getBeanInfo(beanClass)
    beanInfo.getPropertyDescriptors
      .filter(p => p.getReadMethod != null && p.getWriteMethod != null)
  }

  private def elementType(typeToken: TypeToken[_]): TypeToken[_] = {
    val typeToken2 = typeToken.asInstanceOf[TypeToken[_ <: JIterable[_]]]
    val iterableSuperType = typeToken2.getSupertype(classOf[JIterable[_]])
    val iteratorType = iterableSuperType.resolveType(iteratorReturnType)
    iteratorType.resolveType(nextReturnType)
  }

  private def mapKeyValueType(typeToken: TypeToken[_]): (TypeToken[_], TypeToken[_]) = {
    val typeToken2 = typeToken.asInstanceOf[TypeToken[_ <: JMap[_, _]]]
    val mapSuperType = typeToken2.getSupertype(classOf[JMap[_, _]])
    val keyType = elementType(mapSuperType.resolveType(keySetReturnType))
    val valueType = elementType(mapSuperType.resolveType(valuesReturnType))
    keyType -> valueType
  }

  /**
   * Returns the Spark SQL DataType for a given java class.  Where this is not an exact mapping
   * to a native type, an ObjectType is returned.
   *
   * Unlike `inferDataType`, this function doesn't do any massaging of types into the Spark SQL type
   * system.  As a result, ObjectType will be returned for things like boxed Integers.
   */
  private def inferExternalType(cls: Class[_]): DataType = cls match {
    case c if c == java.lang.Boolean.TYPE => BooleanType
    case c if c == java.lang.Byte.TYPE => ByteType
    case c if c == java.lang.Short.TYPE => ShortType
    case c if c == java.lang.Integer.TYPE => IntegerType
    case c if c == java.lang.Long.TYPE => LongType
    case c if c == java.lang.Float.TYPE => FloatType
    case c if c == java.lang.Double.TYPE => DoubleType
    case c if c == classOf[Array[Byte]] => BinaryType
    case _ => ObjectType(cls)
  }

  /**
   * Returns an expression that can be used to construct an object of java bean `T` given an input
   * row with a compatible schema.  Fields of the row will be extracted using UnresolvedAttributes
   * of the same name as the constructor arguments.  Nested classes will have their fields accessed
   * using UnresolvedExtractValue.
   */
  def constructorFor(beanClass: Class[_]): Expression = {
    constructorFor(TypeToken.of(beanClass), None)
  }

  private def constructorFor(typeToken: TypeToken[_], path: Option[Expression]): Expression = {
    /** Returns the current path with a sub-field extracted. */
    def addToPath(part: String): Expression = path
      .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
      .getOrElse(UnresolvedAttribute(part))

    /** Returns the current path or `BoundReference`. */
    def getPath: Expression = path.getOrElse(BoundReference(0, inferDataType(typeToken)._1, true))

    typeToken.getRawType match {
      case c if !inferExternalType(c).isInstanceOf[ObjectType] => getPath

      case c if c == classOf[java.lang.Short] =>
        NewInstance(c, getPath :: Nil, ObjectType(c))
      case c if c == classOf[java.lang.Integer] =>
        NewInstance(c, getPath :: Nil, ObjectType(c))
      case c if c == classOf[java.lang.Long] =>
        NewInstance(c, getPath :: Nil, ObjectType(c))
      case c if c == classOf[java.lang.Double] =>
        NewInstance(c, getPath :: Nil, ObjectType(c))
      case c if c == classOf[java.lang.Byte] =>
        NewInstance(c, getPath :: Nil, ObjectType(c))
      case c if c == classOf[java.lang.Float] =>
        NewInstance(c, getPath :: Nil, ObjectType(c))
      case c if c == classOf[java.lang.Boolean] =>
        NewInstance(c, getPath :: Nil, ObjectType(c))

      case c if c == classOf[java.sql.Date] =>
        StaticInvoke(
          DateTimeUtils.getClass,
          ObjectType(c),
          "toJavaDate",
          getPath :: Nil,
          propagateNull = true)

      case c if c == classOf[java.sql.Timestamp] =>
        StaticInvoke(
          DateTimeUtils.getClass,
          ObjectType(c),
          "toJavaTimestamp",
          getPath :: Nil,
          propagateNull = true)

      case c if c == classOf[java.lang.String] =>
        Invoke(getPath, "toString", ObjectType(classOf[String]))

      case c if c == classOf[java.math.BigDecimal] =>
        Invoke(getPath, "toJavaBigDecimal", ObjectType(classOf[java.math.BigDecimal]))

      case c if c.isArray =>
        val elementType = c.getComponentType
        val primitiveMethod = elementType match {
          case c if c == java.lang.Boolean.TYPE => Some("toBooleanArray")
          case c if c == java.lang.Byte.TYPE => Some("toByteArray")
          case c if c == java.lang.Short.TYPE => Some("toShortArray")
          case c if c == java.lang.Integer.TYPE => Some("toIntArray")
          case c if c == java.lang.Long.TYPE => Some("toLongArray")
          case c if c == java.lang.Float.TYPE => Some("toFloatArray")
          case c if c == java.lang.Double.TYPE => Some("toDoubleArray")
          case _ => None
        }

        primitiveMethod.map { method =>
          Invoke(getPath, method, ObjectType(c))
        }.getOrElse {
          Invoke(
            MapObjects(
              p => constructorFor(typeToken.getComponentType, Some(p)),
              getPath,
              inferDataType(elementType)._1),
            "array",
            ObjectType(c))
        }

      case c if listType.isAssignableFrom(typeToken) =>
        val et = elementType(typeToken)
        val array =
          Invoke(
            MapObjects(
              p => constructorFor(et, Some(p)),
              getPath,
              inferDataType(et)._1),
            "array",
            ObjectType(classOf[Array[Any]]))

        StaticInvoke(classOf[java.util.Arrays], ObjectType(c), "asList", array :: Nil)

      case _ if mapType.isAssignableFrom(typeToken) =>
        val (keyType, valueType) = mapKeyValueType(typeToken)
        val keyDataType = inferDataType(keyType)._1
        val valueDataType = inferDataType(valueType)._1

        val keyData =
          Invoke(
            MapObjects(
              p => constructorFor(keyType, Some(p)),
              Invoke(getPath, "keyArray", ArrayType(keyDataType)),
              keyDataType),
            "array",
            ObjectType(classOf[Array[Any]]))

        val valueData =
          Invoke(
            MapObjects(
              p => constructorFor(valueType, Some(p)),
              Invoke(getPath, "valueArray", ArrayType(valueDataType)),
              valueDataType),
            "array",
            ObjectType(classOf[Array[Any]]))

        StaticInvoke(
          ArrayBasedMapData.getClass,
          ObjectType(classOf[JMap[_, _]]),
          "toJavaMap",
          keyData :: valueData :: Nil)

      case other =>
        val properties = getJavaBeanProperties(other)
        assert(properties.length > 0)

        val setters = properties.map { p =>
          val fieldName = p.getName
          val fieldType = typeToken.method(p.getReadMethod).getReturnType
          val (_, nullable) = inferDataType(fieldType)
          val constructor = constructorFor(fieldType, Some(addToPath(fieldName)))
          val setter = if (nullable) {
            constructor
          } else {
            AssertNotNull(constructor, other.getName, fieldName, fieldType.toString)
          }
          p.getWriteMethod.getName -> setter
        }.toMap

        val newInstance = NewInstance(other, Nil, ObjectType(other), propagateNull = false)
        val result = InitializeJavaBean(newInstance, setters)

        if (path.nonEmpty) {
          expressions.If(
            IsNull(getPath),
            expressions.Literal.create(null, ObjectType(other)),
            result
          )
        } else {
          result
        }
    }
  }

  /**
   * Returns expressions for extracting all the fields from the given type.
   */
  def extractorsFor(beanClass: Class[_]): CreateNamedStruct = {
    val inputObject = BoundReference(0, ObjectType(beanClass), nullable = true)
    extractorFor(inputObject, TypeToken.of(beanClass)).asInstanceOf[CreateNamedStruct]
  }

  private def extractorFor(inputObject: Expression, typeToken: TypeToken[_]): Expression = {

    def toCatalystArray(input: Expression, elementType: TypeToken[_]): Expression = {
      val (dataType, nullable) = inferDataType(elementType)
      if (ScalaReflection.isNativeType(dataType)) {
        NewInstance(
          classOf[GenericArrayData],
          input :: Nil,
          dataType = ArrayType(dataType, nullable))
      } else {
        MapObjects(extractorFor(_, elementType), input, ObjectType(elementType.getRawType))
      }
    }

    if (!inputObject.dataType.isInstanceOf[ObjectType]) {
      inputObject
    } else {
      typeToken.getRawType match {
        case c if c == classOf[String] =>
          StaticInvoke(
            classOf[UTF8String],
            StringType,
            "fromString",
            inputObject :: Nil)

        case c if c == classOf[java.sql.Timestamp] =>
          StaticInvoke(
            DateTimeUtils.getClass,
            TimestampType,
            "fromJavaTimestamp",
            inputObject :: Nil)

        case c if c == classOf[java.sql.Date] =>
          StaticInvoke(
            DateTimeUtils.getClass,
            DateType,
            "fromJavaDate",
            inputObject :: Nil)

        case c if c == classOf[java.math.BigDecimal] =>
          StaticInvoke(
            Decimal.getClass,
            DecimalType.SYSTEM_DEFAULT,
            "apply",
            inputObject :: Nil)

        case c if c == classOf[java.lang.Boolean] =>
          Invoke(inputObject, "booleanValue", BooleanType)
        case c if c == classOf[java.lang.Byte] =>
          Invoke(inputObject, "byteValue", ByteType)
        case c if c == classOf[java.lang.Short] =>
          Invoke(inputObject, "shortValue", ShortType)
        case c if c == classOf[java.lang.Integer] =>
          Invoke(inputObject, "intValue", IntegerType)
        case c if c == classOf[java.lang.Long] =>
          Invoke(inputObject, "longValue", LongType)
        case c if c == classOf[java.lang.Float] =>
          Invoke(inputObject, "floatValue", FloatType)
        case c if c == classOf[java.lang.Double] =>
          Invoke(inputObject, "doubleValue", DoubleType)

        case _ if typeToken.isArray =>
          toCatalystArray(inputObject, typeToken.getComponentType)

        case _ if listType.isAssignableFrom(typeToken) =>
          toCatalystArray(inputObject, elementType(typeToken))

        case _ if mapType.isAssignableFrom(typeToken) =>
          // TODO: for java map, if we get the keys and values by `keySet` and `values`, we can
          // not guarantee they have same iteration order(which is different from scala map).
          // A possible solution is creating a new `MapObjects` that can iterate a map directly.
          throw new UnsupportedOperationException("map type is not supported currently")

        case other =>
          val properties = getJavaBeanProperties(other)
          if (properties.length > 0) {
            CreateNamedStruct(properties.flatMap { p =>
              val fieldName = p.getName
              val fieldType = typeToken.method(p.getReadMethod).getReturnType
              val fieldValue = Invoke(
                inputObject,
                p.getReadMethod.getName,
                inferExternalType(fieldType.getRawType))
              expressions.Literal(fieldName) :: extractorFor(fieldValue, fieldType) :: Nil
            })
          } else {
            throw new UnsupportedOperationException(
              s"Cannot infer type for class ${other.getName} because it is not bean-compliant")
          }
      }
    }
  }
}

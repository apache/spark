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

import java.beans.{Introspector, PropertyDescriptor}
import java.lang.{Iterable => JIterable}
import java.lang.reflect.Type
import java.util.{Iterator => JIterator, List => JList, Map => JMap}

import scala.language.existentials

import com.google.common.reflect.TypeToken

import org.apache.spark.sql.catalyst.DeserializerBuildHelper._
import org.apache.spark.sql.catalyst.SerializerBuildHelper._
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.types._

/**
 * Type-inference utilities for POJOs and Java collections.
 */
object JavaTypeInference {

  private val iterableType = TypeToken.of(classOf[JIterable[_]])
  val mapType = TypeToken.of(classOf[JMap[_, _]])
  val listType = TypeToken.of(classOf[JList[_]])
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
   * @param beanType Java type
   * @return (SQL data type, nullable)
   */
  private[sql] def inferDataType(beanType: Type): (DataType, Boolean) = {
    inferDataType(TypeToken.of(beanType))
  }

  /**
   * Infers the corresponding SQL data type of a Java type.
   * @param typeToken Java type
   * @return (SQL data type, nullable)
   */
  def inferDataType(typeToken: TypeToken[_], seenTypeSet: Set[Class[_]] = Set.empty)
    : (DataType, Boolean) = {
    typeToken.getRawType match {
      case c: Class[_] if c.isAnnotationPresent(classOf[SQLUserDefinedType]) =>
        (c.getAnnotation(classOf[SQLUserDefinedType]).udt().getConstructor().newInstance(), true)

      case c: Class[_] if UDTRegistration.exists(c.getName) =>
        val udt = UDTRegistration.getUDTFor(c.getName).get.getConstructor().newInstance()
          .asInstanceOf[UserDefinedType[_ >: Null]]
        (udt, true)

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
      case c: Class[_] if c == classOf[java.math.BigInteger] => (DecimalType.BigIntDecimal, true)
      case c: Class[_] if c == classOf[java.sql.Date] => (DateType, true)
      case c: Class[_] if c == classOf[java.sql.Timestamp] => (TimestampType, true)

      case _ if typeToken.isArray =>
        val (dataType, nullable) = inferDataType(typeToken.getComponentType, seenTypeSet)
        (ArrayType(dataType, nullable), true)

      case _ if iterableType.isAssignableFrom(typeToken) =>
        val (dataType, nullable) = inferDataType(elementType(typeToken), seenTypeSet)
        (ArrayType(dataType, nullable), true)

      case _ if mapType.isAssignableFrom(typeToken) =>
        val (keyType, valueType) = mapKeyValueType(typeToken)
        val (keyDataType, _) = inferDataType(keyType, seenTypeSet)
        val (valueDataType, nullable) = inferDataType(valueType, seenTypeSet)
        (MapType(keyDataType, valueDataType, nullable), true)

      case other if other.isEnum =>
        (StringType, true)

      case other =>
        if (seenTypeSet.contains(other)) {
          throw new UnsupportedOperationException(
            "Cannot have circular references in bean class, but got the circular reference " +
              s"of class $other")
        }

        // TODO: we should only collect properties that have getter and setter. However, some tests
        // pass in scala case class as java bean class which doesn't have getter and setter.
        val properties = getJavaBeanReadableProperties(other)
        val fields = properties.map { property =>
          val returnType = typeToken.method(property.getReadMethod).getReturnType
          val (dataType, nullable) = inferDataType(returnType, seenTypeSet + other)
          new StructField(property.getName, dataType, nullable)
        }
        (new StructType(fields), true)
    }
  }

  def getJavaBeanReadableProperties(beanClass: Class[_]): Array[PropertyDescriptor] = {
    val beanInfo = Introspector.getBeanInfo(beanClass)
    beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
      .filterNot(_.getName == "declaringClass")
      .filter(_.getReadMethod != null)
  }

  def getJavaBeanReadableAndWritableProperties(
      beanClass: Class[_]): Array[PropertyDescriptor] = {
    getJavaBeanReadableProperties(beanClass).filter(_.getWriteMethod != null)
  }

  def elementType(typeToken: TypeToken[_]): TypeToken[_] = {
    val typeToken2 = typeToken.asInstanceOf[TypeToken[_ <: JIterable[_]]]
    val iterableSuperType = typeToken2.getSupertype(classOf[JIterable[_]])
    val iteratorType = iterableSuperType.resolveType(iteratorReturnType)
    iteratorType.resolveType(nextReturnType)
  }

  def mapKeyValueType(typeToken: TypeToken[_]): (TypeToken[_], TypeToken[_]) = {
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
  def inferExternalType(cls: Class[_]): DataType = cls match {
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
   * Returns an expression that can be used to deserialize a Spark SQL representation to an object
   * of java bean `T` with a compatible schema.  The Spark SQL representation is located at ordinal
   * 0 of a row, i.e., `GetColumnByOrdinal(0, _)`. Nested classes will have their fields accessed
   * using `UnresolvedExtractValue`.
   */
  def deserializerFor(beanClass: Class[_]): Expression = {
    val typeToken = TypeToken.of(beanClass)
    val walkedTypePath = new WalkedTypePath().recordRoot(beanClass.getCanonicalName)
    val (dataType, nullable) = inferDataType(typeToken)

    // Assumes we are deserializing the first column of a row.
    deserializerForWithNullSafetyAndUpcast(GetColumnByOrdinal(0, dataType), dataType,
      nullable = nullable, walkedTypePath, (casted, walkedTypePath) => {
        TypeInference.deserializerFor(JavaInference(typeToken), casted, walkedTypePath)
      })
  }

  /**
   * Returns an expression for serializing an object of the given type to a Spark SQL
   * representation. The input object is located at ordinal 0 of a row, i.e.,
   * `BoundReference(0, _)`.
   */
  def serializerFor(beanClass: Class[_]): Expression = {
    val inputObject = BoundReference(0, ObjectType(beanClass), nullable = true)
    val nullSafeInput = AssertNotNull(inputObject, Seq("top level input bean"))
    serializerFor(nullSafeInput, TypeToken.of(beanClass))
  }

  private def serializerFor(inputObject: Expression, typeToken: TypeToken[_]): Expression = {

    def toCatalystArray(input: Expression, elementType: TypeToken[_]): Expression = {
      val (dataType, nullable) = inferDataType(elementType)
      if (ScalaReflection.isNativeType(dataType)) {
        val cls = input.dataType.asInstanceOf[ObjectType].cls
        if (cls.isArray && cls.getComponentType.isPrimitive) {
          createSerializerForPrimitiveArray(input, dataType)
        } else {
          createSerializerForGenericArray(input, dataType, nullable = nullable)
        }
      } else {
        createSerializerForMapObjects(input, ObjectType(elementType.getRawType),
          serializerFor(_, elementType))
      }
    }

    if (!inputObject.dataType.isInstanceOf[ObjectType]) {
      inputObject
    } else {
      typeToken.getRawType match {
        case c if c == classOf[String] => createSerializerForString(inputObject)

        case c if c == classOf[java.time.Instant] => createSerializerForJavaInstant(inputObject)

        case c if c == classOf[java.sql.Timestamp] => createSerializerForSqlTimestamp(inputObject)

        case c if c == classOf[java.time.LocalDate] => createSerializerForJavaLocalDate(inputObject)

        case c if c == classOf[java.sql.Date] => createSerializerForSqlDate(inputObject)

        case c if c == classOf[java.math.BigDecimal] =>
          createSerializerForJavaBigDecimal(inputObject)

        case c if c == classOf[java.lang.Boolean] => createSerializerForBoolean(inputObject)
        case c if c == classOf[java.lang.Byte] => createSerializerForByte(inputObject)
        case c if c == classOf[java.lang.Short] => createSerializerForShort(inputObject)
        case c if c == classOf[java.lang.Integer] => createSerializerForInteger(inputObject)
        case c if c == classOf[java.lang.Long] => createSerializerForLong(inputObject)
        case c if c == classOf[java.lang.Float] => createSerializerForFloat(inputObject)
        case c if c == classOf[java.lang.Double] => createSerializerForDouble(inputObject)

        case _ if typeToken.isArray =>
          toCatalystArray(inputObject, typeToken.getComponentType)

        case _ if listType.isAssignableFrom(typeToken) =>
          toCatalystArray(inputObject, elementType(typeToken))

        case _ if mapType.isAssignableFrom(typeToken) =>
          val (keyType, valueType) = mapKeyValueType(typeToken)

          createSerializerForMap(
            inputObject,
            MapElementInformation(
              ObjectType(keyType.getRawType),
              nullable = true,
              serializerFor(_, keyType)),
            MapElementInformation(
              ObjectType(valueType.getRawType),
              nullable = true,
              serializerFor(_, valueType))
          )

        case other if other.isEnum =>
          createSerializerForString(
            Invoke(inputObject, "name", ObjectType(classOf[String]), returnNullable = false))

        case other =>
          val properties = getJavaBeanReadableAndWritableProperties(other)
          val fields = properties.map { p =>
            val fieldName = p.getName
            val fieldType = typeToken.method(p.getReadMethod).getReturnType
            val fieldValue = Invoke(
              inputObject,
              p.getReadMethod.getName,
              inferExternalType(fieldType.getRawType))
            (fieldName, serializerFor(fieldValue, fieldType))
          }
          createSerializerForObject(inputObject, fields)
      }
    }
  }
}

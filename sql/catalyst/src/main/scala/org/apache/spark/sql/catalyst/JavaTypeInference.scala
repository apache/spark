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
import java.lang.reflect.{ParameterizedType, Type}
import java.util.{ArrayDeque, List => JList, Map => JMap}
import javax.annotation.Nonnull

import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{ArrayEncoder, BinaryEncoder, BoxedBooleanEncoder, BoxedByteEncoder, BoxedDoubleEncoder, BoxedFloatEncoder, BoxedIntEncoder, BoxedLongEncoder, BoxedShortEncoder, DayTimeIntervalEncoder, DEFAULT_JAVA_DECIMAL_ENCODER, EncoderField, IterableEncoder, JavaBeanEncoder, JavaBigIntEncoder, JavaEnumEncoder, LocalDateTimeEncoder, MapEncoder, PrimitiveBooleanEncoder, PrimitiveByteEncoder, PrimitiveDoubleEncoder, PrimitiveFloatEncoder, PrimitiveIntEncoder, PrimitiveLongEncoder, PrimitiveShortEncoder, STRICT_DATE_ENCODER, STRICT_INSTANT_ENCODER, STRICT_LOCAL_DATE_ENCODER, STRICT_TIMESTAMP_ENCODER, StringEncoder, UDTEncoder, YearMonthIntervalEncoder}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._

/**
 * Type-inference utilities for POJOs and Java collections.
 */
object JavaTypeInference {
  /**
   * Infers the corresponding SQL data type of a JavaBean class.
   * @param beanClass Java type
   * @return (SQL data type, nullable)
   */
  def inferDataType(beanClass: Class[_]): (DataType, Boolean) = {
    val encoder = encoderFor(beanClass)
    (encoder.dataType, encoder.nullable)
  }

  /**
   * Infers the corresponding SQL data type of a Java type.
   * @param beanType Java type
   * @return (SQL data type, nullable)
   */
  private[sql] def inferDataType(beanType: Type): (DataType, Boolean) = {
    inferDataType(beanType.asInstanceOf[Class[_]])
  }

  def encoderFor[T](beanClass: Class[T]): AgnosticEncoder[T] = {
    encoderFor(beanClass, Set.empty).asInstanceOf[AgnosticEncoder[T]]
  }

  def encoderFor(c: Class[_], seenTypeSet: Set[Class[_]]): AgnosticEncoder[_] = c match {

    case _ if c == java.lang.Boolean.TYPE => PrimitiveBooleanEncoder
    case _ if c == java.lang.Byte.TYPE => PrimitiveByteEncoder
    case _ if c == java.lang.Short.TYPE => PrimitiveShortEncoder
    case _ if c == java.lang.Integer.TYPE => PrimitiveIntEncoder
    case _ if c == java.lang.Long.TYPE => PrimitiveLongEncoder
    case _ if c == java.lang.Float.TYPE => PrimitiveFloatEncoder
    case _ if c == java.lang.Double.TYPE => PrimitiveDoubleEncoder

    case _ if c == classOf[java.lang.Boolean] => BoxedBooleanEncoder
    case _ if c == classOf[java.lang.Byte] => BoxedByteEncoder
    case _ if c == classOf[java.lang.Short] => BoxedShortEncoder
    case _ if c == classOf[java.lang.Integer] => BoxedIntEncoder
    case _ if c == classOf[java.lang.Long] => BoxedLongEncoder
    case _ if c == classOf[java.lang.Float] => BoxedFloatEncoder
    case _ if c == classOf[java.lang.Double] => BoxedDoubleEncoder

    case _ if c == classOf[java.lang.String] => StringEncoder
    case _ if c == classOf[Array[Byte]] => BinaryEncoder
    case _ if c == classOf[java.math.BigDecimal] => DEFAULT_JAVA_DECIMAL_ENCODER
    case _ if c == classOf[java.math.BigInteger] => JavaBigIntEncoder
    case _ if c == classOf[java.time.LocalDate] => STRICT_LOCAL_DATE_ENCODER
    case _ if c == classOf[java.sql.Date] => STRICT_DATE_ENCODER
    case _ if c == classOf[java.time.Instant] => STRICT_INSTANT_ENCODER
    case _ if c == classOf[java.sql.Timestamp] => STRICT_TIMESTAMP_ENCODER
    case _ if c == classOf[java.time.LocalDateTime] => LocalDateTimeEncoder
    case _ if c == classOf[java.time.Duration] => DayTimeIntervalEncoder
    case _ if c == classOf[java.time.Period] => YearMonthIntervalEncoder

    case _ if c.isEnum => JavaEnumEncoder(ClassTag(c))

    case _ if c.isAnnotationPresent(classOf[SQLUserDefinedType]) =>
      val udt = c.getAnnotation(classOf[SQLUserDefinedType]).udt()
        .getConstructor().newInstance().asInstanceOf[UserDefinedType[Any]]
      val udtClass = udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()
      UDTEncoder(udt, udtClass)

    case _ if UDTRegistration.exists(c.getName) =>
      val udt = UDTRegistration.getUDTFor(c.getName).get.getConstructor().
        newInstance().asInstanceOf[UserDefinedType[Any]]
      UDTEncoder(udt, udt.getClass)

    case _ if c.isArray =>
      val elementEncoder = encoderFor(c.getComponentType, seenTypeSet)
      ArrayEncoder(elementEncoder, elementEncoder.nullable)

    case _ if classOf[JList[_]].isAssignableFrom(c) =>
      // TODO this is probably too restrictive. Upperbounds for example would sort of work.
      val Array(elementCls: Class[_]) = findTypeArgumentsForInterface(c, classOf[JList[_]])
      val element = encoderFor(elementCls)
      IterableEncoder(ClassTag(c), element, element.nullable, lenientSerialization = false)

    case _ if classOf[JMap[_, _]].isAssignableFrom(c) =>
      val Array(keyCls: Class[_], valueCls: Class[_]) =
        findTypeArgumentsForInterface(c, classOf[JMap[_, _]])
      val keyEncoder = encoderFor(keyCls)
      val valueEncoder = encoderFor(valueCls)
      MapEncoder(ClassTag(c), keyEncoder, valueEncoder, valueEncoder.nullable)

    case _ =>
      if (seenTypeSet.contains(c)) {
        throw QueryExecutionErrors.cannotHaveCircularReferencesInBeanClassError(c)
      }

      // TODO: we should only collect properties that have getter and setter. However, some tests
      // pass in scala case class as java bean class which doesn't have getter and setter.
      val properties = getJavaBeanReadableProperties(c)
      val fields = properties.map { property =>
        val readMethod = property.getReadMethod
        val encoder = encoderFor(readMethod.getReturnType, seenTypeSet + c)
        // The existence of `javax.annotation.Nonnull`, means this field is not nullable.
        val hasNonNull = readMethod.isAnnotationPresent(classOf[Nonnull])
        EncoderField(
          property.getName,
          encoder,
          encoder.nullable && !hasNonNull,
          Metadata.empty,
          Option(readMethod.getName),
          Option(property.getWriteMethod).map(_.getName))
      }
      JavaBeanEncoder(ClassTag(c), fields)
  }

  def getJavaBeanReadableProperties(beanClass: Class[_]): Array[PropertyDescriptor] = {
    val beanInfo = Introspector.getBeanInfo(beanClass)
    beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
      .filterNot(_.getName == "declaringClass")
      .filter(_.getReadMethod != null)
  }

  private def findTypeArgumentsForInterface(
      cls: Class[_],
      interfaceCls: Class[_]): Array[Type] = {
    assert(interfaceCls.getTypeParameters.nonEmpty)
    val queue = new ArrayDeque[Type]()
    queue.add(cls)
    while (!queue.isEmpty) {
      queue.poll() match {
        case pt: ParameterizedType if pt.getRawType == interfaceCls =>
          return pt.getActualTypeArguments
        case pt: ParameterizedType =>
          queue.add(pt.getRawType)
        case c: Class[_] =>
          c.getGenericInterfaces.foreach(queue.add)
      }
    }
    throw new RuntimeException(s"$cls does not implement generic interface $interfaceCls")
  }
}

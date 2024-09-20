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
import java.lang.reflect.{ParameterizedType, Type, TypeVariable}
import java.util.{List => JList, Map => JMap, Set => JSet}
import javax.annotation.Nonnull

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import com.esotericsoftware.kryo.KryoSerializable
import org.apache.commons.lang3.reflect.{TypeUtils => JavaTypeUtils}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * Type-inference utilities for POJOs and Java collections.
 */
object JavaTypeInference {

  /**
   * Infers the corresponding SQL data type of a Java type.
   * @param beanType
   *   Java type
   * @return
   *   (SQL data type, nullable)
   */
  def inferDataType(beanType: Type): (DataType, Boolean) = {
    val encoder = encoderFor(beanType)
    (encoder.dataType, encoder.nullable)
  }

  /**
   * Infer an [[AgnosticEncoder]] for the [[Class]] `cls`.
   */
  def encoderFor[T](cls: Class[T]): AgnosticEncoder[T] = {
    encoderFor(cls.asInstanceOf[Type])
  }

  /**
   * Infer an [[AgnosticEncoder]] for the `beanType`.
   */
  def encoderFor[T](beanType: Type): AgnosticEncoder[T] = {
    encoderFor(beanType, Set.empty).asInstanceOf[AgnosticEncoder[T]]
  }

  private def encoderFor(
      t: Type,
      seenTypeSet: Set[Class[_]],
      typeVariables: Map[TypeVariable[_], Type] = Map.empty,
      forGenericBound: Boolean = false): AgnosticEncoder[_] = t match {

    case c: Class[_] if !forGenericBound && c == java.lang.Boolean.TYPE => PrimitiveBooleanEncoder
    case c: Class[_] if !forGenericBound && c == java.lang.Byte.TYPE => PrimitiveByteEncoder
    case c: Class[_] if !forGenericBound && c == java.lang.Short.TYPE => PrimitiveShortEncoder
    case c: Class[_] if !forGenericBound && c == java.lang.Integer.TYPE => PrimitiveIntEncoder
    case c: Class[_] if !forGenericBound && c == java.lang.Long.TYPE => PrimitiveLongEncoder
    case c: Class[_] if !forGenericBound && c == java.lang.Float.TYPE => PrimitiveFloatEncoder
    case c: Class[_] if !forGenericBound && c == java.lang.Double.TYPE => PrimitiveDoubleEncoder

    case c: Class[_] if !forGenericBound && c == classOf[java.lang.Boolean] => BoxedBooleanEncoder
    case c: Class[_] if !forGenericBound && c == classOf[java.lang.Byte] => BoxedByteEncoder
    case c: Class[_] if !forGenericBound && c == classOf[java.lang.Short] => BoxedShortEncoder
    case c: Class[_] if !forGenericBound && c == classOf[java.lang.Integer] => BoxedIntEncoder
    case c: Class[_] if !forGenericBound && c == classOf[java.lang.Long] => BoxedLongEncoder
    case c: Class[_] if !forGenericBound && c == classOf[java.lang.Float] => BoxedFloatEncoder
    case c: Class[_] if !forGenericBound && c == classOf[java.lang.Double] => BoxedDoubleEncoder

    case c: Class[_] if !forGenericBound && c == classOf[java.lang.String] => StringEncoder
    case c: Class[_] if !forGenericBound && c == classOf[Array[Byte]] => BinaryEncoder
    case c: Class[_] if !forGenericBound && c == classOf[java.math.BigDecimal] =>
      DEFAULT_JAVA_DECIMAL_ENCODER
    case c: Class[_] if !forGenericBound && c == classOf[java.math.BigInteger] =>
      JavaBigIntEncoder
    case c: Class[_] if !forGenericBound && c == classOf[java.time.LocalDate] =>
      STRICT_LOCAL_DATE_ENCODER
    case c: Class[_] if !forGenericBound && c == classOf[java.sql.Date] => STRICT_DATE_ENCODER
    case c: Class[_] if !forGenericBound && c == classOf[java.time.Instant] =>
      STRICT_INSTANT_ENCODER
    case c: Class[_] if !forGenericBound && c == classOf[java.sql.Timestamp] =>
      STRICT_TIMESTAMP_ENCODER
    case c: Class[_] if !forGenericBound && c == classOf[java.time.LocalDateTime] =>
      LocalDateTimeEncoder
    case c: Class[_] if !forGenericBound && c == classOf[java.time.Duration] =>
      DayTimeIntervalEncoder
    case c: Class[_] if !forGenericBound && c == classOf[java.time.Period] =>
      YearMonthIntervalEncoder

    case c: Class[_] if !forGenericBound && c.isEnum => JavaEnumEncoder(ClassTag(c))

    case c: Class[_] if c.isAnnotationPresent(classOf[SQLUserDefinedType]) =>
      val udt = c
        .getAnnotation(classOf[SQLUserDefinedType])
        .udt()
        .getConstructor()
        .newInstance()
        .asInstanceOf[UserDefinedType[Any]]
      val udtClass = udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()
      UDTEncoder(udt, udtClass)

    case c: Class[_] if UDTRegistration.exists(c.getName) =>
      val udt = UDTRegistration
        .getUDTFor(c.getName)
        .get
        .getConstructor()
        .newInstance()
        .asInstanceOf[UserDefinedType[Any]]
      UDTEncoder(udt, udt.getClass)

    case c: Class[_] if !forGenericBound && c.isArray =>
      val elementEncoder = encoderFor(c.getComponentType, seenTypeSet, typeVariables)
      ArrayEncoder(elementEncoder, elementEncoder.nullable)

    case c: Class[_] if !forGenericBound && classOf[JList[_]].isAssignableFrom(c) =>
      val element = encoderFor(c.getTypeParameters.array(0), seenTypeSet, typeVariables)
      IterableEncoder(ClassTag(c), element, element.nullable, lenientSerialization = false)

    case c: Class[_] if !forGenericBound && classOf[JSet[_]].isAssignableFrom(c) =>
      val element = encoderFor(c.getTypeParameters.array(0), seenTypeSet, typeVariables)
      IterableEncoder(ClassTag(c), element, element.nullable, lenientSerialization = false)

    case c: Class[_] if !forGenericBound && classOf[JMap[_, _]].isAssignableFrom(c) =>
      val keyEncoder = encoderFor(c.getTypeParameters.array(0), seenTypeSet, typeVariables)
      val valueEncoder = encoderFor(c.getTypeParameters.array(1), seenTypeSet, typeVariables)
      MapEncoder(ClassTag(c), keyEncoder, valueEncoder, valueEncoder.nullable)

    case tv: TypeVariable[_] =>
      typeVariables.get(tv) match {
        case Some(knownType) => encoderFor(knownType, seenTypeSet, typeVariables)

        case None =>
          // Concrete type unknown, check if the bounds can be used to identify a
          // UDTEncoder and if exception is thrown check if the type extends Serializable
          val concreteBound = tv.getBounds.collectFirst { case cls: Class[_] => cls }
          tv.getBounds
            .flatMap(getAllSuperClasses)
            .foldLeft[(Option[AgnosticEncoder[_]], Option[AgnosticEncoder[_]])](None -> None) {
              case (r @ (Some(_), _), _) => r

              case (r @ (None, serEncoder), bound) =>
                Try(encoderFor(bound, seenTypeSet, typeVariables, forGenericBound = true)) match {
                  case Success(value) => (Option(value), serEncoder)

                  case Failure(UseSerializationEncoder(encoderProvider)) if serEncoder.isEmpty =>
                    None -> Option(encoderProvider(concreteBound.getOrElse(bound)))

                  case Failure(_) => r
                }
            } match {
            case (Some(encd), _) => encd

            case (None, Some(encd)) => encd

            case _ => throw ExecutionErrors.cannotFindEncoderForTypeError(t.getTypeName)
          }
      }

    case pt: ParameterizedType if !forGenericBound =>
      encoderFor(pt.getRawType, seenTypeSet, JavaTypeUtils.getTypeArguments(pt).asScala.toMap)

    case c: Class[_] if !forGenericBound =>
      if (seenTypeSet.contains(c)) {
        throw ExecutionErrors.cannotHaveCircularReferencesInBeanClassError(c)
      }

      // TODO: we should only collect properties that have getter and setter. However, some
      //  tests pass in scala case class as java bean class which doesn't have getter and
      //  setter.
      val properties = getJavaBeanReadableProperties(c)
      // if the properties is empty and this is not a top level enclosing class, then we
      // should not consider class as bean, as otherwise it will be treated as empty schema
      // and loose the data on deser.
      if (properties.isEmpty && seenTypeSet.nonEmpty) {
        if (classOf[KryoSerializable].isAssignableFrom(c)) {
          Encoders.kryo(c).asInstanceOf[AgnosticEncoder[_]]
        } else if (classOf[java.io.Serializable].isAssignableFrom(c)) {
          Encoders.javaSerialization(c).asInstanceOf[AgnosticEncoder[_]]
        } else {
          throw ExecutionErrors.cannotFindEncoderForTypeError(t.getTypeName)
        }
      } else {
        // add type variables from inheritance hierarchy of the class
        val parentClassesTypeMap =
          JavaTypeUtils.getTypeArguments(c, classOf[Object]).asScala.toMap
        val classTV = parentClassesTypeMap ++ typeVariables
        // Note that the fields are ordered by name.
        val fields = properties.map { property =>
          val readMethod = property.getReadMethod
          val methodReturnType = readMethod.getGenericReturnType
          val encoder = encoderFor(methodReturnType, seenTypeSet + c, classTV)
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
        // implies it cannot be assumed a BeanClass.
        // Check if its super class or interface could be represented by an Encoder

        JavaBeanEncoder(ClassTag(c), fields.toImmutableArraySeq)
      }

    case t =>
      throw ExecutionErrors.cannotFindEncoderForTypeError(t.getTypeName)
  }

  private def getAllSuperClasses(typee: Type): Array[Class[_]] = Option(typee)
    .map({
      case clazz: Class[_] =>
        val currentInterfaces = clazz.getInterfaces
        currentInterfaces ++ (clazz.getSuperclass +: currentInterfaces).flatMap(clz =>
          getAllSuperClasses(clz)) :+ clazz

      case tv: TypeVariable[_] =>
        tv.getBounds.foldLeft(Array.empty[Class[_]]) { case (res, bnd) =>
          res ++ getAllSuperClasses(bnd)
        }
    })
    .getOrElse(Array.empty)

  def getJavaBeanReadableProperties(beanClass: Class[_]): Array[PropertyDescriptor] = {
    val beanInfo = Introspector.getBeanInfo(beanClass)
    beanInfo.getPropertyDescriptors
      .filterNot(_.getName == "class")
      .filterNot(_.getName == "declaringClass")
      .filter(_.getReadMethod != null)
  }

  object UseSerializationEncoder {
    def unapply(th: Throwable): Option[Class[_] => AgnosticEncoder[_]] = th match {
      case s: SparkUnsupportedOperationException
          if s.getErrorClass == ExecutionErrors.ENCODER_NOT_FOUND_ERROR =>
        s.getMessageParameters.asScala.get(ExecutionErrors.TYPE_NAME) match {
          case Some(clzzName) if clzzName == classOf[java.io.Serializable].getTypeName =>
            Option(Encoders.javaSerialization(_).asInstanceOf[AgnosticEncoder[_]])

          case Some(clzzName) if clzzName == classOf[KryoSerializable].getTypeName =>
            Option(Encoders.kryo(_).asInstanceOf[AgnosticEncoder[_]])

          case _ => None
        }

      case _ => None
    }
  }
}

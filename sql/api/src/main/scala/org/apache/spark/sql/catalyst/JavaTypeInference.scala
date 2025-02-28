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

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import com.esotericsoftware.kryo.KryoSerializable
import org.apache.commons.lang3.reflect.{TypeUtils => JavaTypeUtils}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{ArrayEncoder, BinaryEncoder, BoxedBooleanEncoder, BoxedByteEncoder, BoxedDoubleEncoder, BoxedFloatEncoder, BoxedIntEncoder, BoxedLongEncoder, BoxedShortEncoder, DayTimeIntervalEncoder, DEFAULT_JAVA_DECIMAL_ENCODER, EncoderField, IterableEncoder, JavaBeanEncoder, JavaBigIntEncoder, JavaEnumEncoder, LocalDateTimeEncoder, MapEncoder, PrimitiveBooleanEncoder, PrimitiveByteEncoder, PrimitiveDoubleEncoder, PrimitiveFloatEncoder, PrimitiveIntEncoder, PrimitiveLongEncoder, PrimitiveShortEncoder, STRICT_DATE_ENCODER, STRICT_INSTANT_ENCODER, STRICT_LOCAL_DATE_ENCODER, STRICT_TIMESTAMP_ENCODER, StringEncoder, UDTEncoder, YearMonthIntervalEncoder}
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * Type-inference utilities for POJOs and Java collections.
 */
object JavaTypeInference {

  private val clientConnectFlag = ThreadLocal.withInitial[Boolean](() => false)

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
    case c: Class[_] if forGenericBound && c.isAnnotationPresent(classOf[SQLUserDefinedType]) =>
      createUDTEncoderUsingAnnotation(c)

    case c: Class[_] if forGenericBound && UDTRegistration.exists(c.getName) =>
      createUDTEncoderUsingRegistration(c)

    case t if forGenericBound =>
      throw ExecutionErrors.cannotFindEncoderForTypeError(t.getTypeName)

    case c: Class[_] if c == java.lang.Boolean.TYPE => PrimitiveBooleanEncoder
    case c: Class[_] if c == java.lang.Byte.TYPE => PrimitiveByteEncoder
    case c: Class[_] if c == java.lang.Short.TYPE => PrimitiveShortEncoder
    case c: Class[_] if c == java.lang.Integer.TYPE => PrimitiveIntEncoder
    case c: Class[_] if c == java.lang.Long.TYPE => PrimitiveLongEncoder
    case c: Class[_] if c == java.lang.Float.TYPE => PrimitiveFloatEncoder
    case c: Class[_] if c == java.lang.Double.TYPE => PrimitiveDoubleEncoder

    case c: Class[_] if c == classOf[java.lang.Boolean] => BoxedBooleanEncoder
    case c: Class[_] if c == classOf[java.lang.Byte] => BoxedByteEncoder
    case c: Class[_] if c == classOf[java.lang.Short] => BoxedShortEncoder
    case c: Class[_] if c == classOf[java.lang.Integer] => BoxedIntEncoder
    case c: Class[_] if c == classOf[java.lang.Long] => BoxedLongEncoder
    case c: Class[_] if c == classOf[java.lang.Float] => BoxedFloatEncoder
    case c: Class[_] if c == classOf[java.lang.Double] => BoxedDoubleEncoder

    case c: Class[_] if c == classOf[java.lang.String] => StringEncoder
    case c: Class[_] if c == classOf[Array[Byte]] => BinaryEncoder
    case c: Class[_] if c == classOf[java.math.BigDecimal] => DEFAULT_JAVA_DECIMAL_ENCODER
    case c: Class[_] if c == classOf[java.math.BigInteger] => JavaBigIntEncoder
    case c: Class[_] if c == classOf[java.time.LocalDate] => STRICT_LOCAL_DATE_ENCODER
    case c: Class[_] if c == classOf[java.sql.Date] => STRICT_DATE_ENCODER
    case c: Class[_] if c == classOf[java.time.Instant] => STRICT_INSTANT_ENCODER
    case c: Class[_] if c == classOf[java.sql.Timestamp] => STRICT_TIMESTAMP_ENCODER
    case c: Class[_] if c == classOf[java.time.LocalDateTime] => LocalDateTimeEncoder
    case c: Class[_] if c == classOf[java.time.Duration] => DayTimeIntervalEncoder
    case c: Class[_] if c == classOf[java.time.Period] => YearMonthIntervalEncoder

    case c: Class[_] if c.isEnum => JavaEnumEncoder(ClassTag(c))

    case c: Class[_] if c.isAnnotationPresent(classOf[SQLUserDefinedType]) =>
      createUDTEncoderUsingAnnotation(c)

    case c: Class[_] if UDTRegistration.exists(c.getName) => createUDTEncoderUsingRegistration(c)

    case c: Class[_] if c.isArray =>
      val elementEncoder = encoderFor(c.getComponentType, seenTypeSet, typeVariables)
      ArrayEncoder(elementEncoder, elementEncoder.nullable)

    case c: Class[_] if classOf[JList[_]].isAssignableFrom(c) =>
      val element = encoderFor(c.getTypeParameters.array(0), seenTypeSet, typeVariables)
      IterableEncoder(ClassTag(c), element, element.nullable, lenientSerialization = false)

    case c: Class[_] if classOf[JSet[_]].isAssignableFrom(c) =>
      val element = encoderFor(c.getTypeParameters.array(0), seenTypeSet, typeVariables)
      IterableEncoder(ClassTag(c), element, element.nullable, lenientSerialization = false)

    case c: Class[_] if classOf[JMap[_, _]].isAssignableFrom(c) =>
      val keyEncoder = encoderFor(c.getTypeParameters.array(0), seenTypeSet, typeVariables)
      val valueEncoder = encoderFor(c.getTypeParameters.array(1), seenTypeSet, typeVariables)
      MapEncoder(ClassTag(c), keyEncoder, valueEncoder, valueEncoder.nullable)

    case tv: TypeVariable[_] =>
      typeVariables.get(tv) match {
        case Some(knownType) => encoderFor(knownType, seenTypeSet, typeVariables, forGenericBound)

        case None =>
          // Concrete type unknown, check if the bounds can be used to identify a
          // UDTEncoder and if exception is thrown check if the type extends Serializable
          val concreteBound = tv.getBounds.collectFirst { case SuperClassesExtractor(clazz) =>
            clazz
          }
          findOptimalEncoderForGenericType(
            tv.getBounds.flatMap(SuperClassesExtractor.getSuperClasses(_)).toSeq,
            seenTypeSet,
            typeVariables,
            concreteBound)
            .getOrElse(throw ExecutionErrors.cannotFindEncoderForTypeError(t.getTypeName))
      }

    case pt: ParameterizedType =>
      encoderFor(pt.getRawType, seenTypeSet, JavaTypeUtils.getTypeArguments(pt).asScala.toMap)

    case c: Class[_] =>
      if (seenTypeSet.contains(c)) {
        throw ExecutionErrors.cannotHaveCircularReferencesInBeanClassError(c)
      }

      // TODO: we should only collect properties that have getter and setter. However, some tests
      //   pass in scala case class as java bean class which doesn't have getter and setter.
      val properties = getJavaBeanReadableProperties(c)

      // if the properties is empty and this is not a top level enclosing class, then we
      // should not consider class as bean, as otherwise it will be treated as empty schema
      // and loose the data on deser.
      if (properties.isEmpty && seenTypeSet.nonEmpty) {
        // Here the encoder possible can only be of type Java or Kryo Serializable type.
        // As if it had been of type UDT, the code will not reach this point.
        findOptimalEncoderForGenericType(
          Seq(c),
          seenTypeSet,
          typeVariables,
          None,
          serializableEncodersOnly = true)
          .getOrElse(throw ExecutionErrors.cannotFindEncoderForTypeError(t.getTypeName))
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
        JavaBeanEncoder(ClassTag(c), fields.toImmutableArraySeq)
      }

    case _ =>
      throw ExecutionErrors.cannotFindEncoderForTypeError(t.toString)
  }

  private def createUDTEncoderUsingAnnotation(c: Class[_]): UDTEncoder[Any] = {
    val udt = c
      .getAnnotation(classOf[SQLUserDefinedType])
      .udt()
      .getConstructor()
      .newInstance()
      .asInstanceOf[UserDefinedType[Any]]
    val udtClass = udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()
    UDTEncoder(udt, udtClass)
  }

  private def createUDTEncoderUsingRegistration(c: Class[_]): UDTEncoder[Any] = {
    val udt = UDTRegistration
      .getUDTFor(c.getName)
      .get
      .getConstructor()
      .newInstance()
      .asInstanceOf[UserDefinedType[Any]]
    UDTEncoder(udt, udt.getClass)
  }

  def getJavaBeanReadableProperties(beanClass: Class[_]): Array[PropertyDescriptor] = {
    val beanInfo = Introspector.getBeanInfo(beanClass)
    beanInfo.getPropertyDescriptors
      .filterNot(_.getName == "class")
      .filterNot(_.getName == "declaringClass")
      .filter(_.getReadMethod != null)
  }

  /*
     This method attempts to find the optimal encoder for generic type.
     A generic type T with one or more upper bounds may indirectly be implementing any
     combination of one or more of the Serializable, KryoSerializable, UDTType or it may
     be implementing none of those. The upper bound of the type could be class with multiple
     interfaces and multiple levels of hierarchy.
     The below function attempts to give preference to UDTEncoder before Serializable Encoders
     ( Java and Kryo). It does not attempt to give preference to Kryo over Java Ser as of now.
     Also if the Encoder is being created of Spark Connect, it should not create a Kryo based
     Encoder.
   */
  private def findOptimalEncoderForGenericType(
      typesToCheck: Seq[Class[_]],
      seenTypeSet: Set[Class[_]],
      typeVariables: Map[TypeVariable[_], Type],
      baseClass: Option[Class[_]],
      serializableEncodersOnly: Boolean = false): Option[AgnosticEncoder[_]] =
    if (serializableEncodersOnly) {
      val isClientConnect = clientConnectFlag.get
      assert(typesToCheck.size == 1)
      typesToCheck.headOption.flatMap(c => {
        if (!isClientConnect && classOf[KryoSerializable].isAssignableFrom(c)) {
          Some(Encoders.kryo(c).asInstanceOf[AgnosticEncoder[_]])
        } else if (classOf[java.io.Serializable].isAssignableFrom(c)) {
          Some(Encoders.javaSerialization(c).asInstanceOf[AgnosticEncoder[_]])
        } else {
          None
        }
      })
    } else {
      // The code below attempts to find UDTbased Encoder if available, else resort to
      // Java/Kryo based encoder. The left option is placeholder for UDTEncoder.
      // If UDTEncoder is found first, then the right option ( for Java/Kryo encoder) is
      // not needed. But if reverse happens, then Right Option is kept populated, while we
      // continue to look for UDT based encoder.
      // In the end, we return with either UDT based or Serialization based encoder or None
      typesToCheck
        .foldLeft[(Option[AgnosticEncoder[_]], Option[AgnosticEncoder[_]])](None -> None) {
          case (r @ (Some(_), _), _) => r

          case (r @ (None, serEncoder), bound) =>
            Try(encoderFor(bound, seenTypeSet, typeVariables, forGenericBound = true)) match {

              // A Success cannot have any other encoder except UDT
              case Success(value: UDTEncoder[_]) =>
                if (baseClass.exists(_.isAssignableFrom(value.udt.userClass))) {
                  (Some(value), serEncoder)
                } else {
                  r
                }

              // This is not expected, but to silence the warning, is needed
              case Success(value) => (Some(value), serEncoder)

              case Failure(UseSerializationEncoder(encoderProvider)) if serEncoder.isEmpty =>
                None -> Some(encoderProvider(baseClass.getOrElse(bound)))

              case Failure(_) => r
            }
        } match {
        case (r @ Some(_), _) => r

        case (None, r @ Some(_)) => r

        case _ => None
      }
    }

  object SuperClassesExtractor {
    def getSuperClasses(typee: Type, firstFoundOnly: Boolean = false): Seq[Class[_]] =
      if (Option(typee).isDefined) {
        val queue = mutable.Queue.apply(typee)
        val typeSeen = mutable.Set.empty[Type]
        while (queue.nonEmpty) {
          val typeToCheck = queue.dequeue()
          typeSeen += typeToCheck
          typeToCheck match {
            case clazz: Class[_] =>
              if (firstFoundOnly) {
                // we have got the first concrete class/interface
                queue.clear()
              } else {
                Option(clazz.getSuperclass).filterNot(typeSeen.contains).foreach(queue += _)
                queue ++= clazz.getInterfaces.filterNot(typeSeen.contains)
              }

            case tv: TypeVariable[_] =>
              queue ++= tv.getBounds.filterNot(typeSeen.contains)
          }
        }
        typeSeen.flatMap {
          case clazz: Class[_] => Seq(clazz)

          case _ => Seq.empty
        }.toSeq
      } else {
        Seq.empty
      }

    def unapply(typee: Type): Option[Class[_]] =
      getSuperClasses(typee, firstFoundOnly = true).headOption
  }

  object UseSerializationEncoder {
    def unapply(th: Throwable): Option[Class[_] => AgnosticEncoder[_]] = th match {
      case s: SparkUnsupportedOperationException
          if s.getCondition == ExecutionErrors.ENCODER_NOT_FOUND_ERROR =>
        s.getMessageParameters.asScala.get(ExecutionErrors.TYPE_NAME) match {
          case Some(clzzName) if clzzName == classOf[java.io.Serializable].getTypeName =>
            Option(Encoders.javaSerialization(_).asInstanceOf[AgnosticEncoder[_]])

          case Some(clzzName)
              if !clientConnectFlag.get && clzzName == classOf[KryoSerializable].getTypeName =>
            Option(Encoders.kryo(_).asInstanceOf[AgnosticEncoder[_]])

          case _ => None
        }

      case _ => None
    }
  }

  private[sql] def setSparkClientFlag(): Unit = this.clientConnectFlag.set(true)

  private[sql] def unsetSparkClientFlag(): Unit = this.clientConnectFlag.set(false)
}

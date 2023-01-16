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

import javax.lang.model.SourceVersion

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.reflect.internal.Symbols
import scala.util.{Failure, Success}

import org.apache.commons.lang3.reflect.ConstructorUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.DeserializerBuildHelper._
import org.apache.spark.sql.catalyst.SerializerBuildHelper._
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.catalyst.expressions.{Expression, _}
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}


/**
 * A helper trait to create [[org.apache.spark.sql.catalyst.encoders.ExpressionEncoder]]s
 * for classes whose fields are entirely defined by constructor params but should not be
 * case classes.
 */
trait DefinedByConstructorParams


private[catalyst] object ScalaSubtypeLock


/**
 * A default version of ScalaReflection that uses the runtime universe.
 */
object ScalaReflection extends ScalaReflection {
  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  // Since we are creating a runtime mirror using the class loader of current thread,
  // we need to use def at here. So, every time we call mirror, it is using the
  // class loader of the current thread.
  override def mirror: universe.Mirror = {
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
  }

  import universe._

  // The Predef.Map is scala.collection.immutable.Map.
  // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
  import scala.collection.Map

  /**
   * Synchronize to prevent concurrent usage of `<:<` operator.
   * This operator is not thread safe in any current version of scala; i.e.
   * (2.11.12, 2.12.10, 2.13.0-M5).
   *
   * See https://github.com/scala/bug/issues/10766
   */
  private[catalyst] def isSubtype(tpe1: `Type`, tpe2: `Type`): Boolean = {
    ScalaSubtypeLock.synchronized {
      tpe1 <:< tpe2
    }
  }

  // TODO this name is slightly misleading. This returns the input
  //  data type we expect to see during serialization.
  private[catalyst] def dataTypeFor(enc: AgnosticEncoder[_]): DataType = {
    // DataType can be native.
    if (isNativeEncoder(enc)) {
      enc.dataType
    } else {
      ObjectType(enc.clsTag.runtimeClass)
    }
  }

  /**
   * Returns true if the value of this data type is same between internal and external.
   */
  def isNativeType(dt: DataType): Boolean = dt match {
    case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BinaryType | CalendarIntervalType => true
    case _ => false
  }

  private def baseType(tpe: `Type`): `Type` = {
    tpe.dealias match {
      case annotatedType: AnnotatedType => annotatedType.underlying
      case other => other
    }
  }

  /**
   * Returns true if the encoders' internal and external data type is the same.
   */
  private def isNativeEncoder(enc: AgnosticEncoder[_]): Boolean = enc match {
    case PrimitiveBooleanEncoder => true
    case PrimitiveByteEncoder => true
    case PrimitiveShortEncoder => true
    case PrimitiveIntEncoder => true
    case PrimitiveLongEncoder => true
    case PrimitiveFloatEncoder => true
    case PrimitiveDoubleEncoder => true
    case NullEncoder => true
    case CalendarIntervalEncoder => true
    case BinaryEncoder => true
    case SparkDecimalEncoder => true
    case _ => false
  }

  private def toArrayMethodName(enc: AgnosticEncoder[_]): String = enc match {
    case PrimitiveBooleanEncoder => "toBooleanArray"
    case PrimitiveByteEncoder => "toByteArray"
    case PrimitiveShortEncoder => "toShortArray"
    case PrimitiveIntEncoder => "toIntArray"
    case PrimitiveLongEncoder => "toLongArray"
    case PrimitiveFloatEncoder => "toFloatArray"
    case PrimitiveDoubleEncoder => "toDoubleArray"
    case _ => "array"
  }

  /**
   * Returns an expression for deserializing the Spark SQL representation of an object into its
   * external form. The mapping between the internal and external representations is
   * described by encoder `enc`. The Spark SQL representation is located at ordinal 0 of
   * a row, i.e., `GetColumnByOrdinal(0, _)`. Nested classes will have their fields accessed using
   * `UnresolvedExtractValue`.
   *
   * The returned expression is used by `ExpressionEncoder`. The encoder will resolve and bind this
   * deserializer expression when using it.
   *
   * @param enc encoder that describes the mapping between the Spark SQL representation and the
   *            external representation.
   */
  def deserializerFor[T](enc: AgnosticEncoder[T]): Expression = {
    val walkedTypePath = WalkedTypePath().recordRoot(enc.clsTag.runtimeClass.getName)
    // Assumes we are deserializing the first column of a row.
    val input = GetColumnByOrdinal(0, enc.dataType)
    val deserializer = deserializerFor(
      enc,
      upCastToExpectedType(input, enc.dataType, walkedTypePath),
      walkedTypePath)
    expressionWithNullSafety(deserializer, enc.nullable, walkedTypePath)
  }

  /**
   * Returns an expression for deserializing the value of an input expression into its external
   * representation. The mapping between the internal and external representations is
   * described by encoder `enc`.
   *
   * @param enc encoder that describes the mapping between the Spark SQL representation and the
   *            external representation.
   * @param path The expression which can be used to extract serialized value.
   * @param walkedTypePath The paths from top to bottom to access current field when deserializing.
   */
  private def deserializerFor(
      enc: AgnosticEncoder[_],
      path: Expression,
      walkedTypePath: WalkedTypePath): Expression = enc match {
    case _ if isNativeEncoder(enc) =>
      path
    case BoxedBooleanEncoder =>
      createDeserializerForTypesSupportValueOf(path, enc.clsTag.runtimeClass)
    case BoxedByteEncoder =>
      createDeserializerForTypesSupportValueOf(path, enc.clsTag.runtimeClass)
    case BoxedShortEncoder =>
      createDeserializerForTypesSupportValueOf(path, enc.clsTag.runtimeClass)
    case BoxedIntEncoder =>
      createDeserializerForTypesSupportValueOf(path, enc.clsTag.runtimeClass)
    case BoxedLongEncoder =>
      createDeserializerForTypesSupportValueOf(path, enc.clsTag.runtimeClass)
    case BoxedFloatEncoder =>
      createDeserializerForTypesSupportValueOf(path, enc.clsTag.runtimeClass)
    case BoxedDoubleEncoder =>
      createDeserializerForTypesSupportValueOf(path, enc.clsTag.runtimeClass)
    case JavaEnumEncoder(tag) =>
      val toString = createDeserializerForString(path, returnNullable = false)
      createDeserializerForTypesSupportValueOf(toString, tag.runtimeClass)
    case ScalaEnumEncoder(parent, tag) =>
      StaticInvoke(
        parent,
        ObjectType(tag.runtimeClass),
        "withName",
        createDeserializerForString(path, returnNullable = false) :: Nil,
        returnNullable = false)
    case StringEncoder =>
      createDeserializerForString(path, returnNullable = false)
    case ScalaDecimalEncoder =>
      createDeserializerForScalaBigDecimal(path, returnNullable = false)
    case JavaDecimalEncoder =>
      createDeserializerForJavaBigDecimal(path, returnNullable = false)
    case ScalaBigIntEncoder =>
      createDeserializerForScalaBigInt(path)
    case JavaBigIntEncoder =>
      createDeserializerForJavaBigInteger(path, returnNullable = false)
    case DayTimeIntervalEncoder =>
      createDeserializerForDuration(path)
    case YearMonthIntervalEncoder =>
      createDeserializerForPeriod(path)
    case DateEncoder =>
      createDeserializerForSqlDate(path)
    case LocalDateEncoder =>
      createDeserializerForLocalDate(path)
    case TimestampEncoder =>
      createDeserializerForSqlTimestamp(path)
    case InstantEncoder =>
      createDeserializerForInstant(path)
    case LocalDateTimeEncoder =>
      createDeserializerForLocalDateTime(path)
    case UDTEncoder(udt, udtClass) =>
      val obj = NewInstance(udtClass, Nil, ObjectType(udtClass))
      Invoke(obj, "deserialize", ObjectType(udt.userClass), path :: Nil)
    case OptionEncoder(valueEnc) =>
      val newTypePath = walkedTypePath.recordOption(valueEnc.clsTag.runtimeClass.getName)
      val deserializer = deserializerFor(valueEnc, path, newTypePath)
      WrapOption(deserializer, dataTypeFor(valueEnc))

    case ArrayEncoder(elementEnc: AgnosticEncoder[_]) =>
      val newTypePath = walkedTypePath.recordArray(elementEnc.clsTag.runtimeClass.getName)
      val mapFunction: Expression => Expression = element => {
        // upcast the array element to the data type the encoder expected.
        deserializerForWithNullSafetyAndUpcast(
          element,
          elementEnc.dataType,
          nullable = elementEnc.nullable,
          newTypePath,
          deserializerFor(elementEnc, _, newTypePath))
      }
      Invoke(
        UnresolvedMapObjects(mapFunction, path),
        toArrayMethodName(elementEnc),
        ObjectType(enc.clsTag.runtimeClass),
        returnNullable = false)

    case IterableEncoder(clsTag, elementEnc) =>
      val newTypePath = walkedTypePath.recordArray(elementEnc.clsTag.runtimeClass.getName)
      val mapFunction: Expression => Expression = element => {
        // upcast the array element to the data type the encoder expected.
        deserializerForWithNullSafetyAndUpcast(
          element,
          elementEnc.dataType,
          nullable = elementEnc.nullable,
          newTypePath,
          deserializerFor(elementEnc, _, newTypePath))
      }
      UnresolvedMapObjects(mapFunction, path, Some(clsTag.runtimeClass))

    case MapEncoder(tag, keyEncoder, valueEncoder) =>
      val newTypePath = walkedTypePath.recordMap(
        keyEncoder.clsTag.runtimeClass.getName,
        valueEncoder.clsTag.runtimeClass.getName)
      UnresolvedCatalystToExternalMap(
        path,
        deserializerFor(keyEncoder, _, newTypePath),
        deserializerFor(valueEncoder, _, newTypePath),
        tag.runtimeClass)

    case ProductEncoder(tag, fields) =>
      val cls = tag.runtimeClass
      val dt = ObjectType(cls)
      val isTuple = cls.getName.startsWith("scala.Tuple")
      val arguments = fields.zipWithIndex.map {
        case (field, i) =>
          val newTypePath = walkedTypePath.recordField(
            field.enc.clsTag.runtimeClass.getName,
            field.name)
          // For tuples, we grab the inner fields by ordinal instead of name.
          val getter = if (isTuple) {
            addToPathOrdinal(path, i, field.enc.dataType, newTypePath)
          } else {
            addToPath(path, field.name, field.enc.dataType, newTypePath)
          }
          expressionWithNullSafety(
            deserializerFor(field.enc, getter, newTypePath),
            field.enc.nullable,
            newTypePath)
      }
      expressions.If(
        IsNull(path),
        expressions.Literal.create(null, dt),
        NewInstance(cls, arguments, dt, propagateNull = false))
  }

  /**
   * Returns an expression for serializing an object into its Spark SQL form. The mapping
   * between the external and internal representations is described by encoder `enc`. The
   * input object is located at ordinal 0 of a row, i.e., `BoundReference(0, _)`.
   */
  def serializerFor(enc: AgnosticEncoder[_]): Expression = {
    val input = BoundReference(0, dataTypeFor(enc), nullable = enc.nullable)
    serializerFor(enc, input)
  }

  /**
   * Returns an expression for serializing the value of an input expression into its Spark SQL
   * representation. The mapping between the external and internal representations is described
   * by encoder `enc`.
   */
  private def serializerFor(enc: AgnosticEncoder[_], input: Expression): Expression = enc match {
    case _ if isNativeEncoder(enc) => input
    case BoxedBooleanEncoder => createSerializerForBoolean(input)
    case BoxedByteEncoder => createSerializerForByte(input)
    case BoxedShortEncoder => createSerializerForShort(input)
    case BoxedIntEncoder => createSerializerForInteger(input)
    case BoxedLongEncoder => createSerializerForLong(input)
    case BoxedFloatEncoder => createSerializerForFloat(input)
    case BoxedDoubleEncoder => createSerializerForDouble(input)
    case JavaEnumEncoder(_) => createSerializerForJavaEnum(input)
    case ScalaEnumEncoder(_, _) => createSerializerForScalaEnum(input)
    case StringEncoder => createSerializerForString(input)
    case ScalaDecimalEncoder => createSerializerForScalaBigDecimal(input)
    case JavaDecimalEncoder => createSerializerForJavaBigDecimal(input)
    case ScalaBigIntEncoder => createSerializerForScalaBigInt(input)
    case JavaBigIntEncoder => createSerializerForJavaBigInteger(input)
    case DayTimeIntervalEncoder => createSerializerForJavaDuration(input)
    case YearMonthIntervalEncoder => createSerializerForJavaPeriod(input)
    case DateEncoder => createSerializerForSqlDate(input)
    case LocalDateEncoder => createSerializerForJavaLocalDate(input)
    case TimestampEncoder => createSerializerForSqlTimestamp(input)
    case InstantEncoder => createSerializerForJavaInstant(input)
    case LocalDateTimeEncoder => createSerializerForLocalDateTime(input)
    case UDTEncoder(udt, udtClass) => createSerializerForUserDefinedType(input, udt, udtClass)
    case OptionEncoder(valueEnc) =>
      serializerFor(valueEnc, UnwrapOption(dataTypeFor(valueEnc), input))

    case ArrayEncoder(elementEncoder) =>
      serializerForArray(isArray = true, elementEncoder, input)

    case IterableEncoder(ctag, elementEncoder) =>
      val getter = if (classOf[scala.collection.Set[_]].isAssignableFrom(ctag.runtimeClass)) {
        // There's no corresponding Catalyst type for `Set`, we serialize a `Set` to Catalyst array.
        // Note that the property of `Set` is only kept when manipulating the data as domain object.
        Invoke(input, "toSeq", ObjectType(classOf[Seq[_]]))
      } else {
        input
      }
      serializerForArray(isArray = false, elementEncoder, getter)

    case MapEncoder(_, keyEncoder, valueEncoder) =>
      createSerializerForMap(
        input,
        MapElementInformation(
          dataTypeFor(keyEncoder),
          nullable = !keyEncoder.isPrimitive,
          serializerFor(keyEncoder, _)),
        MapElementInformation(
          dataTypeFor(valueEncoder),
          nullable = !valueEncoder.isPrimitive,
          serializerFor(valueEncoder, _))
      )

    case ProductEncoder(_, fields) =>
      val serializedFields = fields.map { field =>
        // SPARK-26730 inputObject won't be null with If's guard below. And KnownNotNul
        // is necessary here. Because for a nullable nested inputObject with struct data
        // type, e.g. StructType(IntegerType, StringType), it will return nullable=true
        // for IntegerType without KnownNotNull. And that's what we do not expect to.
        val getter = Invoke(
          KnownNotNull(input),
          field.name,
          dataTypeFor(field.enc),
          returnNullable = field.enc.nullable)
        field.name -> serializerFor(field.enc, getter)
      }
      createSerializerForObject(input, serializedFields)
  }

  private def serializerForArray(
      isArray: Boolean,
      elementEnc: AgnosticEncoder[_],
      input: Expression): Expression = {
    dataTypeFor(elementEnc) match {
      case dt: ObjectType =>
        createSerializerForMapObjects(input, dt, serializerFor(elementEnc, _))
      case dt if isArray && elementEnc.isPrimitive =>
        createSerializerForPrimitiveArray(input, dt)
      case dt =>
        createSerializerForGenericArray(input, dt, elementEnc.nullable)
    }
  }

  /**
   * Returns the parameter names for the primary constructor of this class.
   *
   * Logically we should call `getConstructorParameters` and throw away the parameter types to get
   * parameter names, however there are some weird scala reflection problems and this method is a
   * workaround to avoid getting parameter types.
   */
  def getConstructorParameterNames(cls: Class[_]): Seq[String] = {
    val m = runtimeMirror(cls.getClassLoader)
    val classSymbol = m.staticClass(cls.getName)
    val t = selfType(classSymbol)
    constructParams(t).map(_.name.decodedName.toString)
  }

  /**
   * Workaround for [[https://github.com/scala/bug/issues/12190 Scala bug #12190]]
   *
   * `ClassSymbol.selfType` can throw an exception in case of cyclic annotation reference
   * in Java classes. A retry of this operation will succeed as the class which defines the
   * cycle is now resolved. It can however expose further recursive annotation references, so
   * we keep retrying until we exhaust our retry threshold. Default threshold is set to 5
   * to allow for a few level of cyclic references.
   */
  @tailrec
  private def selfType(clsSymbol: ClassSymbol, tries: Int = 5): Type = {
    scala.util.Try {
      clsSymbol.selfType
    } match {
      case Success(x) => x
      case Failure(_: Symbols#CyclicReference) if tries > 1 =>
        // Retry on Symbols#CyclicReference if we haven't exhausted our retry limit
        selfType(clsSymbol, tries - 1)
      case Failure(e: RuntimeException)
        if e.getMessage.contains("illegal cyclic reference") && tries > 1 =>
        // UnPickler.unpickle wraps the original Symbols#CyclicReference exception into a runtime
        // exception and does not set the cause, so we inspect the message. The previous case
        // statement is useful for Java classes while this one is for Scala classes.
        selfType(clsSymbol, tries - 1)
      case Failure(e) => throw e
    }
  }

  /**
   * Returns the parameter values for the primary constructor of this class.
   */
  def getConstructorParameterValues(obj: DefinedByConstructorParams): Seq[AnyRef] = {
    getConstructorParameterNames(obj.getClass).map { name =>
      obj.getClass.getMethod(name).invoke(obj)
    }
  }

  private def erasure(tpe: Type): Type = {
    // For user-defined AnyVal classes, we should not erasure it. Otherwise, it will
    // resolve to underlying type which wrapped by this class, e.g erasure
    // `case class Foo(i: Int) extends AnyVal` will return type `Int` instead of `Foo`.
    // But, for other types, we do need to erasure it. For example, we need to erasure
    // `scala.Any` to `java.lang.Object` in order to load it from Java ClassLoader.
    // Please see SPARK-17368 & SPARK-31190 for more details.
    if (isSubtype(tpe, localTypeOf[AnyVal]) && !tpe.toString.startsWith("scala")) {
      tpe
    } else {
      tpe.erasure
    }
  }

  /**
   * Returns the full class name for a type. The returned name is the canonical
   * Scala name, where each component is separated by a period. It is NOT the
   * Java-equivalent runtime name (no dollar signs).
   *
   * In simple cases, both the Scala and Java names are the same, however when Scala
   * generates constructs that do not map to a Java equivalent, such as singleton objects
   * or nested classes in package objects, it uses the dollar sign ($) to create
   * synthetic classes, emulating behaviour in Java bytecode.
   */
  def getClassNameFromType(tpe: `Type`): String = {
    erasure(tpe).dealias.typeSymbol.asClass.fullName
  }

  /*
   * Retrieves the runtime class corresponding to the provided type.
   */
  def getClassFromType(tpe: Type): Class[_] =
    mirror.runtimeClass(erasure(tpe).dealias.typeSymbol.asClass)

  case class Schema(dataType: DataType, nullable: Boolean)

  /** Returns a Sequence of attributes for the given case class type. */
  def attributesFor[T: TypeTag]: Seq[Attribute] = schemaFor[T] match {
    case Schema(s: StructType, _) =>
      s.toAttributes
    case others => throw QueryExecutionErrors.attributesForTypeUnsupportedError(others)
  }

  /** Returns a catalyst DataType and its nullability for the given Scala Type using reflection. */
  def schemaFor[T: TypeTag]: Schema = schemaFor(localTypeOf[T])

  /** Returns a catalyst DataType and its nullability for the given Scala Type using reflection. */
  def schemaFor(tpe: `Type`): Schema = {
    val enc = encoderFor(tpe)
    Schema(enc.dataType, enc.nullable)
  }

  /**
   * Finds an accessible constructor with compatible parameters. This is a more flexible search than
   * the exact matching algorithm in `Class.getConstructor`. The first assignment-compatible
   * matching constructor is returned if it exists. Otherwise, we check for additional compatible
   * constructors defined in the companion object as `apply` methods. Otherwise, it returns `None`.
   */
  def findConstructor[T](cls: Class[T], paramTypes: Seq[Class[_]]): Option[Seq[AnyRef] => T] = {
    Option(ConstructorUtils.getMatchingAccessibleConstructor(cls, paramTypes: _*)) match {
      case Some(c) => Some(x => c.newInstance(x: _*))
      case None =>
        val companion = mirror.staticClass(cls.getName).companion
        val moduleMirror = mirror.reflectModule(companion.asModule)
        val applyMethods = companion.asTerm.typeSignature
          .member(universe.TermName("apply")).asTerm.alternatives
        applyMethods.find { method =>
          val params = method.typeSignature.paramLists.head
          // Check that the needed params are the same length and of matching types
          params.size == paramTypes.size &&
          params.zip(paramTypes).forall { case(ps, pc) =>
            ps.typeSignature.typeSymbol == mirror.classSymbol(pc)
          }
        }.map { applyMethodSymbol =>
          val expectedArgsCount = applyMethodSymbol.typeSignature.paramLists.head.size
          val instanceMirror = mirror.reflect(moduleMirror.instance)
          val method = instanceMirror.reflectMethod(applyMethodSymbol.asMethod)
          (_args: Seq[AnyRef]) => {
            // Drop the "outer" argument if it is provided
            val args = if (_args.size == expectedArgsCount) _args else _args.tail
            method.apply(args: _*).asInstanceOf[T]
          }
        }
    }
  }

  /**
   * Whether the fields of the given type is defined entirely by its constructor parameters.
   */
  def definedByConstructorParams(tpe: Type): Boolean = cleanUpReflectionObjects {
    tpe.dealias match {
      // `Option` is a `Product`, but we don't wanna treat `Option[Int]` as a struct type.
      case t if isSubtype(t, localTypeOf[Option[_]]) => definedByConstructorParams(t.typeArgs.head)
      case _ => isSubtype(tpe.dealias, localTypeOf[Product]) ||
        isSubtype(tpe.dealias, localTypeOf[DefinedByConstructorParams])
    }
  }

  val typeJavaMapping: Map[DataType, Class[_]] = Map[DataType, Class[_]](
    BooleanType -> classOf[Boolean],
    ByteType -> classOf[Byte],
    ShortType -> classOf[Short],
    IntegerType -> classOf[Int],
    LongType -> classOf[Long],
    FloatType -> classOf[Float],
    DoubleType -> classOf[Double],
    StringType -> classOf[UTF8String],
    DateType -> classOf[DateType.InternalType],
    TimestampType -> classOf[TimestampType.InternalType],
    TimestampNTZType -> classOf[TimestampNTZType.InternalType],
    BinaryType -> classOf[BinaryType.InternalType],
    CalendarIntervalType -> classOf[CalendarInterval]
  )

  val typeBoxedJavaMapping: Map[DataType, Class[_]] = Map[DataType, Class[_]](
    BooleanType -> classOf[java.lang.Boolean],
    ByteType -> classOf[java.lang.Byte],
    ShortType -> classOf[java.lang.Short],
    IntegerType -> classOf[java.lang.Integer],
    LongType -> classOf[java.lang.Long],
    FloatType -> classOf[java.lang.Float],
    DoubleType -> classOf[java.lang.Double],
    DateType -> classOf[java.lang.Integer],
    TimestampType -> classOf[java.lang.Long],
    TimestampNTZType -> classOf[java.lang.Long]
  )

  def dataTypeJavaClass(dt: DataType): Class[_] = {
    dt match {
      case _: DecimalType => classOf[Decimal]
      case it: DayTimeIntervalType => classOf[it.InternalType]
      case it: YearMonthIntervalType => classOf[it.InternalType]
      case _: StructType => classOf[InternalRow]
      case _: ArrayType => classOf[ArrayData]
      case _: MapType => classOf[MapData]
      case ObjectType(cls) => cls
      case _ => typeJavaMapping.getOrElse(dt, classOf[java.lang.Object])
    }
  }

  @scala.annotation.tailrec
  def javaBoxedType(dt: DataType): Class[_] = dt match {
    case _: DecimalType => classOf[Decimal]
    case _: DayTimeIntervalType => classOf[java.lang.Long]
    case _: YearMonthIntervalType => classOf[java.lang.Integer]
    case BinaryType => classOf[Array[Byte]]
    case StringType => classOf[UTF8String]
    case CalendarIntervalType => classOf[CalendarInterval]
    case _: StructType => classOf[InternalRow]
    case _: ArrayType => classOf[ArrayType]
    case _: MapType => classOf[MapType]
    case udt: UserDefinedType[_] => javaBoxedType(udt.sqlType)
    case ObjectType(cls) => cls
    case _ => ScalaReflection.typeBoxedJavaMapping.getOrElse(dt, classOf[java.lang.Object])
  }

  def expressionJavaClasses(arguments: Seq[Expression]): Seq[Class[_]] = {
    if (arguments != Nil) {
      arguments.map(e => dataTypeJavaClass(e.dataType))
    } else {
      Seq.empty
    }
  }

  def encodeFieldNameToIdentifier(fieldName: String): String = {
    TermName(fieldName).encodedName.toString
  }

  /**
   * Create an [[AgnosticEncoder]] from a [[TypeTag]].
   *
   * If the given type is not supported, i.e. there is no encoder can be built for this type,
   * an [[SparkUnsupportedOperationException]] will be thrown with detailed error message to
   * explain the type path walked so far and which class we are not supporting.
   * There are 4 kinds of type path:
   *  * the root type: `root class: "abc.xyz.MyClass"`
   *  * the value type of [[Option]]: `option value class: "abc.xyz.MyClass"`
   *  * the element type of [[Array]] or [[Seq]]: `array element class: "abc.xyz.MyClass"`
   *  * the field of [[Product]]: `field (class: "abc.xyz.MyClass", name: "myField")`
   */
  def encoderFor[E : TypeTag]: AgnosticEncoder[E] = {
    encoderFor(typeTag[E].in(mirror).tpe).asInstanceOf[AgnosticEncoder[E]]
  }

  /**
   * Create an [[AgnosticEncoder]] for a [[Type]].
   */
  def encoderFor(tpe: `Type`): AgnosticEncoder[_] = cleanUpReflectionObjects {
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = WalkedTypePath().recordRoot(clsName)
    encoderFor(tpe, Set.empty, walkedTypePath)
  }

  private def encoderFor(
      tpe: `Type`,
      seenTypeSet: Set[`Type`],
      path: WalkedTypePath): AgnosticEncoder[_] = {
    def createIterableEncoder(t: `Type`, fallbackClass: Class[_]): AgnosticEncoder[_] = {
      val TypeRef(_, _, Seq(elementType)) = t
      val encoder = encoderFor(
        elementType,
        seenTypeSet,
        path.recordArray(getClassNameFromType(elementType)))
      val companion = t.dealias.typeSymbol.companion.typeSignature
      val targetClass = companion.member(TermName("newBuilder")) match {
        case NoSymbol => fallbackClass
        case _ => mirror.runtimeClass(t.typeSymbol.asClass)
      }
      IterableEncoder(ClassTag(targetClass), encoder)
    }

    baseType(tpe) match {
      // this must be the first case, since all objects in scala are instances of Null, therefore
      // Null type would wrongly match the first of them, which is Option as of now
      case t if isSubtype(t, definitions.NullTpe) => NullEncoder

      // Primitive encoders
      case t if isSubtype(t, definitions.BooleanTpe) => PrimitiveBooleanEncoder
      case t if isSubtype(t, definitions.ByteTpe) => PrimitiveByteEncoder
      case t if isSubtype(t, definitions.ShortTpe) => PrimitiveShortEncoder
      case t if isSubtype(t, definitions.IntTpe) => PrimitiveIntEncoder
      case t if isSubtype(t, definitions.LongTpe) => PrimitiveLongEncoder
      case t if isSubtype(t, definitions.FloatTpe) => PrimitiveFloatEncoder
      case t if isSubtype(t, definitions.DoubleTpe) => PrimitiveDoubleEncoder
      case t if isSubtype(t, localTypeOf[java.lang.Boolean]) => BoxedBooleanEncoder
      case t if isSubtype(t, localTypeOf[java.lang.Byte]) => BoxedByteEncoder
      case t if isSubtype(t, localTypeOf[java.lang.Short]) => BoxedShortEncoder
      case t if isSubtype(t, localTypeOf[java.lang.Integer]) => BoxedIntEncoder
      case t if isSubtype(t, localTypeOf[java.lang.Long]) => BoxedLongEncoder
      case t if isSubtype(t, localTypeOf[java.lang.Float]) => BoxedFloatEncoder
      case t if isSubtype(t, localTypeOf[java.lang.Double]) => BoxedDoubleEncoder
      case t if isSubtype(t, localTypeOf[Array[Byte]]) => BinaryEncoder

      // Enums
      case t if isSubtype(t, localTypeOf[java.lang.Enum[_]]) =>
        JavaEnumEncoder(ClassTag(getClassFromType(t)))
      case t if isSubtype(t, localTypeOf[Enumeration#Value]) =>
        // package example
        // object Foo extends Enumeration {
        //  type Foo = Value
        //  val E1, E2 = Value
        // }
        // the fullName of tpe is example.Foo.Foo, but we need example.Foo so that
        // we can call example.Foo.withName to deserialize string to enumeration.
        val parent = getClassFromType(t.asInstanceOf[TypeRef].pre)
        ScalaEnumEncoder(parent, ClassTag(getClassFromType(t)))

      // Leaf encoders
      case t if isSubtype(t, localTypeOf[String]) => StringEncoder
      case t if isSubtype(t, localTypeOf[Decimal]) => SparkDecimalEncoder
      case t if isSubtype(t, localTypeOf[BigDecimal]) => ScalaDecimalEncoder
      case t if isSubtype(t, localTypeOf[java.math.BigDecimal]) => JavaDecimalEncoder
      case t if isSubtype(t, localTypeOf[BigInt]) => ScalaBigIntEncoder
      case t if isSubtype(t, localTypeOf[java.math.BigInteger]) => JavaBigIntEncoder
      case t if isSubtype(t, localTypeOf[CalendarInterval]) => CalendarIntervalEncoder
      case t if isSubtype(t, localTypeOf[java.time.Duration]) => DayTimeIntervalEncoder
      case t if isSubtype(t, localTypeOf[java.time.Period]) => YearMonthIntervalEncoder
      case t if isSubtype(t, localTypeOf[java.sql.Date]) => DateEncoder
      case t if isSubtype(t, localTypeOf[java.time.LocalDate]) => LocalDateEncoder
      case t if isSubtype(t, localTypeOf[java.sql.Timestamp]) => TimestampEncoder
      case t if isSubtype(t, localTypeOf[java.time.Instant]) => InstantEncoder
      case t if isSubtype(t, localTypeOf[java.time.LocalDateTime]) => LocalDateTimeEncoder

      // UDT encoders
      case t if t.typeSymbol.annotations.exists(_.tree.tpe =:= typeOf[SQLUserDefinedType]) =>
        val udt = getClassFromType(t).getAnnotation(classOf[SQLUserDefinedType]).udt().
          getConstructor().newInstance().asInstanceOf[UserDefinedType[Any]]
        val udtClass = udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()
        UDTEncoder(udt, udtClass)

      case t if UDTRegistration.exists(getClassNameFromType(t)) =>
        val udt = UDTRegistration.getUDTFor(getClassNameFromType(t)).get.getConstructor().
          newInstance().asInstanceOf[UserDefinedType[Any]]
        UDTEncoder(udt, udt.getClass)

      // Complex encoders
      case t if isSubtype(t, localTypeOf[Option[_]]) =>
        val TypeRef(_, _, Seq(optType)) = t
        val encoder = encoderFor(
          optType,
          seenTypeSet,
          path.recordOption(getClassNameFromType(optType)))
        OptionEncoder(encoder)

      case t if isSubtype(t, localTypeOf[Array[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        val encoder = encoderFor(
          elementType,
          seenTypeSet,
          path.recordArray(getClassNameFromType(elementType)))
        ArrayEncoder(encoder)

      case t if isSubtype(t, localTypeOf[scala.collection.Seq[_]]) =>
        createIterableEncoder(t, classOf[scala.collection.Seq[_]])

      case t if isSubtype(t, localTypeOf[scala.collection.Set[_]]) =>
        createIterableEncoder(t, classOf[scala.collection.Set[_]])

      case t if isSubtype(t, localTypeOf[Map[_, _]]) =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t
        val keyEncoder = encoderFor(
          keyType,
          seenTypeSet,
          path.recordKeyForMap(getClassNameFromType(keyType)))
        val valueEncoder = encoderFor(
          valueType,
          seenTypeSet,
          path.recordValueForMap(getClassNameFromType(valueType)))
        MapEncoder(ClassTag(getClassFromType(t)), keyEncoder, valueEncoder)

      case t if definedByConstructorParams(t) =>
        if (seenTypeSet.contains(t)) {
          throw QueryExecutionErrors.cannotHaveCircularReferencesInClassError(t.toString)
        }
        val params = getConstructorParameters(t).map {
          case (fieldName, fieldType) =>
            if (SourceVersion.isKeyword(fieldName) ||
              !SourceVersion.isIdentifier(encodeFieldNameToIdentifier(fieldName))) {
              throw QueryExecutionErrors.cannotUseInvalidJavaIdentifierAsFieldNameError(
                fieldName,
                path)
            }
            val encoder = encoderFor(
              fieldType,
              seenTypeSet + t,
              path.recordField(getClassNameFromType(fieldType), fieldName))
            EncoderField(fieldName, encoder)
        }
        ProductEncoder(ClassTag(getClassFromType(t)), params)
      case _ =>
        throw QueryExecutionErrors.cannotFindEncoderForTypeError(tpe.toString)
    }
  }
}

/**
 * Support for generating catalyst schemas for scala objects.  Note that unlike its companion
 * object, this trait able to work in both the runtime and the compile time (macro) universe.
 */
trait ScalaReflection extends Logging {
  /** The universe we work in (runtime or macro) */
  val universe: scala.reflect.api.Universe

  /** The mirror used to access types in the universe */
  def mirror: universe.Mirror

  import universe._

  /**
   * Any codes calling `scala.reflect.api.Types.TypeApi.<:<` should be wrapped by this method to
   * clean up the Scala reflection garbage automatically. Otherwise, it will leak some objects to
   * `scala.reflect.runtime.JavaUniverse.undoLog`.
   *
   * @see https://github.com/scala/bug/issues/8302
   */
  def cleanUpReflectionObjects[T](func: => T): T = {
    universe.asInstanceOf[scala.reflect.runtime.JavaUniverse].undoLog.undo(func)
  }

  /**
   * Return the Scala Type for `T` in the current classloader mirror.
   *
   * Use this method instead of the convenience method `universe.typeOf`, which
   * assumes that all types can be found in the classloader that loaded scala-reflect classes.
   * That's not necessarily the case when running using Eclipse launchers or even
   * Sbt console or test (without `fork := true`).
   *
   * @see SPARK-5281
   */
  def localTypeOf[T: TypeTag]: `Type` = {
    val tag = implicitly[TypeTag[T]]
    tag.in(mirror).tpe.dealias
  }

  private def isValueClass(tpe: Type): Boolean = {
    tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isDerivedValueClass
  }

  /** Returns the name and type of the underlying parameter of value class `tpe`. */
  private def getUnderlyingTypeOfValueClass(tpe: `Type`): Type = {
    getConstructorParameters(tpe).head._2
  }

  /**
   * Returns the parameter names and types for the primary constructor of this type.
   *
   * Note that it only works for scala classes with primary constructor, and currently doesn't
   * support inner class.
   */
  def getConstructorParameters(tpe: Type): Seq[(String, Type)] = {
    val dealiasedTpe = tpe.dealias
    val formalTypeArgs = dealiasedTpe.typeSymbol.asClass.typeParams
    val TypeRef(_, _, actualTypeArgs) = dealiasedTpe
    val params = constructParams(dealiasedTpe)
    params.map { p =>
      val paramTpe = p.typeSignature
      if (isValueClass(paramTpe)) {
        // Replace value class with underlying type
        p.name.decodedName.toString -> getUnderlyingTypeOfValueClass(paramTpe)
      } else {
        p.name.decodedName.toString -> paramTpe.substituteTypes(formalTypeArgs, actualTypeArgs)
      }
    }
  }

  /**
   * If our type is a Scala trait it may have a companion object that
   * only defines a constructor via `apply` method.
   */
  private def getCompanionConstructor(tpe: Type): Symbol = {
    def throwUnsupportedOperation = {
      throw QueryExecutionErrors.cannotFindConstructorForTypeError(tpe.toString)
    }
    tpe.typeSymbol.asClass.companion match {
      case NoSymbol => throwUnsupportedOperation
      case sym => sym.asTerm.typeSignature.member(universe.TermName("apply")) match {
        case NoSymbol => throwUnsupportedOperation
        case constructorSym => constructorSym
      }
    }
  }

  protected def constructParams(tpe: Type): Seq[Symbol] = {
    val constructorSymbol = tpe.member(termNames.CONSTRUCTOR) match {
      case NoSymbol => getCompanionConstructor(tpe)
      case sym => sym
    }
    val params = if (constructorSymbol.isMethod) {
      constructorSymbol.asMethod.paramLists
    } else {
      // Find the primary constructor, and use its parameter ordering.
      val primaryConstructorSymbol: Option[Symbol] = constructorSymbol.asTerm.alternatives.find(
        s => s.isMethod && s.asMethod.isPrimaryConstructor)
      if (primaryConstructorSymbol.isEmpty) {
        throw QueryExecutionErrors.primaryConstructorNotFoundError(tpe.getClass)
      } else {
        primaryConstructorSymbol.get.asMethod.paramLists
      }
    }
    params.flatten
  }

}

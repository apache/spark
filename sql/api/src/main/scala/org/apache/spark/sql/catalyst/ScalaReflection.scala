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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, OuterScopes}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, VariantVal}

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
   * Synchronize to prevent concurrent usage of `<:<` operator. This operator is not thread safe
   * in any current version of scala; i.e. (2.11.12, 2.12.10, 2.13.0-M5).
   *
   * See https://github.com/scala/bug/issues/10766
   */
  private[catalyst] def isSubtype(tpe1: `Type`, tpe2: `Type`): Boolean = {
    ScalaSubtypeLock.synchronized {
      tpe1 <:< tpe2
    }
  }

  private def baseType(tpe: `Type`): `Type` = {
    tpe.dealias match {
      case annotatedType: AnnotatedType => annotatedType.underlying
      case other => other
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
   * `ClassSymbol.selfType` can throw an exception in case of cyclic annotation reference in Java
   * classes. A retry of this operation will succeed as the class which defines the cycle is now
   * resolved. It can however expose further recursive annotation references, so we keep retrying
   * until we exhaust our retry threshold. Default threshold is set to 5 to allow for a few level
   * of cyclic references.
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
   * Returns the full class name for a type. The returned name is the canonical Scala name, where
   * each component is separated by a period. It is NOT the Java-equivalent runtime name (no
   * dollar signs).
   *
   * In simple cases, both the Scala and Java names are the same, however when Scala generates
   * constructs that do not map to a Java equivalent, such as singleton objects or nested classes
   * in package objects, it uses the dollar sign ($) to create synthetic classes, emulating
   * behaviour in Java bytecode.
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

  /**
   * Returns a catalyst DataType and its nullability for the given Scala Type using reflection.
   */
  def schemaFor[T: TypeTag]: Schema = schemaFor(localTypeOf[T])

  /**
   * Returns a catalyst DataType and its nullability for the given Scala Type using reflection.
   */
  def schemaFor(tpe: `Type`): Schema = {
    val enc = encoderFor(tpe)
    Schema(enc.dataType, enc.nullable)
  }

  /**
   * Finds an accessible constructor with compatible parameters. This is a more flexible search
   * than the exact matching algorithm in `Class.getConstructor`. The first assignment-compatible
   * matching constructor is returned if it exists. Otherwise, we check for additional compatible
   * constructors defined in the companion object as `apply` methods. Otherwise, it returns
   * `None`.
   */
  def findConstructor[T](cls: Class[T], paramTypes: Seq[Class[_]]): Option[Seq[AnyRef] => T] = {
    Option(ConstructorUtils.getMatchingAccessibleConstructor(cls, paramTypes: _*)) match {
      case Some(c) => Some(x => c.newInstance(x: _*))
      case None =>
        val companion = mirror.staticClass(cls.getName).companion
        val moduleMirror = mirror.reflectModule(companion.asModule)
        val applyMethods = companion.asTerm.typeSignature
          .member(universe.TermName("apply"))
          .asTerm
          .alternatives
        applyMethods
          .find { method =>
            val params = method.typeSignature.paramLists.head
            // Check that the needed params are the same length and of matching types
            params.size == paramTypes.size &&
            params.zip(paramTypes).forall { case (ps, pc) =>
              ps.typeSignature.typeSymbol == mirror.classSymbol(pc)
            }
          }
          .map { applyMethodSymbol =>
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
      case t if isSubtype(t, localTypeOf[Option[_]]) =>
        definedByConstructorParams(t.typeArgs.head)
      case _ =>
        isSubtype(tpe.dealias, localTypeOf[Product]) ||
        isSubtype(tpe.dealias, localTypeOf[DefinedByConstructorParams])
    }
  }

  def encodeFieldNameToIdentifier(fieldName: String): String = {
    TermName(fieldName).encodedName.toString
  }

  /**
   * Create an [[AgnosticEncoder]] from a [[TypeTag]].
   *
   * If the given type is not supported, i.e. there is no encoder can be built for this type, an
   * [[SparkUnsupportedOperationException]] will be thrown with detailed error message to explain
   * the type path walked so far and which class we are not supporting. There are 4 kinds of type
   * path: * the root type: `root class: "abc.xyz.MyClass"` * the value type of [[Option]]:
   * `option value class: "abc.xyz.MyClass"` * the element type of [[Array]] or [[Seq]]: `array
   * element class: "abc.xyz.MyClass"` * the field of [[Product]]: `field (class:
   * "abc.xyz.MyClass", name: "myField")`
   */
  def encoderFor[E: TypeTag]: AgnosticEncoder[E] = {
    encoderFor(typeTag[E].in(mirror).tpe).asInstanceOf[AgnosticEncoder[E]]
  }

  /**
   * Same as [[encoderFor]] but with extended support to return [[UnboundRowEncoder]] for [[Row]]
   * type.
   */
  def encoderForWithRowEncoderSupport[E: TypeTag]: AgnosticEncoder[E] = {
    encoderFor(typeTag[E].in(mirror).tpe, isRowEncoderSupported = true)
      .asInstanceOf[AgnosticEncoder[E]]
  }

  /**
   * Create an [[AgnosticEncoder]] for a [[Type]].
   */
  def encoderFor(tpe: `Type`, isRowEncoderSupported: Boolean = false): AgnosticEncoder[_] =
    cleanUpReflectionObjects {
      val clsName = getClassNameFromType(tpe)
      val walkedTypePath = WalkedTypePath().recordRoot(clsName)
      encoderFor(tpe, Set.empty, walkedTypePath, isRowEncoderSupported)
    }

  private def encoderFor(
      tpe: `Type`,
      seenTypeSet: Set[`Type`],
      path: WalkedTypePath,
      isRowEncoderSupported: Boolean): AgnosticEncoder[_] = {
    def createIterableEncoder(t: `Type`, fallbackClass: Class[_]): AgnosticEncoder[_] = {
      val TypeRef(_, _, Seq(elementType)) = t
      val encoder = encoderFor(
        elementType,
        seenTypeSet,
        path.recordArray(getClassNameFromType(elementType)),
        isRowEncoderSupported)
      val companion = t.dealias.typeSymbol.companion.typeSignature
      val targetClass = companion.member(TermName("newBuilder")) match {
        case NoSymbol => fallbackClass
        case _ => mirror.runtimeClass(t.typeSymbol.asClass)
      }
      IterableEncoder(
        ClassTag(targetClass),
        encoder,
        encoder.nullable,
        lenientSerialization = false)
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
      case t if isSubtype(t, localTypeOf[Decimal]) => DEFAULT_SPARK_DECIMAL_ENCODER
      case t if isSubtype(t, localTypeOf[BigDecimal]) => DEFAULT_SCALA_DECIMAL_ENCODER
      case t if isSubtype(t, localTypeOf[java.math.BigDecimal]) => DEFAULT_JAVA_DECIMAL_ENCODER
      case t if isSubtype(t, localTypeOf[BigInt]) => ScalaBigIntEncoder
      case t if isSubtype(t, localTypeOf[java.math.BigInteger]) => JavaBigIntEncoder
      case t if isSubtype(t, localTypeOf[CalendarInterval]) => CalendarIntervalEncoder
      case t if isSubtype(t, localTypeOf[java.time.Duration]) => DayTimeIntervalEncoder
      case t if isSubtype(t, localTypeOf[java.time.Period]) => YearMonthIntervalEncoder
      case t if isSubtype(t, localTypeOf[java.sql.Date]) => STRICT_DATE_ENCODER
      case t if isSubtype(t, localTypeOf[java.time.LocalDate]) => STRICT_LOCAL_DATE_ENCODER
      case t if isSubtype(t, localTypeOf[java.sql.Timestamp]) => STRICT_TIMESTAMP_ENCODER
      case t if isSubtype(t, localTypeOf[java.time.Instant]) => STRICT_INSTANT_ENCODER
      case t if isSubtype(t, localTypeOf[java.time.LocalDateTime]) => LocalDateTimeEncoder
      case t if isSubtype(t, localTypeOf[java.time.LocalTime]) => LocalTimeEncoder
      case t if isSubtype(t, localTypeOf[VariantVal]) => VariantEncoder
      case t if isSubtype(t, localTypeOf[Row]) => UnboundRowEncoder

      // UDT encoders
      case t if t.typeSymbol.annotations.exists(_.tree.tpe =:= typeOf[SQLUserDefinedType]) =>
        val udt = getClassFromType(t)
          .getAnnotation(classOf[SQLUserDefinedType])
          .udt()
          .getConstructor()
          .newInstance()
          .asInstanceOf[UserDefinedType[Any]]
        val udtClass = udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()
        UDTEncoder(udt, udtClass)

      case t if UDTRegistration.exists(getClassNameFromType(t)) =>
        val udt = UDTRegistration
          .getUDTFor(getClassNameFromType(t))
          .get
          .getConstructor()
          .newInstance()
          .asInstanceOf[UserDefinedType[Any]]
        UDTEncoder(udt, udt.getClass)

      // Complex encoders
      case t if isSubtype(t, localTypeOf[Option[_]]) =>
        val TypeRef(_, _, Seq(optType)) = t
        val encoder = encoderFor(
          optType,
          seenTypeSet,
          path.recordOption(getClassNameFromType(optType)),
          isRowEncoderSupported)
        OptionEncoder(encoder)

      case t if isSubtype(t, localTypeOf[Array[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        val encoder = encoderFor(
          elementType,
          seenTypeSet,
          path.recordArray(getClassNameFromType(elementType)),
          isRowEncoderSupported)
        ArrayEncoder(encoder, encoder.nullable)

      case t if isSubtype(t, localTypeOf[scala.collection.Seq[_]]) =>
        createIterableEncoder(t, classOf[scala.collection.Seq[_]])

      case t if isSubtype(t, localTypeOf[scala.collection.Set[_]]) =>
        createIterableEncoder(t, classOf[scala.collection.Set[_]])

      case t if isSubtype(t, localTypeOf[Map[_, _]]) =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t
        val keyEncoder = encoderFor(
          keyType,
          seenTypeSet,
          path.recordKeyForMap(getClassNameFromType(keyType)),
          isRowEncoderSupported)
        val valueEncoder = encoderFor(
          valueType,
          seenTypeSet,
          path.recordValueForMap(getClassNameFromType(valueType)),
          isRowEncoderSupported)
        MapEncoder(ClassTag(getClassFromType(t)), keyEncoder, valueEncoder, valueEncoder.nullable)

      case t if definedByConstructorParams(t) =>
        if (seenTypeSet.contains(t)) {
          throw ExecutionErrors.cannotHaveCircularReferencesInClassError(t.toString)
        }
        val params = getConstructorParameters(t).map { case (fieldName, fieldType) =>
          if (SourceVersion.isKeyword(fieldName) ||
            !SourceVersion.isIdentifier(encodeFieldNameToIdentifier(fieldName))) {
            throw ExecutionErrors.cannotUseInvalidJavaIdentifierAsFieldNameError(fieldName, path)
          }
          val encoder = encoderFor(
            fieldType,
            seenTypeSet + t,
            path.recordField(getClassNameFromType(fieldType), fieldName),
            isRowEncoderSupported)
          EncoderField(fieldName, encoder, encoder.nullable, Metadata.empty)
        }
        val cls = getClassFromType(t)
        ProductEncoder(ClassTag(cls), params, Option(OuterScopes.getOuterScope(cls)))
      case _ =>
        throw ExecutionErrors.cannotFindEncoderForTypeError(tpe.toString)
    }
  }
}

/**
 * Support for generating catalyst schemas for scala objects. Note that unlike its companion
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
   * @see
   *   https://github.com/scala/bug/issues/8302
   */
  def cleanUpReflectionObjects[T](func: => T): T = {
    universe.asInstanceOf[scala.reflect.runtime.JavaUniverse].undoLog.undo(func)
  }

  /**
   * Return the Scala Type for `T` in the current classloader mirror.
   *
   * Use this method instead of the convenience method `universe.typeOf`, which assumes that all
   * types can be found in the classloader that loaded scala-reflect classes. That's not
   * necessarily the case when running using Eclipse launchers or even Sbt console or test
   * (without `fork := true`).
   *
   * @see
   *   SPARK-5281
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
   * If our type is a Scala trait it may have a companion object that only defines a constructor
   * via `apply` method.
   */
  private def getCompanionConstructor(tpe: Type): Symbol = {
    def throwUnsupportedOperation = {
      throw ExecutionErrors.cannotFindConstructorForTypeError(tpe.toString)
    }
    tpe.typeSymbol.asClass.companion match {
      case NoSymbol => throwUnsupportedOperation
      case sym =>
        sym.asTerm.typeSignature.member(universe.TermName("apply")) match {
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
      val primaryConstructorSymbol: Option[Symbol] =
        constructorSymbol.asTerm.alternatives.find(s =>
          s.isMethod && s.asMethod.isPrimaryConstructor)
      if (primaryConstructorSymbol.isEmpty) {
        throw ExecutionErrors.primaryConstructorNotFoundError(tpe.getClass)
      } else {
        primaryConstructorSymbol.get.asMethod.paramLists
      }
    }
    params.flatten
  }

}

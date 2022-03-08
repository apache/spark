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

import org.apache.commons.lang3.reflect.ConstructorUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.DeserializerBuildHelper._
import org.apache.spark.sql.catalyst.SerializerBuildHelper._
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
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
   * Returns the Spark SQL DataType for a given scala type.  Where this is not an exact mapping
   * to a native type, an ObjectType is returned. Special handling is also used for Arrays including
   * those that hold primitive types.
   *
   * Unlike `schemaFor`, this function doesn't do any massaging of types into the Spark SQL type
   * system.  As a result, ObjectType will be returned for things like boxed Integers
   */
  def dataTypeFor[T : TypeTag]: DataType = dataTypeFor(localTypeOf[T])

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

  private def dataTypeFor(tpe: `Type`): DataType = cleanUpReflectionObjects {
    tpe.dealias match {
      case t if isSubtype(t, definitions.NullTpe) => NullType
      case t if isSubtype(t, definitions.IntTpe) => IntegerType
      case t if isSubtype(t, definitions.LongTpe) => LongType
      case t if isSubtype(t, definitions.DoubleTpe) => DoubleType
      case t if isSubtype(t, definitions.FloatTpe) => FloatType
      case t if isSubtype(t, definitions.ShortTpe) => ShortType
      case t if isSubtype(t, definitions.ByteTpe) => ByteType
      case t if isSubtype(t, definitions.BooleanTpe) => BooleanType
      case t if isSubtype(t, localTypeOf[Array[Byte]]) => BinaryType
      case t if isSubtype(t, localTypeOf[CalendarInterval]) => CalendarIntervalType
      case t if isSubtype(t, localTypeOf[Decimal]) => DecimalType.SYSTEM_DEFAULT
      case _ =>
        val className = getClassNameFromType(tpe)
        className match {
          case "scala.Array" =>
            val TypeRef(_, _, Seq(elementType)) = tpe.dealias
            arrayClassFor(elementType)
          case other =>
            val clazz = getClassFromType(tpe)
            ObjectType(clazz)
        }
    }
  }

  /**
   * Given a type `T` this function constructs `ObjectType` that holds a class of type
   * `Array[T]`.
   *
   * Special handling is performed for primitive types to map them back to their raw
   * JVM form instead of the Scala Array that handles auto boxing.
   */
  private def arrayClassFor(tpe: `Type`): ObjectType = cleanUpReflectionObjects {
    val cls = tpe.dealias match {
      case t if isSubtype(t, definitions.IntTpe) => classOf[Array[Int]]
      case t if isSubtype(t, definitions.LongTpe) => classOf[Array[Long]]
      case t if isSubtype(t, definitions.DoubleTpe) => classOf[Array[Double]]
      case t if isSubtype(t, definitions.FloatTpe) => classOf[Array[Float]]
      case t if isSubtype(t, definitions.ShortTpe) => classOf[Array[Short]]
      case t if isSubtype(t, definitions.ByteTpe) => classOf[Array[Byte]]
      case t if isSubtype(t, definitions.BooleanTpe) => classOf[Array[Boolean]]
      case t if isSubtype(t, localTypeOf[Array[Byte]]) => classOf[Array[Array[Byte]]]
      case t if isSubtype(t, localTypeOf[CalendarInterval]) => classOf[Array[CalendarInterval]]
      case t if isSubtype(t, localTypeOf[Decimal]) => classOf[Array[Decimal]]
      case other =>
        // There is probably a better way to do this, but I couldn't find it...
        val elementType = dataTypeFor(other).asInstanceOf[ObjectType].cls
        java.lang.reflect.Array.newInstance(elementType, 0).getClass

    }
    ObjectType(cls)
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
   * Returns an expression that can be used to deserialize a Spark SQL representation to an object
   * of type `T` with a compatible schema. The Spark SQL representation is located at ordinal 0 of
   * a row, i.e., `GetColumnByOrdinal(0, _)`. Nested classes will have their fields accessed using
   * `UnresolvedExtractValue`.
   *
   * The returned expression is used by `ExpressionEncoder`. The encoder will resolve and bind this
   * deserializer expression when using it.
   */
  def deserializerForType(tpe: `Type`): Expression = {
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = new WalkedTypePath().recordRoot(clsName)
    val Schema(dataType, nullable) = schemaFor(tpe)

    // Assumes we are deserializing the first column of a row.
    deserializerForWithNullSafetyAndUpcast(GetColumnByOrdinal(0, dataType), dataType,
      nullable = nullable, walkedTypePath,
      (casted, typePath) => deserializerFor(tpe, casted, typePath))
  }

  /**
   * Returns an expression that can be used to deserialize an input expression to an object of type
   * `T` with a compatible schema.
   *
   * @param tpe The `Type` of deserialized object.
   * @param path The expression which can be used to extract serialized value.
   * @param walkedTypePath The paths from top to bottom to access current field when deserializing.
   */
  private def deserializerFor(
      tpe: `Type`,
      path: Expression,
      walkedTypePath: WalkedTypePath): Expression = cleanUpReflectionObjects {
    baseType(tpe) match {
      case t if !dataTypeFor(t).isInstanceOf[ObjectType] => path

      case t if isSubtype(t, localTypeOf[Option[_]]) =>
        val TypeRef(_, _, Seq(optType)) = t
        val className = getClassNameFromType(optType)
        val newTypePath = walkedTypePath.recordOption(className)
        WrapOption(deserializerFor(optType, path, newTypePath), dataTypeFor(optType))

      case t if isSubtype(t, localTypeOf[java.lang.Integer]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Integer])

      case t if isSubtype(t, localTypeOf[java.lang.Long]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Long])

      case t if isSubtype(t, localTypeOf[java.lang.Double]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Double])

      case t if isSubtype(t, localTypeOf[java.lang.Float]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Float])

      case t if isSubtype(t, localTypeOf[java.lang.Short]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Short])

      case t if isSubtype(t, localTypeOf[java.lang.Byte]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Byte])

      case t if isSubtype(t, localTypeOf[java.lang.Boolean]) =>
        createDeserializerForTypesSupportValueOf(path,
          classOf[java.lang.Boolean])

      case t if isSubtype(t, localTypeOf[java.time.LocalDate]) =>
        createDeserializerForLocalDate(path)

      case t if isSubtype(t, localTypeOf[java.sql.Date]) =>
        createDeserializerForSqlDate(path)

      case t if isSubtype(t, localTypeOf[java.time.Instant]) =>
        createDeserializerForInstant(path)

      case t if isSubtype(t, localTypeOf[java.lang.Enum[_]]) =>
        createDeserializerForTypesSupportValueOf(
          Invoke(path, "toString", ObjectType(classOf[String]), returnNullable = false),
          getClassFromType(t))

      case t if isSubtype(t, localTypeOf[java.sql.Timestamp]) =>
        createDeserializerForSqlTimestamp(path)

      case t if isSubtype(t, localTypeOf[java.time.LocalDateTime]) =>
        createDeserializerForLocalDateTime(path)

      case t if isSubtype(t, localTypeOf[java.time.Duration]) =>
        createDeserializerForDuration(path)

      case t if isSubtype(t, localTypeOf[java.time.Period]) =>
        createDeserializerForPeriod(path)

      case t if isSubtype(t, localTypeOf[java.lang.String]) =>
        createDeserializerForString(path, returnNullable = false)

      case t if isSubtype(t, localTypeOf[java.math.BigDecimal]) =>
        createDeserializerForJavaBigDecimal(path, returnNullable = false)

      case t if isSubtype(t, localTypeOf[BigDecimal]) =>
        createDeserializerForScalaBigDecimal(path, returnNullable = false)

      case t if isSubtype(t, localTypeOf[java.math.BigInteger]) =>
        createDeserializerForJavaBigInteger(path, returnNullable = false)

      case t if isSubtype(t, localTypeOf[scala.math.BigInt]) =>
        createDeserializerForScalaBigInt(path)

      case t if isSubtype(t, localTypeOf[Array[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        val Schema(dataType, elementNullable) = schemaFor(elementType)
        val className = getClassNameFromType(elementType)
        val newTypePath = walkedTypePath.recordArray(className)

        val mapFunction: Expression => Expression = element => {
          // upcast the array element to the data type the encoder expected.
          deserializerForWithNullSafetyAndUpcast(
            element,
            dataType,
            nullable = elementNullable,
            newTypePath,
            (casted, typePath) => deserializerFor(elementType, casted, typePath))
        }

        val arrayData = UnresolvedMapObjects(mapFunction, path)
        val arrayCls = arrayClassFor(elementType)

        val methodName = elementType match {
          case t if isSubtype(t, definitions.IntTpe) => "toIntArray"
          case t if isSubtype(t, definitions.LongTpe) => "toLongArray"
          case t if isSubtype(t, definitions.DoubleTpe) => "toDoubleArray"
          case t if isSubtype(t, definitions.FloatTpe) => "toFloatArray"
          case t if isSubtype(t, definitions.ShortTpe) => "toShortArray"
          case t if isSubtype(t, definitions.ByteTpe) => "toByteArray"
          case t if isSubtype(t, definitions.BooleanTpe) => "toBooleanArray"
          // non-primitive
          case _ => "array"
        }
        Invoke(arrayData, methodName, arrayCls, returnNullable = false)

      // We serialize a `Set` to Catalyst array. When we deserialize a Catalyst array
      // to a `Set`, if there are duplicated elements, the elements will be de-duplicated.
      case t if isSubtype(t, localTypeOf[scala.collection.Seq[_]]) ||
          isSubtype(t, localTypeOf[scala.collection.Set[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        val Schema(dataType, elementNullable) = schemaFor(elementType)
        val className = getClassNameFromType(elementType)
        val newTypePath = walkedTypePath.recordArray(className)

        val mapFunction: Expression => Expression = element => {
          deserializerForWithNullSafetyAndUpcast(
            element,
            dataType,
            nullable = elementNullable,
            newTypePath,
            (casted, typePath) => deserializerFor(elementType, casted, typePath))
        }

        val companion = t.dealias.typeSymbol.companion.typeSignature
        val cls = companion.member(TermName("newBuilder")) match {
          case NoSymbol if isSubtype(t, localTypeOf[Seq[_]]) => classOf[Seq[_]]
          case NoSymbol if isSubtype(t, localTypeOf[scala.collection.Set[_]]) =>
            classOf[scala.collection.Set[_]]
          case _ => mirror.runtimeClass(t.typeSymbol.asClass)
        }
        UnresolvedMapObjects(mapFunction, path, Some(cls))

      case t if isSubtype(t, localTypeOf[Map[_, _]]) =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t

        val classNameForKey = getClassNameFromType(keyType)
        val classNameForValue = getClassNameFromType(valueType)

        val newTypePath = walkedTypePath.recordMap(classNameForKey, classNameForValue)

        UnresolvedCatalystToExternalMap(
          path,
          p => deserializerFor(keyType, p, newTypePath),
          p => deserializerFor(valueType, p, newTypePath),
          mirror.runtimeClass(t.typeSymbol.asClass)
        )

      case t if t.typeSymbol.annotations.exists(_.tree.tpe =:= typeOf[SQLUserDefinedType]) =>
        val udt = getClassFromType(t).getAnnotation(classOf[SQLUserDefinedType]).udt().
          getConstructor().newInstance()
        val obj = NewInstance(
          udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt(),
          Nil,
          dataType = ObjectType(udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()))
        Invoke(obj, "deserialize", ObjectType(udt.userClass), path :: Nil)

      case t if UDTRegistration.exists(getClassNameFromType(t)) =>
        val udt = UDTRegistration.getUDTFor(getClassNameFromType(t)).get.getConstructor().
          newInstance().asInstanceOf[UserDefinedType[_]]
        val obj = NewInstance(
          udt.getClass,
          Nil,
          dataType = ObjectType(udt.getClass))
        Invoke(obj, "deserialize", ObjectType(udt.userClass), path :: Nil)

      case t if definedByConstructorParams(t) =>
        val params = getConstructorParameters(t)

        val cls = getClassFromType(tpe)

        val arguments = params.zipWithIndex.map { case ((fieldName, fieldType), i) =>
          val Schema(dataType, nullable) = schemaFor(fieldType)
          val clsName = getClassNameFromType(fieldType)
          val newTypePath = walkedTypePath.recordField(clsName, fieldName)

          // For tuples, we based grab the inner fields by ordinal instead of name.
          val newPath = if (cls.getName startsWith "scala.Tuple") {
            deserializerFor(
              fieldType,
              addToPathOrdinal(path, i, dataType, newTypePath),
              newTypePath)
          } else {
            deserializerFor(
              fieldType,
              addToPath(path, fieldName, dataType, newTypePath),
              newTypePath)
          }
          expressionWithNullSafety(
            newPath,
            nullable = nullable,
            newTypePath)
        }

        val newInstance = NewInstance(cls, arguments, ObjectType(cls), propagateNull = false)

        expressions.If(
          IsNull(path),
          expressions.Literal.create(null, ObjectType(cls)),
          newInstance
        )

      case t if isSubtype(t, localTypeOf[Enumeration#Value]) =>
        // package example
        // object Foo extends Enumeration {
        //  type Foo = Value
        //  val E1, E2 = Value
        // }
        // the fullName of tpe is example.Foo.Foo, but we need example.Foo so that
        // we can call example.Foo.withName to deserialize string to enumeration.
        val parent = t.asInstanceOf[TypeRef].pre.typeSymbol.asClass
        val cls = mirror.runtimeClass(parent)
        StaticInvoke(
          cls,
          ObjectType(getClassFromType(t)),
          "withName",
          createDeserializerForString(path, false) :: Nil,
          returnNullable = false)
    }
  }

  /**
   * Returns an expression for serializing an object of type T to Spark SQL representation. The
   * input object is located at ordinal 0 of a row, i.e., `BoundReference(0, _)`.
   *
   * If the given type is not supported, i.e. there is no encoder can be built for this type,
   * an [[UnsupportedOperationException]] will be thrown with detailed error message to explain
   * the type path walked so far and which class we are not supporting.
   * There are 4 kinds of type path:
   *  * the root type: `root class: "abc.xyz.MyClass"`
   *  * the value type of [[Option]]: `option value class: "abc.xyz.MyClass"`
   *  * the element type of [[Array]] or [[Seq]]: `array element class: "abc.xyz.MyClass"`
   *  * the field of [[Product]]: `field (class: "abc.xyz.MyClass", name: "myField")`
   */
  def serializerForType(tpe: `Type`): Expression = ScalaReflection.cleanUpReflectionObjects {
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = new WalkedTypePath().recordRoot(clsName)

    // The input object to `ExpressionEncoder` is located at first column of an row.
    val isPrimitive = tpe.typeSymbol.asClass.isPrimitive
    val inputObject = BoundReference(0, dataTypeFor(tpe), nullable = !isPrimitive)

    serializerFor(inputObject, tpe, walkedTypePath)
  }

  /**
   * Returns an expression for serializing the value of an input expression into Spark SQL
   * internal representation.
   */
  private def serializerFor(
      inputObject: Expression,
      tpe: `Type`,
      walkedTypePath: WalkedTypePath,
      seenTypeSet: Set[`Type`] = Set.empty): Expression = cleanUpReflectionObjects {

    def toCatalystArray(input: Expression, elementType: `Type`): Expression = {
      dataTypeFor(elementType) match {
        case dt: ObjectType =>
          val clsName = getClassNameFromType(elementType)
          val newPath = walkedTypePath.recordArray(clsName)
          createSerializerForMapObjects(input, dt,
            serializerFor(_, elementType, newPath, seenTypeSet))

        case dt @ (BooleanType | ByteType | ShortType | IntegerType | LongType |
                   FloatType | DoubleType) =>
          val cls = input.dataType.asInstanceOf[ObjectType].cls
          if (cls.isArray && cls.getComponentType.isPrimitive) {
            createSerializerForPrimitiveArray(input, dt)
          } else {
            createSerializerForGenericArray(input, dt, nullable = schemaFor(elementType).nullable)
          }

        case dt =>
          createSerializerForGenericArray(input, dt, nullable = schemaFor(elementType).nullable)
      }
    }
    baseType(tpe) match {
      case _ if !inputObject.dataType.isInstanceOf[ObjectType] => inputObject

      case t if isSubtype(t, localTypeOf[Option[_]]) =>
        val TypeRef(_, _, Seq(optType)) = t
        val className = getClassNameFromType(optType)
        val newPath = walkedTypePath.recordOption(className)
        val unwrapped = UnwrapOption(dataTypeFor(optType), inputObject)
        serializerFor(unwrapped, optType, newPath, seenTypeSet)

      // Since List[_] also belongs to localTypeOf[Product], we put this case before
      // "case t if definedByConstructorParams(t)" to make sure it will match to the
      // case "localTypeOf[Seq[_]]"
      case t if isSubtype(t, localTypeOf[scala.collection.Seq[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        toCatalystArray(inputObject, elementType)
      case t if isSubtype(t, localTypeOf[Array[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        toCatalystArray(inputObject, elementType)

      case t if isSubtype(t, localTypeOf[Map[_, _]]) =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t
        val keyClsName = getClassNameFromType(keyType)
        val valueClsName = getClassNameFromType(valueType)
        val keyPath = walkedTypePath.recordKeyForMap(keyClsName)
        val valuePath = walkedTypePath.recordValueForMap(valueClsName)

        createSerializerForMap(
          inputObject,
          MapElementInformation(
            dataTypeFor(keyType),
            nullable = !keyType.typeSymbol.asClass.isPrimitive,
            serializerFor(_, keyType, keyPath, seenTypeSet)),
          MapElementInformation(
            dataTypeFor(valueType),
            nullable = !valueType.typeSymbol.asClass.isPrimitive,
            serializerFor(_, valueType, valuePath, seenTypeSet))
        )

      case t if isSubtype(t, localTypeOf[scala.collection.Set[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t

        // There's no corresponding Catalyst type for `Set`, we serialize a `Set` to Catalyst array.
        // Note that the property of `Set` is only kept when manipulating the data as domain object.
        val newInput =
          Invoke(
           inputObject,
           "toSeq",
           ObjectType(classOf[Seq[_]]))

        toCatalystArray(newInput, elementType)

      case t if isSubtype(t, localTypeOf[String]) => createSerializerForString(inputObject)

      case t if isSubtype(t, localTypeOf[java.time.Instant]) =>
        createSerializerForJavaInstant(inputObject)

      case t if isSubtype(t, localTypeOf[java.sql.Timestamp]) =>
        createSerializerForSqlTimestamp(inputObject)

      case t if isSubtype(t, localTypeOf[java.time.LocalDateTime]) =>
        createSerializerForLocalDateTime(inputObject)

      case t if isSubtype(t, localTypeOf[java.time.LocalDate]) =>
        createSerializerForJavaLocalDate(inputObject)

      case t if isSubtype(t, localTypeOf[java.sql.Date]) => createSerializerForSqlDate(inputObject)

      case t if isSubtype(t, localTypeOf[java.time.Duration]) =>
        createSerializerForJavaDuration(inputObject)

      case t if isSubtype(t, localTypeOf[java.time.Period]) =>
        createSerializerForJavaPeriod(inputObject)

      case t if isSubtype(t, localTypeOf[BigDecimal]) =>
        createSerializerForScalaBigDecimal(inputObject)

      case t if isSubtype(t, localTypeOf[java.math.BigDecimal]) =>
        createSerializerForJavaBigDecimal(inputObject)

      case t if isSubtype(t, localTypeOf[java.math.BigInteger]) =>
        createSerializerForJavaBigInteger(inputObject)

      case t if isSubtype(t, localTypeOf[java.lang.Enum[_]]) =>
        createSerializerForJavaEnum(inputObject)

      case t if isSubtype(t, localTypeOf[scala.math.BigInt]) =>
        createSerializerForScalaBigInt(inputObject)

      case t if isSubtype(t, localTypeOf[java.lang.Integer]) =>
        createSerializerForInteger(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Long]) => createSerializerForLong(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Double]) =>
        createSerializerForDouble(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Float]) => createSerializerForFloat(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Short]) => createSerializerForShort(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Byte]) => createSerializerForByte(inputObject)
      case t if isSubtype(t, localTypeOf[java.lang.Boolean]) =>
        createSerializerForBoolean(inputObject)

      case t if t.typeSymbol.annotations.exists(_.tree.tpe =:= typeOf[SQLUserDefinedType]) =>
        val udt = getClassFromType(t)
          .getAnnotation(classOf[SQLUserDefinedType]).udt().getConstructor().newInstance()
        val udtClass = udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()
        createSerializerForUserDefinedType(inputObject, udt, udtClass)

      case t if UDTRegistration.exists(getClassNameFromType(t)) =>
        val udt = UDTRegistration.getUDTFor(getClassNameFromType(t)).get.getConstructor().
          newInstance().asInstanceOf[UserDefinedType[_]]
        val udtClass = udt.getClass
        createSerializerForUserDefinedType(inputObject, udt, udtClass)

      case t if definedByConstructorParams(t) =>
        if (seenTypeSet.contains(t)) {
          throw QueryExecutionErrors.cannotHaveCircularReferencesInClassError(t.toString)
        }

        val params = getConstructorParameters(t)
        val fields = params.map { case (fieldName, fieldType) =>
          if (SourceVersion.isKeyword(fieldName) ||
              !SourceVersion.isIdentifier(encodeFieldNameToIdentifier(fieldName))) {
            throw QueryExecutionErrors.cannotUseInvalidJavaIdentifierAsFieldNameError(
              fieldName, walkedTypePath)
          }

          // SPARK-26730 inputObject won't be null with If's guard below. And KnownNotNul
          // is necessary here. Because for a nullable nested inputObject with struct data
          // type, e.g. StructType(IntegerType, StringType), it will return nullable=true
          // for IntegerType without KnownNotNull. And that's what we do not expect to.
          val fieldValue = Invoke(KnownNotNull(inputObject), fieldName, dataTypeFor(fieldType),
            returnNullable = !fieldType.typeSymbol.asClass.isPrimitive)
          val clsName = getClassNameFromType(fieldType)
          val newPath = walkedTypePath.recordField(clsName, fieldName)
          (fieldName, serializerFor(fieldValue, fieldType, newPath, seenTypeSet + t))
        }
        createSerializerForObject(inputObject, fields)

      case t if isSubtype(t, localTypeOf[Enumeration#Value]) =>
        createSerializerForString(
          Invoke(
            inputObject,
            "toString",
            ObjectType(classOf[java.lang.String]),
            returnNullable = false))

      case _ =>
        throw QueryExecutionErrors.cannotFindEncoderForTypeError(tpe.toString, walkedTypePath)
    }
  }

  /**
   * Returns true if the given type is option of product type, e.g. `Option[Tuple2]`. Note that,
   * we also treat [[DefinedByConstructorParams]] as product type.
   */
  def optionOfProductType(tpe: `Type`): Boolean = cleanUpReflectionObjects {
    tpe.dealias match {
      case t if isSubtype(t, localTypeOf[Option[_]]) =>
        val TypeRef(_, _, Seq(optType)) = t
        definedByConstructorParams(optType)
      case _ => false
    }
  }

  /**
   * Returns the parameter names and types for the primary constructor of this class.
   *
   * Note that it only works for scala classes with primary constructor, and currently doesn't
   * support inner class.
   */
  def getConstructorParameters(cls: Class[_]): Seq[(String, Type)] = {
    val m = runtimeMirror(cls.getClassLoader)
    val classSymbol = m.staticClass(cls.getName)
    val t = classSymbol.selfType
    getConstructorParameters(t)
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
    val t = classSymbol.selfType
    constructParams(t).map(_.name.decodedName.toString)
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
  def schemaFor(tpe: `Type`): Schema = cleanUpReflectionObjects {
    baseType(tpe) match {
      // this must be the first case, since all objects in scala are instances of Null, therefore
      // Null type would wrongly match the first of them, which is Option as of now
      case t if isSubtype(t, definitions.NullTpe) => Schema(NullType, nullable = true)
      case t if t.typeSymbol.annotations.exists(_.tree.tpe =:= typeOf[SQLUserDefinedType]) =>
        val udt = getClassFromType(t).getAnnotation(classOf[SQLUserDefinedType]).udt().
          getConstructor().newInstance()
        Schema(udt, nullable = true)
      case t if UDTRegistration.exists(getClassNameFromType(t)) =>
        val udt = UDTRegistration.getUDTFor(getClassNameFromType(t)).get.getConstructor().
          newInstance().asInstanceOf[UserDefinedType[_]]
        Schema(udt, nullable = true)
      case t if isSubtype(t, localTypeOf[Option[_]]) =>
        val TypeRef(_, _, Seq(optType)) = t
        Schema(schemaFor(optType).dataType, nullable = true)
      case t if isSubtype(t, localTypeOf[Array[Byte]]) => Schema(BinaryType, nullable = true)
      case t if isSubtype(t, localTypeOf[Array[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        val Schema(dataType, nullable) = schemaFor(elementType)
        Schema(ArrayType(dataType, containsNull = nullable), nullable = true)
      case t if isSubtype(t, localTypeOf[scala.collection.Seq[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        val Schema(dataType, nullable) = schemaFor(elementType)
        Schema(ArrayType(dataType, containsNull = nullable), nullable = true)
      case t if isSubtype(t, localTypeOf[Map[_, _]]) =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t
        val Schema(valueDataType, valueNullable) = schemaFor(valueType)
        Schema(MapType(schemaFor(keyType).dataType,
          valueDataType, valueContainsNull = valueNullable), nullable = true)
      case t if isSubtype(t, localTypeOf[Set[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        val Schema(dataType, nullable) = schemaFor(elementType)
        Schema(ArrayType(dataType, containsNull = nullable), nullable = true)
      case t if isSubtype(t, localTypeOf[String]) => Schema(StringType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.time.Instant]) =>
        Schema(TimestampType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.sql.Timestamp]) =>
        Schema(TimestampType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.time.LocalDateTime]) =>
        Schema(TimestampNTZType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.time.LocalDate]) => Schema(DateType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.sql.Date]) => Schema(DateType, nullable = true)
      case t if isSubtype(t, localTypeOf[CalendarInterval]) =>
        Schema(CalendarIntervalType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.time.Duration]) =>
        Schema(DayTimeIntervalType(), nullable = true)
      case t if isSubtype(t, localTypeOf[java.time.Period]) =>
        Schema(YearMonthIntervalType(), nullable = true)
      case t if isSubtype(t, localTypeOf[BigDecimal]) =>
        Schema(DecimalType.SYSTEM_DEFAULT, nullable = true)
      case t if isSubtype(t, localTypeOf[java.math.BigDecimal]) =>
        Schema(DecimalType.SYSTEM_DEFAULT, nullable = true)
      case t if isSubtype(t, localTypeOf[java.math.BigInteger]) =>
        Schema(DecimalType.BigIntDecimal, nullable = true)
      case t if isSubtype(t, localTypeOf[scala.math.BigInt]) =>
        Schema(DecimalType.BigIntDecimal, nullable = true)
      case t if isSubtype(t, localTypeOf[Decimal]) =>
        Schema(DecimalType.SYSTEM_DEFAULT, nullable = true)
      case t if isSubtype(t, localTypeOf[java.lang.Integer]) => Schema(IntegerType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.lang.Long]) => Schema(LongType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.lang.Double]) => Schema(DoubleType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.lang.Float]) => Schema(FloatType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.lang.Short]) => Schema(ShortType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.lang.Byte]) => Schema(ByteType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.lang.Boolean]) => Schema(BooleanType, nullable = true)
      case t if isSubtype(t, localTypeOf[java.lang.Enum[_]]) => Schema(StringType, nullable = true)
      case t if isSubtype(t, definitions.IntTpe) => Schema(IntegerType, nullable = false)
      case t if isSubtype(t, definitions.LongTpe) => Schema(LongType, nullable = false)
      case t if isSubtype(t, definitions.DoubleTpe) => Schema(DoubleType, nullable = false)
      case t if isSubtype(t, definitions.FloatTpe) => Schema(FloatType, nullable = false)
      case t if isSubtype(t, definitions.ShortTpe) => Schema(ShortType, nullable = false)
      case t if isSubtype(t, definitions.ByteTpe) => Schema(ByteType, nullable = false)
      case t if isSubtype(t, definitions.BooleanTpe) => Schema(BooleanType, nullable = false)
      case t if definedByConstructorParams(t) =>
        val params = getConstructorParameters(t)
        Schema(StructType(
          params.map { case (fieldName, fieldType) =>
            val Schema(dataType, nullable) = schemaFor(fieldType)
            StructField(fieldName, dataType, nullable)
          }), nullable = true)
      case t if isSubtype(t, localTypeOf[Enumeration#Value]) =>
        Schema(StringType, nullable = true)
      case other =>
        throw QueryExecutionErrors.schemaForTypeUnsupportedError(other.toString)
    }
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
          params.size == paramTypes.tail.size &&
          params.zip(paramTypes.tail).forall { case(ps, pc) =>
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

  val typeJavaMapping = Map[DataType, Class[_]](
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

  val typeBoxedJavaMapping = Map[DataType, Class[_]](
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
    tpe.typeSymbol.asClass.isDerivedValueClass
  }

  private def isTypeParameter(tpe: Type): Boolean = {
    tpe.typeSymbol.isParameter
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
      if (isTypeParameter(paramTpe)) {
        // if there are type variables to fill in, do the substitution
        // (SomeClass[T] -> SomeClass[Int])
        p.name.decodedName.toString -> paramTpe.substituteTypes(formalTypeArgs, actualTypeArgs)
      } else if (isValueClass(paramTpe)) {
        // Replace value class with underlying type
        p.name.decodedName.toString -> getUnderlyingTypeOfValueClass(paramTpe)
      } else {
        p.name.decodedName.toString -> paramTpe
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

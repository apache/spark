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

import org.apache.spark.sql.catalyst.analysis.{UnresolvedExtractValue, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayBasedMapData, DateTimeUtils}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * A default version of ScalaReflection that uses the runtime universe.
 */
object ScalaReflection extends ScalaReflection {
  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  // Since we are creating a runtime mirror usign the class loader of current thread,
  // we need to use def at here. So, every time we call mirror, it is using the
  // class loader of the current thread.
  override def mirror: universe.Mirror =
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)

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

  private def dataTypeFor(tpe: `Type`): DataType = ScalaReflectionLock.synchronized {
    tpe match {
      case t if t <:< definitions.IntTpe => IntegerType
      case t if t <:< definitions.LongTpe => LongType
      case t if t <:< definitions.DoubleTpe => DoubleType
      case t if t <:< definitions.FloatTpe => FloatType
      case t if t <:< definitions.ShortTpe => ShortType
      case t if t <:< definitions.ByteTpe => ByteType
      case t if t <:< definitions.BooleanTpe => BooleanType
      case t if t <:< localTypeOf[Array[Byte]] => BinaryType
      case _ =>
        val className = getClassNameFromType(tpe)
        className match {
          case "scala.Array" =>
            val TypeRef(_, _, Seq(elementType)) = tpe
            arrayClassFor(elementType)
          case other =>
            val clazz = getClassFromType(tpe)
            ObjectType(clazz)
        }
    }
  }

  /**
   * Given a type `T` this function constructs and ObjectType that holds a class of type
   * Array[T].  Special handling is performed for primitive types to map them back to their raw
   * JVM form instead of the Scala Array that handles auto boxing.
   */
  private def arrayClassFor(tpe: `Type`): DataType = ScalaReflectionLock.synchronized {
    val cls = tpe match {
      case t if t <:< definitions.IntTpe => classOf[Array[Int]]
      case t if t <:< definitions.LongTpe => classOf[Array[Long]]
      case t if t <:< definitions.DoubleTpe => classOf[Array[Double]]
      case t if t <:< definitions.FloatTpe => classOf[Array[Float]]
      case t if t <:< definitions.ShortTpe => classOf[Array[Short]]
      case t if t <:< definitions.ByteTpe => classOf[Array[Byte]]
      case t if t <:< definitions.BooleanTpe => classOf[Array[Boolean]]
      case other =>
        // There is probably a better way to do this, but I couldn't find it...
        val elementType = dataTypeFor(other).asInstanceOf[ObjectType].cls
        java.lang.reflect.Array.newInstance(elementType, 1).getClass

    }
    ObjectType(cls)
  }

  /**
   * Returns true if the value of this data type is same between internal and external.
   */
  def isNativeType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BinaryType => true
    case _ => false
  }

  /**
   * Returns an expression that can be used to construct an object of type `T` given an input
   * row with a compatible schema.  Fields of the row will be extracted using UnresolvedAttributes
   * of the same name as the constructor arguments.  Nested classes will have their fields accessed
   * using UnresolvedExtractValue.
   *
   * When used on a primitive type, the constructor will instead default to extracting the value
   * from ordinal 0 (since there are no names to map to).  The actual location can be moved by
   * calling resolve/bind with a new schema.
   */
  def constructorFor[T : TypeTag]: Expression = {
    val tpe = localTypeOf[T]
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = s"""- root class: "${clsName}"""" :: Nil
    constructorFor(tpe, None, walkedTypePath)
  }

  private def constructorFor(
      tpe: `Type`,
      path: Option[Expression],
      walkedTypePath: Seq[String]): Expression = ScalaReflectionLock.synchronized {

    /** Returns the current path with a sub-field extracted. */
    def addToPath(part: String, dataType: DataType, walkedTypePath: Seq[String]): Expression = {
      val newPath = path
        .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
        .getOrElse(UnresolvedAttribute(part))
      upCastToExpectedType(newPath, dataType, walkedTypePath)
    }

    /** Returns the current path with a field at ordinal extracted. */
    def addToPathOrdinal(
        ordinal: Int,
        dataType: DataType,
        walkedTypePath: Seq[String]): Expression = {
      val newPath = path
        .map(p => GetStructField(p, ordinal))
        .getOrElse(BoundReference(ordinal, dataType, false))
      upCastToExpectedType(newPath, dataType, walkedTypePath)
    }

    /** Returns the current path or `BoundReference`. */
    def getPath: Expression = {
      val dataType = schemaFor(tpe).dataType
      if (path.isDefined) {
        path.get
      } else {
        upCastToExpectedType(BoundReference(0, dataType, true), dataType, walkedTypePath)
      }
    }

    /**
     * When we build the `fromRowExpression` for an encoder, we set up a lot of "unresolved" stuff
     * and lost the required data type, which may lead to runtime error if the real type doesn't
     * match the encoder's schema.
     * For example, we build an encoder for `case class Data(a: Int, b: String)` and the real type
     * is [a: int, b: long], then we will hit runtime error and say that we can't construct class
     * `Data` with int and long, because we lost the information that `b` should be a string.
     *
     * This method help us "remember" the required data type by adding a `UpCast`.  Note that we
     * don't need to cast struct type because there must be `UnresolvedExtractValue` or
     * `GetStructField` wrapping it, thus we only need to handle leaf type.
     */
    def upCastToExpectedType(
        expr: Expression,
        expected: DataType,
        walkedTypePath: Seq[String]): Expression = expected match {
      case _: StructType => expr
      case _ => UpCast(expr, expected, walkedTypePath)
    }

    val className = getClassNameFromType(tpe)
    tpe match {
      case t if !dataTypeFor(t).isInstanceOf[ObjectType] => getPath

      case t if t <:< localTypeOf[Option[_]] =>
        val TypeRef(_, _, Seq(optType)) = t
        val className = getClassNameFromType(optType)
        val newTypePath = s"""- option value class: "$className"""" +: walkedTypePath
        WrapOption(constructorFor(optType, path, newTypePath), dataTypeFor(optType))

      case t if t <:< localTypeOf[java.lang.Integer] =>
        val boxedType = classOf[java.lang.Integer]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, objectType)

      case t if t <:< localTypeOf[java.lang.Long] =>
        val boxedType = classOf[java.lang.Long]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, objectType)

      case t if t <:< localTypeOf[java.lang.Double] =>
        val boxedType = classOf[java.lang.Double]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, objectType)

      case t if t <:< localTypeOf[java.lang.Float] =>
        val boxedType = classOf[java.lang.Float]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, objectType)

      case t if t <:< localTypeOf[java.lang.Short] =>
        val boxedType = classOf[java.lang.Short]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, objectType)

      case t if t <:< localTypeOf[java.lang.Byte] =>
        val boxedType = classOf[java.lang.Byte]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, objectType)

      case t if t <:< localTypeOf[java.lang.Boolean] =>
        val boxedType = classOf[java.lang.Boolean]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, objectType)

      case t if t <:< localTypeOf[java.sql.Date] =>
        StaticInvoke(
          DateTimeUtils.getClass,
          ObjectType(classOf[java.sql.Date]),
          "toJavaDate",
          getPath :: Nil,
          propagateNull = true)

      case t if t <:< localTypeOf[java.sql.Timestamp] =>
        StaticInvoke(
          DateTimeUtils.getClass,
          ObjectType(classOf[java.sql.Timestamp]),
          "toJavaTimestamp",
          getPath :: Nil,
          propagateNull = true)

      case t if t <:< localTypeOf[java.lang.String] =>
        Invoke(getPath, "toString", ObjectType(classOf[String]))

      case t if t <:< localTypeOf[java.math.BigDecimal] =>
        Invoke(getPath, "toJavaBigDecimal", ObjectType(classOf[java.math.BigDecimal]))

      case t if t <:< localTypeOf[BigDecimal] =>
        Invoke(getPath, "toBigDecimal", ObjectType(classOf[BigDecimal]))

      case t if t <:< localTypeOf[Array[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t

        // TODO: add runtime null check for primitive array
        val primitiveMethod = elementType match {
          case t if t <:< definitions.IntTpe => Some("toIntArray")
          case t if t <:< definitions.LongTpe => Some("toLongArray")
          case t if t <:< definitions.DoubleTpe => Some("toDoubleArray")
          case t if t <:< definitions.FloatTpe => Some("toFloatArray")
          case t if t <:< definitions.ShortTpe => Some("toShortArray")
          case t if t <:< definitions.ByteTpe => Some("toByteArray")
          case t if t <:< definitions.BooleanTpe => Some("toBooleanArray")
          case _ => None
        }

        primitiveMethod.map { method =>
          Invoke(getPath, method, arrayClassFor(elementType))
        }.getOrElse {
          val className = getClassNameFromType(elementType)
          val newTypePath = s"""- array element class: "$className"""" +: walkedTypePath
          Invoke(
            MapObjects(
              p => constructorFor(elementType, Some(p), newTypePath),
              getPath,
              schemaFor(elementType).dataType),
            "array",
            arrayClassFor(elementType))
        }

      case t if t <:< localTypeOf[Seq[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val Schema(dataType, nullable) = schemaFor(elementType)
        val className = getClassNameFromType(elementType)
        val newTypePath = s"""- array element class: "$className"""" +: walkedTypePath

        val mapFunction: Expression => Expression = p => {
          val converter = constructorFor(elementType, Some(p), newTypePath)
          if (nullable) {
            converter
          } else {
            AssertNotNull(converter, newTypePath)
          }
        }

        val array = Invoke(
          MapObjects(mapFunction, getPath, dataType),
          "array",
          ObjectType(classOf[Array[Any]]))

        StaticInvoke(
          scala.collection.mutable.WrappedArray.getClass,
          ObjectType(classOf[Seq[_]]),
          "make",
          array :: Nil)

      case t if t <:< localTypeOf[Map[_, _]] =>
        // TODO: add walked type path for map
        val TypeRef(_, _, Seq(keyType, valueType)) = t

        val keyData =
          Invoke(
            MapObjects(
              p => constructorFor(keyType, Some(p), walkedTypePath),
              Invoke(getPath, "keyArray", ArrayType(schemaFor(keyType).dataType)),
              schemaFor(keyType).dataType),
            "array",
            ObjectType(classOf[Array[Any]]))

        val valueData =
          Invoke(
            MapObjects(
              p => constructorFor(valueType, Some(p), walkedTypePath),
              Invoke(getPath, "valueArray", ArrayType(schemaFor(valueType).dataType)),
              schemaFor(valueType).dataType),
            "array",
            ObjectType(classOf[Array[Any]]))

        StaticInvoke(
          ArrayBasedMapData.getClass,
          ObjectType(classOf[Map[_, _]]),
          "toScalaMap",
          keyData :: valueData :: Nil)

      case t if t <:< localTypeOf[Product] =>
        val params = getConstructorParameters(t)

        val cls = getClassFromType(tpe)

        val arguments = params.zipWithIndex.map { case ((fieldName, fieldType), i) =>
          val Schema(dataType, nullable) = schemaFor(fieldType)
          val clsName = getClassNameFromType(fieldType)
          val newTypePath = s"""- field (class: "$clsName", name: "$fieldName")""" +: walkedTypePath
          // For tuples, we based grab the inner fields by ordinal instead of name.
          if (cls.getName startsWith "scala.Tuple") {
            constructorFor(
              fieldType,
              Some(addToPathOrdinal(i, dataType, newTypePath)),
              newTypePath)
          } else {
            val constructor = constructorFor(
              fieldType,
              Some(addToPath(fieldName, dataType, newTypePath)),
              newTypePath)

            if (!nullable) {
              AssertNotNull(constructor, newTypePath)
            } else {
              constructor
            }
          }
        }

        val newInstance = NewInstance(cls, arguments, ObjectType(cls), propagateNull = false)

        if (path.nonEmpty) {
          expressions.If(
            IsNull(getPath),
            expressions.Literal.create(null, ObjectType(cls)),
            newInstance
          )
        } else {
          newInstance
        }

      case t if Utils.classIsLoadable(className) &&
        Utils.classForName(className).isAnnotationPresent(classOf[SQLUserDefinedType]) =>
        val udt = Utils.classForName(className)
          .getAnnotation(classOf[SQLUserDefinedType]).udt().newInstance()
        val obj = NewInstance(
          udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt(),
          Nil,
          dataType = ObjectType(udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()))
        Invoke(obj, "deserialize", ObjectType(udt.userClass), getPath :: Nil)
    }
  }

  /**
   * Returns expressions for extracting all the fields from the given type.
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
  def extractorsFor[T : TypeTag](inputObject: Expression): CreateNamedStruct = {
    val tpe = localTypeOf[T]
    val clsName = getClassNameFromType(tpe)
    val walkedTypePath = s"""- root class: "${clsName}"""" :: Nil
    extractorFor(inputObject, tpe, walkedTypePath) match {
      case expressions.If(_, _, s: CreateNamedStruct) if tpe <:< localTypeOf[Product] => s
      case other => CreateNamedStruct(expressions.Literal("value") :: other :: Nil)
    }
  }

  /** Helper for extracting internal fields from a case class. */
  private def extractorFor(
      inputObject: Expression,
      tpe: `Type`,
      walkedTypePath: Seq[String]): Expression = ScalaReflectionLock.synchronized {

    def toCatalystArray(input: Expression, elementType: `Type`): Expression = {
      val externalDataType = dataTypeFor(elementType)
      val Schema(catalystType, nullable) = silentSchemaFor(elementType)
      if (isNativeType(externalDataType)) {
        NewInstance(
          classOf[GenericArrayData],
          input :: Nil,
          dataType = ArrayType(catalystType, nullable))
      } else {
        val clsName = getClassNameFromType(elementType)
        val newPath = s"""- array element class: "$clsName"""" +: walkedTypePath
        MapObjects(extractorFor(_, elementType, newPath), input, externalDataType)
      }
    }

    if (!inputObject.dataType.isInstanceOf[ObjectType]) {
      inputObject
    } else {
      val className = getClassNameFromType(tpe)
      tpe match {
        case t if t <:< localTypeOf[Option[_]] =>
          val TypeRef(_, _, Seq(optType)) = t
          optType match {
            // For primitive types we must manually unbox the value of the object.
            case t if t <:< definitions.IntTpe =>
              Invoke(
                UnwrapOption(ObjectType(classOf[java.lang.Integer]), inputObject),
                "intValue",
                IntegerType)
            case t if t <:< definitions.LongTpe =>
              Invoke(
                UnwrapOption(ObjectType(classOf[java.lang.Long]), inputObject),
                "longValue",
                LongType)
            case t if t <:< definitions.DoubleTpe =>
              Invoke(
                UnwrapOption(ObjectType(classOf[java.lang.Double]), inputObject),
                "doubleValue",
                DoubleType)
            case t if t <:< definitions.FloatTpe =>
              Invoke(
                UnwrapOption(ObjectType(classOf[java.lang.Float]), inputObject),
                "floatValue",
                FloatType)
            case t if t <:< definitions.ShortTpe =>
              Invoke(
                UnwrapOption(ObjectType(classOf[java.lang.Short]), inputObject),
                "shortValue",
                ShortType)
            case t if t <:< definitions.ByteTpe =>
              Invoke(
                UnwrapOption(ObjectType(classOf[java.lang.Byte]), inputObject),
                "byteValue",
                ByteType)
            case t if t <:< definitions.BooleanTpe =>
              Invoke(
                UnwrapOption(ObjectType(classOf[java.lang.Boolean]), inputObject),
                "booleanValue",
                BooleanType)

            // For non-primitives, we can just extract the object from the Option and then recurse.
            case other =>
              val className = getClassNameFromType(optType)
              val classObj = Utils.classForName(className)
              val optionObjectType = ObjectType(classObj)
              val newPath = s"""- option value class: "$className"""" +: walkedTypePath

              val unwrapped = UnwrapOption(optionObjectType, inputObject)
              expressions.If(
                IsNull(unwrapped),
                expressions.Literal.create(null, silentSchemaFor(optType).dataType),
                extractorFor(unwrapped, optType, newPath))
          }

        // Since List[_] also belongs to localTypeOf[Product], we put this case before
        // "case t if t <:< localTypeOf[Product]" to make sure it will match to the
        // case "localTypeOf[Seq[_]]"
        case t if t <:< localTypeOf[Seq[_]] =>
          val TypeRef(_, _, Seq(elementType)) = t
          toCatalystArray(inputObject, elementType)

        case t if t <:< localTypeOf[Product] =>
          val params = getConstructorParameters(t)
          val nonNullOutput = CreateNamedStruct(params.flatMap { case (fieldName, fieldType) =>
            val fieldValue = Invoke(inputObject, fieldName, dataTypeFor(fieldType))
            val clsName = getClassNameFromType(fieldType)
            val newPath = s"""- field (class: "$clsName", name: "$fieldName")""" +: walkedTypePath
            expressions.Literal(fieldName) :: extractorFor(fieldValue, fieldType, newPath) :: Nil
          })
          val nullOutput = expressions.Literal.create(null, nonNullOutput.dataType)
          expressions.If(IsNull(inputObject), nullOutput, nonNullOutput)

        case t if t <:< localTypeOf[Array[_]] =>
          val TypeRef(_, _, Seq(elementType)) = t
          toCatalystArray(inputObject, elementType)

        case t if t <:< localTypeOf[Map[_, _]] =>
          val TypeRef(_, _, Seq(keyType, valueType)) = t

          val keys =
            Invoke(
              Invoke(inputObject, "keysIterator",
                ObjectType(classOf[scala.collection.Iterator[_]])),
              "toSeq",
              ObjectType(classOf[scala.collection.Seq[_]]))
          val convertedKeys = toCatalystArray(keys, keyType)

          val values =
            Invoke(
              Invoke(inputObject, "valuesIterator",
                ObjectType(classOf[scala.collection.Iterator[_]])),
              "toSeq",
              ObjectType(classOf[scala.collection.Seq[_]]))
          val convertedValues = toCatalystArray(values, valueType)

          val Schema(keyDataType, _) = schemaFor(keyType)
          val Schema(valueDataType, valueNullable) = schemaFor(valueType)
          NewInstance(
            classOf[ArrayBasedMapData],
            convertedKeys :: convertedValues :: Nil,
            dataType = MapType(keyDataType, valueDataType, valueNullable))

        case t if t <:< localTypeOf[String] =>
          StaticInvoke(
            classOf[UTF8String],
            StringType,
            "fromString",
            inputObject :: Nil)

        case t if t <:< localTypeOf[java.sql.Timestamp] =>
          StaticInvoke(
            DateTimeUtils.getClass,
            TimestampType,
            "fromJavaTimestamp",
            inputObject :: Nil)

        case t if t <:< localTypeOf[java.sql.Date] =>
          StaticInvoke(
            DateTimeUtils.getClass,
            DateType,
            "fromJavaDate",
            inputObject :: Nil)

        case t if t <:< localTypeOf[BigDecimal] =>
          StaticInvoke(
            Decimal.getClass,
            DecimalType.SYSTEM_DEFAULT,
            "apply",
            inputObject :: Nil)

        case t if t <:< localTypeOf[java.math.BigDecimal] =>
          StaticInvoke(
            Decimal.getClass,
            DecimalType.SYSTEM_DEFAULT,
            "apply",
            inputObject :: Nil)

        case t if t <:< localTypeOf[java.lang.Integer] =>
          Invoke(inputObject, "intValue", IntegerType)
        case t if t <:< localTypeOf[java.lang.Long] =>
          Invoke(inputObject, "longValue", LongType)
        case t if t <:< localTypeOf[java.lang.Double] =>
          Invoke(inputObject, "doubleValue", DoubleType)
        case t if t <:< localTypeOf[java.lang.Float] =>
          Invoke(inputObject, "floatValue", FloatType)
        case t if t <:< localTypeOf[java.lang.Short] =>
          Invoke(inputObject, "shortValue", ShortType)
        case t if t <:< localTypeOf[java.lang.Byte] =>
          Invoke(inputObject, "byteValue", ByteType)
        case t if t <:< localTypeOf[java.lang.Boolean] =>
          Invoke(inputObject, "booleanValue", BooleanType)

        case t if Utils.classIsLoadable(className) &&
          Utils.classForName(className).isAnnotationPresent(classOf[SQLUserDefinedType]) =>
          val udt = Utils.classForName(className)
            .getAnnotation(classOf[SQLUserDefinedType]).udt().newInstance()
          val obj = NewInstance(
            udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt(),
            Nil,
            dataType = ObjectType(udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()))
          Invoke(obj, "serialize", udt.sqlType, inputObject :: Nil)

        case other =>
          throw new UnsupportedOperationException(
            s"No Encoder found for $tpe\n" + walkedTypePath.mkString("\n"))
      }
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

  def getClassFromType(tpe: Type): Class[_] = mirror.runtimeClass(tpe.erasure.typeSymbol.asClass)
}

/**
 * Support for generating catalyst schemas for scala objects.  Note that unlike its companion
 * object, this trait able to work in both the runtime and the compile time (macro) universe.
 */
trait ScalaReflection {
  /** The universe we work in (runtime or macro) */
  val universe: scala.reflect.api.Universe

  /** The mirror used to access types in the universe */
  def mirror: universe.Mirror

  import universe._

  // The Predef.Map is scala.collection.immutable.Map.
  // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
  import scala.collection.Map

  case class Schema(dataType: DataType, nullable: Boolean)

  /** Returns a Sequence of attributes for the given case class type. */
  def attributesFor[T: TypeTag]: Seq[Attribute] = schemaFor[T] match {
    case Schema(s: StructType, _) =>
      s.toAttributes
  }

  /** Returns a catalyst DataType and its nullability for the given Scala Type using reflection. */
  def schemaFor[T: TypeTag]: Schema = schemaFor(localTypeOf[T])

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
  def localTypeOf[T: TypeTag]: `Type` = typeTag[T].in(mirror).tpe

  /** Returns a catalyst DataType and its nullability for the given Scala Type using reflection. */
  def schemaFor(tpe: `Type`): Schema = ScalaReflectionLock.synchronized {
    val className = getClassNameFromType(tpe)
    tpe match {
      case t if Utils.classIsLoadable(className) &&
        Utils.classForName(className).isAnnotationPresent(classOf[SQLUserDefinedType]) =>
        // Note: We check for classIsLoadable above since Utils.classForName uses Java reflection,
        //       whereas className is from Scala reflection.  This can make it hard to find classes
        //       in some cases, such as when a class is enclosed in an object (in which case
        //       Java appends a '$' to the object name but Scala does not).
        val udt = Utils.classForName(className)
          .getAnnotation(classOf[SQLUserDefinedType]).udt().newInstance()
        Schema(udt, nullable = true)
      case t if t <:< localTypeOf[Option[_]] =>
        val TypeRef(_, _, Seq(optType)) = t
        Schema(schemaFor(optType).dataType, nullable = true)
      case t if t <:< localTypeOf[Array[Byte]] => Schema(BinaryType, nullable = true)
      case t if t <:< localTypeOf[Array[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val Schema(dataType, nullable) = schemaFor(elementType)
        Schema(ArrayType(dataType, containsNull = nullable), nullable = true)
      case t if t <:< localTypeOf[Seq[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val Schema(dataType, nullable) = schemaFor(elementType)
        Schema(ArrayType(dataType, containsNull = nullable), nullable = true)
      case t if t <:< localTypeOf[Map[_, _]] =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t
        val Schema(valueDataType, valueNullable) = schemaFor(valueType)
        Schema(MapType(schemaFor(keyType).dataType,
          valueDataType, valueContainsNull = valueNullable), nullable = true)
      case t if t <:< localTypeOf[Product] =>
        val params = getConstructorParameters(t)
        Schema(StructType(
          params.map { case (fieldName, fieldType) =>
            val Schema(dataType, nullable) = schemaFor(fieldType)
            StructField(fieldName, dataType, nullable)
          }), nullable = true)
      case t if t <:< localTypeOf[String] => Schema(StringType, nullable = true)
      case t if t <:< localTypeOf[java.sql.Timestamp] => Schema(TimestampType, nullable = true)
      case t if t <:< localTypeOf[java.sql.Date] => Schema(DateType, nullable = true)
      case t if t <:< localTypeOf[BigDecimal] => Schema(DecimalType.SYSTEM_DEFAULT, nullable = true)
      case t if t <:< localTypeOf[java.math.BigDecimal] =>
        Schema(DecimalType.SYSTEM_DEFAULT, nullable = true)
      case t if t <:< localTypeOf[Decimal] => Schema(DecimalType.SYSTEM_DEFAULT, nullable = true)
      case t if t <:< localTypeOf[java.lang.Integer] => Schema(IntegerType, nullable = true)
      case t if t <:< localTypeOf[java.lang.Long] => Schema(LongType, nullable = true)
      case t if t <:< localTypeOf[java.lang.Double] => Schema(DoubleType, nullable = true)
      case t if t <:< localTypeOf[java.lang.Float] => Schema(FloatType, nullable = true)
      case t if t <:< localTypeOf[java.lang.Short] => Schema(ShortType, nullable = true)
      case t if t <:< localTypeOf[java.lang.Byte] => Schema(ByteType, nullable = true)
      case t if t <:< localTypeOf[java.lang.Boolean] => Schema(BooleanType, nullable = true)
      case t if t <:< definitions.IntTpe => Schema(IntegerType, nullable = false)
      case t if t <:< definitions.LongTpe => Schema(LongType, nullable = false)
      case t if t <:< definitions.DoubleTpe => Schema(DoubleType, nullable = false)
      case t if t <:< definitions.FloatTpe => Schema(FloatType, nullable = false)
      case t if t <:< definitions.ShortTpe => Schema(ShortType, nullable = false)
      case t if t <:< definitions.ByteTpe => Schema(ByteType, nullable = false)
      case t if t <:< definitions.BooleanTpe => Schema(BooleanType, nullable = false)
      case other =>
        throw new UnsupportedOperationException(s"Schema for type $other is not supported")
    }
  }

  /**
   * Returns a catalyst DataType and its nullability for the given Scala Type using reflection.
   *
   * Unlike `schemaFor`, this method won't throw exception for un-supported type, it will return
   * `NullType` silently instead.
   */
  def silentSchemaFor(tpe: `Type`): Schema = try {
    schemaFor(tpe)
  } catch {
    case _: UnsupportedOperationException => Schema(NullType, nullable = true)
  }

  /** Returns the full class name for a type. */
  def getClassNameFromType(tpe: `Type`): String = {
    tpe.erasure.typeSymbol.asClass.fullName
  }

  /**
   * Returns classes of input parameters of scala function object.
   */
  def getParameterTypes(func: AnyRef): Seq[Class[_]] = {
    val methods = func.getClass.getMethods.filter(m => m.getName == "apply" && !m.isBridge)
    assert(methods.length == 1)
    methods.head.getParameterTypes
  }

  /**
   * Returns the parameter names and types for the primary constructor of this type.
   *
   * Note that it only works for scala classes with primary constructor, and currently doesn't
   * support inner class.
   */
  def getConstructorParameters(tpe: Type): Seq[(String, Type)] = {
    val formalTypeArgs = tpe.typeSymbol.asClass.typeParams
    val TypeRef(_, _, actualTypeArgs) = tpe
    val constructorSymbol = tpe.member(nme.CONSTRUCTOR)
    val params = if (constructorSymbol.isMethod) {
      constructorSymbol.asMethod.paramss
    } else {
      // Find the primary constructor, and use its parameter ordering.
      val primaryConstructorSymbol: Option[Symbol] = constructorSymbol.asTerm.alternatives.find(
        s => s.isMethod && s.asMethod.isPrimaryConstructor)
      if (primaryConstructorSymbol.isEmpty) {
        sys.error("Internal SQL error: Product object did not have a primary constructor.")
      } else {
        primaryConstructorSymbol.get.asMethod.paramss
      }
    }

    params.flatten.map { p =>
      p.name.toString -> p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
    }
  }
}

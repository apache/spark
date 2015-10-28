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

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

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
}

/**
 * Support for generating catalyst schemas for scala objects.
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
  def schemaFor[T: TypeTag]: Schema =
    ScalaReflectionLock.synchronized { schemaFor(localTypeOf[T]) }

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
  private def localTypeOf[T: TypeTag]: `Type` = typeTag[T].in(mirror).tpe

  /**
   * Returns the Spark SQL DataType for a given scala type.  Where this is not an exact mapping
   * to a native type, an ObjectType is returned. Special handling is also used for Arrays including
   * those that hold primitive types.
   */
  def dataTypeFor(tpe: `Type`): DataType = tpe match {
    case t if t <:< definitions.IntTpe => IntegerType
    case t if t <:< definitions.LongTpe => LongType
    case t if t <:< definitions.DoubleTpe => DoubleType
    case t if t <:< definitions.FloatTpe => FloatType
    case t if t <:< definitions.ShortTpe => ShortType
    case t if t <:< definitions.ByteTpe => ByteType
    case t if t <:< definitions.BooleanTpe => BooleanType
    case t if t <:< localTypeOf[Array[Byte]] => BinaryType
    case _ =>
      val className: String = tpe.erasure.typeSymbol.asClass.fullName
      className match {
        case "scala.Array" =>
          val TypeRef(_, _, Seq(arrayType)) = tpe
          val cls = arrayType match {
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
        case other => ObjectType(Utils.classForName(className))
      }
  }

  /** Returns expressions for extracting all the fields from the given type. */
  def extractorsFor[T : TypeTag](inputObject: Expression): Seq[Expression] = {
    ScalaReflectionLock.synchronized {
      extractorFor(inputObject, typeTag[T].tpe).asInstanceOf[CreateStruct].children
    }
  }

  /** Helper for extracting internal fields from a case class. */
  protected def extractorFor(
      inputObject: Expression,
      tpe: `Type`): Expression = ScalaReflectionLock.synchronized {
    if (!inputObject.dataType.isInstanceOf[ObjectType]) {
      inputObject
    } else {
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
              val className: String = optType.erasure.typeSymbol.asClass.fullName
              val classObj = Utils.classForName(className)
              val optionObjectType = ObjectType(classObj)

              val unwrapped = UnwrapOption(optionObjectType, inputObject)
              expressions.If(
                IsNull(unwrapped),
                expressions.Literal.create(null, schemaFor(optType).dataType),
                extractorFor(unwrapped, optType))
          }

        case t if t <:< localTypeOf[Product] =>
          val formalTypeArgs = t.typeSymbol.asClass.typeParams
          val TypeRef(_, _, actualTypeArgs) = t
          val constructorSymbol = t.member(nme.CONSTRUCTOR)
          val params = if (constructorSymbol.isMethod) {
            constructorSymbol.asMethod.paramss
          } else {
            // Find the primary constructor, and use its parameter ordering.
            val primaryConstructorSymbol: Option[Symbol] =
              constructorSymbol.asTerm.alternatives.find(s =>
                s.isMethod && s.asMethod.isPrimaryConstructor)

            if (primaryConstructorSymbol.isEmpty) {
              sys.error("Internal SQL error: Product object did not have a primary constructor.")
            } else {
              primaryConstructorSymbol.get.asMethod.paramss
            }
          }

          CreateStruct(params.head.map { p =>
            val fieldName = p.name.toString
            val fieldType = p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
            val fieldValue = Invoke(inputObject, fieldName, dataTypeFor(fieldType))
            extractorFor(fieldValue, fieldType)
          })

        case t if t <:< localTypeOf[Array[_]] =>
          val TypeRef(_, _, Seq(elementType)) = t
          val elementDataType = dataTypeFor(elementType)
          val Schema(dataType, nullable) = schemaFor(elementType)

          if (!elementDataType.isInstanceOf[AtomicType]) {
            MapObjects(extractorFor(_, elementType), inputObject, elementDataType)
          } else {
            NewInstance(
              classOf[GenericArrayData],
              inputObject :: Nil,
              dataType = ArrayType(dataType, nullable))
          }

        case t if t <:< localTypeOf[Seq[_]] =>
          val TypeRef(_, _, Seq(elementType)) = t
          val elementDataType = dataTypeFor(elementType)
          val Schema(dataType, nullable) = schemaFor(elementType)

          if (!elementDataType.isInstanceOf[AtomicType]) {
            MapObjects(extractorFor(_, elementType), inputObject, elementDataType)
          } else {
            NewInstance(
              classOf[GenericArrayData],
              inputObject :: Nil,
              dataType = ArrayType(dataType, nullable))
          }

        case t if t <:< localTypeOf[Map[_, _]] =>
          val TypeRef(_, _, Seq(keyType, valueType)) = t
          val Schema(keyDataType, _) = schemaFor(keyType)
          val Schema(valueDataType, valueNullable) = schemaFor(valueType)

          val rawMap = inputObject
          val keys =
            NewInstance(
              classOf[GenericArrayData],
              Invoke(rawMap, "keys", ObjectType(classOf[scala.collection.GenIterable[_]])) :: Nil,
              dataType = ObjectType(classOf[ArrayData]))
          val values =
            NewInstance(
              classOf[GenericArrayData],
              Invoke(rawMap, "values", ObjectType(classOf[scala.collection.GenIterable[_]])) :: Nil,
              dataType = ObjectType(classOf[ArrayData]))
          NewInstance(
            classOf[ArrayBasedMapData],
            keys :: values :: Nil,
            dataType = MapType(keyDataType, valueDataType, valueNullable))

        case t if t <:< localTypeOf[String] =>
          StaticInvoke(
            classOf[UTF8String],
            StringType,
            "fromString",
            inputObject :: Nil)

        case t if t <:< localTypeOf[java.sql.Timestamp] =>
          StaticInvoke(
            DateTimeUtils,
            TimestampType,
            "fromJavaTimestamp",
            inputObject :: Nil)

        case t if t <:< localTypeOf[java.sql.Date] =>
          StaticInvoke(
            DateTimeUtils,
            DateType,
            "fromJavaDate",
            inputObject :: Nil)
        case t if t <:< localTypeOf[BigDecimal] =>
          StaticInvoke(
            Decimal,
            DecimalType.SYSTEM_DEFAULT,
            "apply",
            inputObject :: Nil)

        case t if t <:< localTypeOf[java.math.BigDecimal] =>
          StaticInvoke(
            Decimal,
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

        case other =>
          throw new UnsupportedOperationException(s"Extractor for type $other is not supported")
      }
    }
  }

  /** Returns a catalyst DataType and its nullability for the given Scala Type using reflection. */
  def schemaFor(tpe: `Type`): Schema = ScalaReflectionLock.synchronized {
    val className: String = tpe.erasure.typeSymbol.asClass.fullName
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
        val formalTypeArgs = t.typeSymbol.asClass.typeParams
        val TypeRef(_, _, actualTypeArgs) = t
        val constructorSymbol = t.member(nme.CONSTRUCTOR)
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
        Schema(StructType(
          params.head.map { p =>
            val Schema(dataType, nullable) =
              schemaFor(p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs))
            StructField(p.name.toString, dataType, nullable)
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

  def typeOfObject: PartialFunction[Any, DataType] = {
    // The data type can be determined without ambiguity.
    case obj: Boolean => BooleanType
    case obj: Array[Byte] => BinaryType
    case obj: String => StringType
    case obj: UTF8String => StringType
    case obj: Byte => ByteType
    case obj: Short => ShortType
    case obj: Int => IntegerType
    case obj: Long => LongType
    case obj: Float => FloatType
    case obj: Double => DoubleType
    case obj: java.sql.Date => DateType
    case obj: java.math.BigDecimal => DecimalType.SYSTEM_DEFAULT
    case obj: Decimal => DecimalType.SYSTEM_DEFAULT
    case obj: java.sql.Timestamp => TimestampType
    case null => NullType
    // For other cases, there is no obvious mapping from the type of the given object to a
    // Catalyst data type. A user should provide his/her specific rules
    // (in a user-defined PartialFunction) to infer the Catalyst data type for other types of
    // objects and then compose the user-defined PartialFunction with this one.
  }

  implicit class CaseClassRelation[A <: Product : TypeTag](data: Seq[A]) {

    /**
     * Implicitly added to Sequences of case class objects.  Returns a catalyst logical relation
     * for the the data in the sequence.
     */
    def asRelation: LocalRelation = {
      val output = attributesFor[A]
      LocalRelation.fromProduct(output, data)
    }
  }
}

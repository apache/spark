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

package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.util.Utils
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, ArrayBasedMapData, GenericArrayData}

import scala.reflect.ClassTag

object ProductEncoder {
  import ScalaReflection.universe._
  import ScalaReflection.localTypeOf
  import ScalaReflection.dataTypeFor
  import ScalaReflection.Schema
  import ScalaReflection.schemaFor
  import ScalaReflection.arrayClassFor

  def apply[T <: Product : TypeTag]: ExpressionEncoder[T] = {
    // We convert the not-serializable TypeTag into StructType and ClassTag.
    val tpe = typeTag[T].tpe
    val mirror = typeTag[T].mirror
    val cls = mirror.runtimeClass(tpe)

    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val toRowExpression = extractorFor(inputObject, tpe).asInstanceOf[CreateNamedStruct]
    val fromRowExpression = constructorFor(tpe)

    new ExpressionEncoder[T](
      toRowExpression.dataType,
      flat = false,
      toRowExpression.flatten,
      fromRowExpression,
      ClassTag[T](cls))
  }

  // The Predef.Map is scala.collection.immutable.Map.
  // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
  import scala.collection.Map

  def extractorFor(
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

          CreateNamedStruct(params.head.flatMap { p =>
            val fieldName = p.name.toString
            val fieldType = p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
            val fieldValue = Invoke(inputObject, fieldName, dataTypeFor(fieldType))
            expressions.Literal(fieldName) :: extractorFor(fieldValue, fieldType) :: Nil
          })

        case t if t <:< localTypeOf[Array[_]] =>
          val TypeRef(_, _, Seq(elementType)) = t
          toCatalystArray(inputObject, elementType)

        case t if t <:< localTypeOf[Seq[_]] =>
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
          throw new UnsupportedOperationException(s"Encoder for type $other is not supported")
      }
    }
  }

  private def toCatalystArray(input: Expression, elementType: `Type`): Expression = {
    val externalDataType = dataTypeFor(elementType)
    val Schema(catalystType, nullable) = schemaFor(elementType)
    if (RowEncoder.isNativeType(catalystType)) {
      NewInstance(
        classOf[GenericArrayData],
        input :: Nil,
        dataType = ArrayType(catalystType, nullable))
    } else {
      MapObjects(extractorFor(_, elementType), input, externalDataType)
    }
  }

  def constructorFor(
      tpe: `Type`,
      path: Option[Expression] = None): Expression = ScalaReflectionLock.synchronized {

    /** Returns the current path with a sub-field extracted. */
    def addToPath(part: String): Expression = path
      .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
      .getOrElse(UnresolvedAttribute(part))

    /** Returns the current path with a field at ordinal extracted. */
    def addToPathOrdinal(ordinal: Int, dataType: DataType): Expression = path
      .map(p => GetInternalRowField(p, ordinal, dataType))
      .getOrElse(BoundReference(ordinal, dataType, false))

    /** Returns the current path or `BoundReference`. */
    def getPath: Expression = path.getOrElse(BoundReference(0, schemaFor(tpe).dataType, true))

    tpe match {
      case t if !dataTypeFor(t).isInstanceOf[ObjectType] => getPath

      case t if t <:< localTypeOf[Option[_]] =>
        val TypeRef(_, _, Seq(optType)) = t
        WrapOption(null, constructorFor(optType, path))

      case t if t <:< localTypeOf[java.lang.Integer] =>
        val boxedType = classOf[java.lang.Integer]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, propagateNull = true, objectType)

      case t if t <:< localTypeOf[java.lang.Long] =>
        val boxedType = classOf[java.lang.Long]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, propagateNull = true, objectType)

      case t if t <:< localTypeOf[java.lang.Double] =>
        val boxedType = classOf[java.lang.Double]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, propagateNull = true, objectType)

      case t if t <:< localTypeOf[java.lang.Float] =>
        val boxedType = classOf[java.lang.Float]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, propagateNull = true, objectType)

      case t if t <:< localTypeOf[java.lang.Short] =>
        val boxedType = classOf[java.lang.Short]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, propagateNull = true, objectType)

      case t if t <:< localTypeOf[java.lang.Byte] =>
        val boxedType = classOf[java.lang.Byte]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, propagateNull = true, objectType)

      case t if t <:< localTypeOf[java.lang.Boolean] =>
        val boxedType = classOf[java.lang.Boolean]
        val objectType = ObjectType(boxedType)
        NewInstance(boxedType, getPath :: Nil, propagateNull = true, objectType)

      case t if t <:< localTypeOf[java.sql.Date] =>
        StaticInvoke(
          DateTimeUtils,
          ObjectType(classOf[java.sql.Date]),
          "toJavaDate",
          getPath :: Nil,
          propagateNull = true)

      case t if t <:< localTypeOf[java.sql.Timestamp] =>
        StaticInvoke(
          DateTimeUtils,
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
          Invoke(
            MapObjects(
              p => constructorFor(elementType, Some(p)),
              getPath,
              schemaFor(elementType).dataType),
            "array",
            arrayClassFor(elementType))
        }

      case t if t <:< localTypeOf[Seq[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val arrayData =
          Invoke(
            MapObjects(
              p => constructorFor(elementType, Some(p)),
              getPath,
              schemaFor(elementType).dataType),
            "array",
            ObjectType(classOf[Array[Any]]))

        StaticInvoke(
          scala.collection.mutable.WrappedArray,
          ObjectType(classOf[Seq[_]]),
          "make",
          arrayData :: Nil)

      case t if t <:< localTypeOf[Map[_, _]] =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t

        val keyData =
          Invoke(
            MapObjects(
              p => constructorFor(keyType, Some(p)),
              Invoke(getPath, "keyArray", ArrayType(schemaFor(keyType).dataType)),
              schemaFor(keyType).dataType),
            "array",
            ObjectType(classOf[Array[Any]]))

        val valueData =
          Invoke(
            MapObjects(
              p => constructorFor(valueType, Some(p)),
              Invoke(getPath, "valueArray", ArrayType(schemaFor(valueType).dataType)),
              schemaFor(valueType).dataType),
            "array",
            ObjectType(classOf[Array[Any]]))

        StaticInvoke(
          ArrayBasedMapData,
          ObjectType(classOf[Map[_, _]]),
          "toScalaMap",
          keyData :: valueData :: Nil)

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

        val className: String = t.erasure.typeSymbol.asClass.fullName
        val cls = Utils.classForName(className)

        val arguments = params.head.zipWithIndex.map { case (p, i) =>
          val fieldName = p.name.toString
          val fieldType = p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
          val dataType = schemaFor(fieldType).dataType

          // For tuples, we based grab the inner fields by ordinal instead of name.
          if (className startsWith "scala.Tuple") {
            constructorFor(fieldType, Some(addToPathOrdinal(i, dataType)))
          } else {
            constructorFor(fieldType, Some(addToPath(fieldName)))
          }
        }

        val newInstance = NewInstance(cls, arguments, propagateNull = false, ObjectType(cls))

        if (path.nonEmpty) {
          expressions.If(
            IsNull(getPath),
            expressions.Literal.create(null, ObjectType(cls)),
            newInstance
          )
        } else {
          newInstance
        }
    }
  }
}

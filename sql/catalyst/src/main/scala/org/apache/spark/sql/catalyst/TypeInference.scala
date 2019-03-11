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

import java.util.{Map => JMap}

import scala.collection.Map
import scala.language.existentials

import com.google.common.reflect.TypeToken

import org.apache.spark.sql.catalyst.DeserializerBuildHelper._
import org.apache.spark.sql.catalyst.JavaTypeInference._
import org.apache.spark.sql.catalyst.ScalaReflection._
import org.apache.spark.sql.catalyst.ScalaReflection.universe._
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNull, MapKeys, MapValues}
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.{ObjectType, SQLUserDefinedType, UDTRegistration, UserDefinedType}

object TypeInference {

  /**
   * Returns an expression that can be used to deserialize an input expression to an object of type
   * represented by an `TypeInference` with a compatible schema.
   *
   * @param t The `TypeInference` object providing type information.
   * @param path The expression which can be used to extract serialized value.
   * @param walkedTypePath The paths from top to bottom to access current field when deserializing.
   */
  def deserializerFor(
      t: TypeInference,
      path: Expression,
      walkedTypePath: WalkedTypePath): Expression = cleanUpReflectionObjects {
    t match {
      case _ if t.isNotObjectType => path
      case _ if t.isJavaBoolean =>
        createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Boolean])
      case _ if t.isJavaByte =>
        createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Byte])
      case _ if t.isJavaInteger =>
        createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Integer])
      case _ if t.isJavaLong =>
        createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Long])
      case _ if t.isJavaDouble =>
        createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Double])
      case _ if t.isJavaFloat =>
        createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Float])
      case _ if t.isJavaShort =>
        createDeserializerForTypesSupportValueOf(path, classOf[java.lang.Short])
      case _ if t.isJavaLocalDate =>
        createDeserializerForLocalDate(path)
      case _ if t.isJavaDate =>
        createDeserializerForSqlDate(path)
      case _ if t.isJavaInstant =>
        createDeserializerForInstant(path)
      case _ if t.isJavaTimestamp =>
        createDeserializerForSqlTimestamp(path)
      case _ if t.isJavaString =>
        createDeserializerForString(path, returnNullable = false)
      case _ if t.isJavaBigDecimal =>
        createDeserializerForJavaBigDecimal(path, returnNullable = false)
      case _ if t.isScalaBigDecimal =>
        createDeserializerForScalaBigDecimal(path, returnNullable = false)
      case _ if t.isJavaBigInteger =>
        createDeserializerForJavaBigInteger(path, returnNullable = false)
      case _ if t.isScalaBigInt =>
        createDeserializerForScalaBigInt(path)

      case _ => t.deserializerFor(path, walkedTypePath)
    }
  }
}

trait TypeInference {

  def isNotObjectType: Boolean = false

  def isJavaShort: Boolean = false
  def isJavaInteger: Boolean = false
  def isJavaLong: Boolean = false
  def isJavaDouble: Boolean = false
  def isJavaFloat: Boolean = false
  def isJavaByte: Boolean = false
  def isJavaBoolean: Boolean = false

  def isJavaLocalDate: Boolean = false
  def isJavaDate: Boolean = false
  def isJavaInstant: Boolean = false
  def isJavaTimestamp: Boolean = false
  def isJavaString: Boolean = false
  def isJavaBigDecimal: Boolean = false
  def isJavaBigInteger: Boolean = false

  def isScalaBigDecimal: Boolean = false
  def isScalaBigInt: Boolean = false

  /**
   * Defines deserializers for specified `TypeInference`.
   */
  def deserializerFor(path: Expression, walkedTypePath: WalkedTypePath): Expression
}

case class ScalaInference(scalaType: `Type`) extends TypeInference {

  val t = scalaType.dealias

  override def isNotObjectType: Boolean = !dataTypeFor(t).isInstanceOf[ObjectType]

  override def isJavaShort: Boolean = t <:< localTypeOf[java.lang.Short]
  override def isJavaInteger: Boolean = t <:< localTypeOf[java.lang.Integer]
  override def isJavaLong: Boolean = t <:< localTypeOf[java.lang.Long]
  override def isJavaDouble: Boolean = t <:< localTypeOf[java.lang.Double]
  override def isJavaFloat: Boolean = t <:< localTypeOf[java.lang.Float]
  override def isJavaByte: Boolean = t <:< localTypeOf[java.lang.Byte]
  override def isJavaBoolean: Boolean = t <:< localTypeOf[java.lang.Boolean]

  override def isJavaLocalDate: Boolean = t <:< localTypeOf[java.time.LocalDate]
  override def isJavaDate: Boolean = t <:< localTypeOf[java.sql.Date]
  override def isJavaInstant: Boolean = t <:< localTypeOf[java.time.Instant]
  override def isJavaTimestamp: Boolean = t <:< localTypeOf[java.sql.Timestamp]
  override def isJavaString: Boolean = t <:< localTypeOf[java.lang.String]
  override def isJavaBigDecimal: Boolean = t <:< localTypeOf[java.math.BigDecimal]
  override def isJavaBigInteger: Boolean = t <:< localTypeOf[java.math.BigInteger]

  override def isScalaBigDecimal: Boolean = t <:< localTypeOf[BigDecimal]
  override def isScalaBigInt: Boolean = t <:< localTypeOf[scala.math.BigInt]

  def isSQLUserDefinedType: Boolean =
    t.typeSymbol.annotations.exists(_.tree.tpe =:= typeOf[SQLUserDefinedType])
  def isRegisteredUDT: Boolean = UDTRegistration.exists(getClassNameFromType(t))

  def isScalaArray: Boolean = t <:< localTypeOf[Array[_]]
  def isScalaOption: Boolean = t <:< localTypeOf[Option[_]]
  def isScalaSeq: Boolean = t <:< localTypeOf[Seq[_]]
  def isScalaSet: Boolean = t <:< localTypeOf[scala.collection.Set[_]]
  def isScalaMap: Boolean = t <:< localTypeOf[Map[_, _]]
  def isDefinedByConstructorParams: Boolean = ScalaReflection.definedByConstructorParams(t)

  override def deserializerFor(
      path: Expression,
      walkedTypePath: WalkedTypePath): Expression = cleanUpReflectionObjects {
    this match {
      case _ if isScalaOption =>
        val TypeRef(_, _, Seq(optType)) = t
        val className = getClassNameFromType(optType)
        val newTypePath = walkedTypePath.recordOption(className)
        WrapOption(TypeInference.deserializerFor(ScalaInference(optType), path, newTypePath),
          dataTypeFor(optType))

      case _ if isScalaArray =>
        val TypeRef(_, _, Seq(elementType)) = t
        val Schema(dataType, elementNullable) = schemaFor(elementType)
        val className = getClassNameFromType(elementType)
        val newTypePath = walkedTypePath.recordArray(className)

        val typeInference = ScalaInference(elementType)

        val mapFunction: Expression => Expression = element => {
          // upcast the array element to the data type the encoder expected.
          deserializerForWithNullSafetyAndUpcast(
            element,
            dataType,
            nullable = elementNullable,
            newTypePath,
            (casted, typePath) => TypeInference.deserializerFor(typeInference, casted, typePath))
        }

        val arrayData = UnresolvedMapObjects(mapFunction, path)
        val arrayCls = arrayClassFor(elementType)

        val methodName = elementType match {
          case t if t <:< definitions.IntTpe => "toIntArray"
          case t if t <:< definitions.LongTpe => "toLongArray"
          case t if t <:< definitions.DoubleTpe => "toDoubleArray"
          case t if t <:< definitions.FloatTpe => "toFloatArray"
          case t if t <:< definitions.ShortTpe => "toShortArray"
          case t if t <:< definitions.ByteTpe => "toByteArray"
          case t if t <:< definitions.BooleanTpe => "toBooleanArray"
          // non-primitive
          case _ => "array"
        }
        Invoke(arrayData, methodName, arrayCls, returnNullable = false)

      // We serialize a `Set` to Catalyst array. When we deserialize a Catalyst array
      // to a `Set`, if there are duplicated elements, the elements will be de-duplicated.
      case _ if isScalaSeq | isScalaSet =>
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
            (casted, typePath) =>
              TypeInference.deserializerFor(ScalaInference(elementType), casted, typePath))
        }

        val companion = scalaType.dealias.typeSymbol.companion.typeSignature
        val cls = companion.member(TermName("newBuilder")) match {
          case NoSymbol if isScalaSeq => classOf[Seq[_]]
          case NoSymbol if isScalaSet => classOf[scala.collection.Set[_]]
          case _ => mirror.runtimeClass(scalaType.typeSymbol.asClass)
        }
        UnresolvedMapObjects(mapFunction, path, Some(cls))

      case _ if isScalaMap =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t

        val classNameForKey = getClassNameFromType(keyType)
        val classNameForValue = getClassNameFromType(valueType)

        val newTypePath = walkedTypePath.recordMap(classNameForKey, classNameForValue)

        UnresolvedCatalystToExternalMap(
          path,
          p => TypeInference.deserializerFor(ScalaInference(keyType), p, newTypePath),
          p => TypeInference.deserializerFor(ScalaInference(valueType), p, newTypePath),
          mirror.runtimeClass(scalaType.typeSymbol.asClass)
        )

      case _ if isSQLUserDefinedType =>
        val udt = getClassFromType(t).getAnnotation(classOf[SQLUserDefinedType]).udt().
          getConstructor().newInstance()
        val obj = NewInstance(
          udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt(),
          Nil,
          dataType = ObjectType(udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()))
        Invoke(obj, "deserialize", ObjectType(udt.userClass), path :: Nil)

      case _ if isRegisteredUDT =>
        val udt = UDTRegistration.getUDTFor(getClassNameFromType(t)).get.getConstructor().
          newInstance().asInstanceOf[UserDefinedType[_]]
        val obj = NewInstance(
          udt.getClass,
          Nil,
          dataType = ObjectType(udt.getClass))
        Invoke(obj, "deserialize", ObjectType(udt.userClass), path :: Nil)

      case _ if isDefinedByConstructorParams =>
        val params = getConstructorParameters(t)

        val cls = getClassFromType(t)

        val arguments = params.zipWithIndex.map { case ((fieldName, fieldType), i) =>
          val Schema(dataType, nullable) = schemaFor(fieldType)
          val clsName = getClassNameFromType(fieldType)
          val newTypePath = walkedTypePath.recordField(clsName, fieldName)

          // For tuples, we based grab the inner fields by ordinal instead of name.
          val newPath = if (cls.getName startsWith "scala.Tuple") {
            TypeInference.deserializerFor(
              ScalaInference(fieldType),
              addToPathOrdinal(path, i, dataType, newTypePath),
              newTypePath)
          } else {
            TypeInference.deserializerFor(
              ScalaInference(fieldType),
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
    }
  }
}

case class JavaInference(typeToken: TypeToken[_]) extends TypeInference {

  val c: Class[_] = typeToken.getRawType

  override def isNotObjectType: Boolean = !inferExternalType(c).isInstanceOf[ObjectType]

  override def isJavaShort: Boolean = c == classOf[java.lang.Short]
  override def isJavaInteger: Boolean = c == classOf[java.lang.Integer]
  override def isJavaLong: Boolean = c == classOf[java.lang.Long]
  override def isJavaDouble: Boolean = c == classOf[java.lang.Double]
  override def isJavaFloat: Boolean = c == classOf[java.lang.Float]
  override def isJavaByte: Boolean = c == classOf[java.lang.Byte]
  override def isJavaBoolean: Boolean = c == classOf[java.lang.Boolean]

  override def isJavaLocalDate: Boolean = c == classOf[java.time.LocalDate]
  override def isJavaDate: Boolean = c == classOf[java.sql.Date]
  override def isJavaInstant: Boolean = c == classOf[java.time.Instant]
  override def isJavaTimestamp: Boolean = c == classOf[java.sql.Timestamp]
  override def isJavaString: Boolean = c == classOf[java.lang.String]
  override def isJavaBigDecimal: Boolean = c == classOf[java.math.BigDecimal]
  override def isJavaBigInteger: Boolean = c == classOf[java.math.BigInteger]

  def isJavaArray: Boolean = c.isArray
  def isJavaList: Boolean = listType.isAssignableFrom(typeToken)
  def isJavaMap: Boolean = mapType.isAssignableFrom(typeToken)
  def isJavaEnum: Boolean = c.isEnum

  override def deserializerFor(
      path: Expression,
      walkedTypePath: WalkedTypePath): Expression = this match {
    case _ if isJavaArray =>
      val elementType = c.getComponentType
      val newTypePath = walkedTypePath.recordArray(elementType.getCanonicalName)
      val (dataType, elementNullable) = inferDataType(elementType)
      val mapFunction: Expression => Expression = element => {
        // upcast the array element to the data type the encoder expected.
        deserializerForWithNullSafetyAndUpcast(
          element,
          dataType,
          nullable = elementNullable,
          newTypePath,
          (casted, typePath) => TypeInference.deserializerFor(
            JavaInference(typeToken.getComponentType), casted, typePath))
      }

      val arrayData = UnresolvedMapObjects(mapFunction, path)

      val methodName = elementType match {
        case c if c == java.lang.Integer.TYPE => "toIntArray"
        case c if c == java.lang.Long.TYPE => "toLongArray"
        case c if c == java.lang.Double.TYPE => "toDoubleArray"
        case c if c == java.lang.Float.TYPE => "toFloatArray"
        case c if c == java.lang.Short.TYPE => "toShortArray"
        case c if c == java.lang.Byte.TYPE => "toByteArray"
        case c if c == java.lang.Boolean.TYPE => "toBooleanArray"
        // non-primitive
        case _ => "array"
      }
      Invoke(arrayData, methodName, ObjectType(c))

    case _ if isJavaList =>
      val et = elementType(typeToken)
      val newTypePath = walkedTypePath.recordArray(et.getType.getTypeName)
      val (dataType, elementNullable) = inferDataType(et)
      val mapFunction: Expression => Expression = element => {
        // upcast the array element to the data type the encoder expected.
        deserializerForWithNullSafetyAndUpcast(
          element,
          dataType,
          nullable = elementNullable,
          newTypePath,
          (casted, typePath) =>
            TypeInference.deserializerFor(JavaInference(et), casted, typePath))
      }
      UnresolvedMapObjects(mapFunction, path, customCollectionCls = Some(c))

    case _ if isJavaMap =>
      val (keyType, valueType) = mapKeyValueType(typeToken)
      val newTypePath = walkedTypePath.recordMap(keyType.getType.getTypeName,
        valueType.getType.getTypeName)

      val keyData =
        Invoke(
          UnresolvedMapObjects(
            p => TypeInference.deserializerFor(JavaInference(keyType), p, newTypePath),
            MapKeys(path)),
          "array",
          ObjectType(classOf[Array[Any]]))

      val valueData =
        Invoke(
          UnresolvedMapObjects(
            p => TypeInference.deserializerFor(JavaInference(valueType), p, newTypePath),
            MapValues(path)),
          "array",
          ObjectType(classOf[Array[Any]]))

      StaticInvoke(
        ArrayBasedMapData.getClass,
        ObjectType(classOf[JMap[_, _]]),
        "toJavaMap",
        keyData :: valueData :: Nil,
        returnNullable = false)

    case _ if isJavaEnum =>
      createDeserializerForTypesSupportValueOf(
        createDeserializerForString(path, returnNullable = false), c)

    case _ =>
      val properties = getJavaBeanReadableAndWritableProperties(c)
      val setters = properties.map { p =>
        val fieldName = p.getName
        val fieldType = typeToken.method(p.getReadMethod).getReturnType
        val (dataType, nullable) = inferDataType(fieldType)
        val newTypePath = walkedTypePath.recordField(fieldType.getType.getTypeName, fieldName)
        val setter = expressionWithNullSafety(
          TypeInference.deserializerFor(JavaInference(fieldType),
            addToPath(path, fieldName, dataType, newTypePath),
            newTypePath),
          nullable = nullable,
          newTypePath)
        p.getWriteMethod.getName -> setter
      }.toMap

      val newInstance = NewInstance(c, Nil, ObjectType(c), propagateNull = false)
      val result = InitializeJavaBean(newInstance, setters)

      expressions.If(
        IsNull(path),
        expressions.Literal.create(null, ObjectType(c)),
        result
      )
  }
}

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

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types._

/**
 * Provides experimental support for generating catalyst schemas for scala objects.
 */
object ScalaReflection {
  import scala.reflect.runtime.universe._

  /** Returns a Sequence of attributes for the given case class type. */
  def attributesFor[T: TypeTag]: Seq[Attribute] = schemaFor[T] match {
    case s: StructType =>
      s.fields.map(f => AttributeReference(f.name, f.dataType, nullable = true)())
  }

  /** Returns a catalyst DataType for the given Scala Type using reflection. */
  def schemaFor[T: TypeTag]: DataType = schemaFor(typeOf[T])

  /** Returns a catalyst DataType for the given Scala Type using reflection. */
  def schemaFor(tpe: `Type`): DataType = tpe match {
    case t if t <:< typeOf[Option[_]] =>
      val TypeRef(_, _, Seq(optType)) = t
      schemaFor(optType)
    case t if t <:< typeOf[Product] =>
      val params = t.member("<init>": TermName).asMethod.paramss
      StructType(
        params.head.map(p =>
          StructField(p.name.toString, schemaFor(p.typeSignature), nullable = true)))
    // Need to decide if we actually need a special type here.
    case t if t <:< typeOf[Array[Byte]] => BinaryType
    case t if t <:< typeOf[Array[_]] =>
      sys.error(s"Only Array[Byte] supported now, use Seq instead of $t")
    case t if t <:< typeOf[Seq[_]] =>
      val TypeRef(_, _, Seq(elementType)) = t
      ArrayType(schemaFor(elementType))
    case t if t <:< typeOf[Map[_,_]] =>
      val TypeRef(_, _, Seq(keyType, valueType)) = t
      MapType(schemaFor(keyType), schemaFor(valueType))
    case t if t <:< typeOf[String] => StringType
    case t if t <:< typeOf[Timestamp] => TimestampType
    case t if t <:< typeOf[BigDecimal] => DecimalType
    case t if t <:< typeOf[java.lang.Integer] => IntegerType
    case t if t <:< typeOf[java.lang.Long] => LongType
    case t if t <:< typeOf[java.lang.Double] => DoubleType
    case t if t <:< typeOf[java.lang.Float] => FloatType
    case t if t <:< typeOf[java.lang.Short] => ShortType
    case t if t <:< typeOf[java.lang.Byte] => ByteType
    case t if t <:< typeOf[java.lang.Boolean] => BooleanType
    // TODO: The following datatypes could be marked as non-nullable.
    case t if t <:< definitions.IntTpe => IntegerType
    case t if t <:< definitions.LongTpe => LongType
    case t if t <:< definitions.DoubleTpe => DoubleType
    case t if t <:< definitions.FloatTpe => FloatType
    case t if t <:< definitions.ShortTpe => ShortType
    case t if t <:< definitions.ByteTpe => ByteType
    case t if t <:< definitions.BooleanTpe => BooleanType
  }

  implicit class CaseClassRelation[A <: Product : TypeTag](data: Seq[A]) {

    /**
     * Implicitly added to Sequences of case class objects.  Returns a catalyst logical relation
     * for the the data in the sequence.
     */
    def asRelation: LocalRelation = {
      val output = attributesFor[A]
      LocalRelation(output, data)
    }
  }
}

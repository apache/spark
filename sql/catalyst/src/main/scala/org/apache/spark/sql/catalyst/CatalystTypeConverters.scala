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

import java.lang.{Iterable => JavaIterable}
import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.Date
import java.util.{Map => JavaMap}

import scala.collection.mutable.HashMap

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateUtils
import org.apache.spark.sql.types._

/**
 * Functions to convert Scala types to Catalyst types and vice versa.
 */
object CatalystTypeConverters {
  // The Predef.Map is scala.collection.immutable.Map.
  // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
  import scala.collection.Map

  private def getConverterForType(dataType: DataType): CatalystTypeConverter[Any, Any] = {
    val converter = dataType match {
      // Check UDT first since UDTs can override other types
      case udt: UserDefinedType[_] => UDTConverter(udt)
      case arrayType: ArrayType => ArrayConverter(arrayType.elementType)
      case mapType: MapType => MapConverter(mapType.keyType, mapType.valueType)
      case structType: StructType => StructConverter(structType)
      case StringType => StringConverter
      case DateType => DateConverter
      case dt: DecimalType => BigDecimalConverter
      case BooleanType => BooleanConverter
      case ByteType => ByteConverter
      case ShortType => ShortConverter
      case IntegerType => IntConverter
      case LongType => LongConverter
      case FloatType => FloatConverter
      case DoubleType => DoubleConverter
      case _ => IdentityConverter
    }
    converter.asInstanceOf[CatalystTypeConverter[Any, Any]]
  }

  private abstract class CatalystTypeConverter[ScalaType, CatalystType] extends Serializable {

    final def toCatalyst(maybeScalaValue: Any): Any = {
      maybeScalaValue match {
        case None => null
        case null => null
        case Some(scalaValue: ScalaType) => toCatalystImpl(scalaValue)
        case scalaValue: ScalaType => toCatalystImpl(scalaValue)
      }
    }

    final def toScala(row: Row, column: Int): Any = {
      if (row.isNullAt(column)) null else toScalaImpl(row, column)
    }

    def toScala(catalystValue: CatalystType): ScalaType
    protected def toCatalystImpl(scalaValue: ScalaType): CatalystType
    protected def toScalaImpl(row: Row, column: Int): ScalaType
  }

  private abstract class PrimitiveCatalystTypeConverter[T] extends CatalystTypeConverter[T, T] {
    override final def toScala(catalystValue: T): T = catalystValue
    override final def toCatalystImpl(scalaValue: T): T = scalaValue
  }

  private object IdentityConverter extends CatalystTypeConverter[Any, Any] {
    override def toCatalystImpl(scalaValue: Any): Any = scalaValue
    override def toScala(catalystValue: Any): Any = catalystValue
    override def toScalaImpl(row: Row, column: Int): Any = row(column)
  }

  private case class UDTConverter(udt: UserDefinedType[_]) extends CatalystTypeConverter[Any, Any] {
    override def toCatalystImpl(scalaValue: Any): Any = udt.serialize(scalaValue)
    override def toScala(catalystValue: Any): Any = udt.deserialize(catalystValue)
    override def toScalaImpl(row: Row, column: Int): Any = toScala(row(column))
  }

  // Converter for array, seq, iterables.
  private case class ArrayConverter(
      elementType: DataType) extends CatalystTypeConverter[Any, Seq[Any]] {

    private[this] val elementConverter = getConverterForType(elementType)

    override def toCatalystImpl(scalaValue: Any): Seq[Any] = {
      scalaValue match {
        case a: Array[_] => a.toSeq.map(elementConverter.toCatalyst)
        case s: Seq[_] => s.map(elementConverter.toCatalyst)
        case i: JavaIterable[_] =>
          val iter = i.iterator
          var convertedIterable: List[Any] = List()
          while (iter.hasNext) {
            val item = iter.next()
            convertedIterable :+= elementConverter.toCatalyst(item)
          }
          convertedIterable
      }
    }

    override def toScala(catalystValue: Seq[Any]): Seq[Any] = {
      if (catalystValue == null) {
        null
      } else {
        catalystValue.asInstanceOf[Seq[_]].map(elementConverter.toScala)
      }
    }

    override def toScalaImpl(row: Row, column: Int): Seq[Any] =
      toScala(row(column).asInstanceOf[Seq[Any]])
  }

  private case class MapConverter(
      keyType: DataType,
      valueType: DataType
    ) extends CatalystTypeConverter[Any, Map[Any, Any]] {

    private[this] val keyConverter = getConverterForType(keyType)
    private[this] val valueConverter = getConverterForType(valueType)

    override def toCatalystImpl(scalaValue: Any): Map[Any, Any] = scalaValue match {
      case m: Map[_, _] =>
        m.map { case (k, v) =>
          keyConverter.toCatalyst(k) -> valueConverter.toCatalyst(v)
        }

      case jmap: JavaMap[_, _] =>
        val iter = jmap.entrySet.iterator
        val convertedMap: HashMap[Any, Any] = HashMap()
        while (iter.hasNext) {
          val entry = iter.next()
          val key = keyConverter.toCatalyst(entry.getKey)
          convertedMap(key) = valueConverter.toCatalyst(entry.getValue)
        }
        convertedMap
    }

    override def toScala(catalystValue: Map[Any, Any]): Map[Any, Any] = {
      if (catalystValue == null) {
        null
      } else {
        catalystValue.map { case (k, v) =>
          keyConverter.toScala(k) -> valueConverter.toScala(v)
        }
      }
    }

    override def toScalaImpl(row: Row, column: Int): Map[Any, Any] =
      toScala(row(column).asInstanceOf[Map[Any, Any]])
  }

  private case class StructConverter(
      structType: StructType) extends CatalystTypeConverter[Any, Row] {

    private[this] val converters = structType.fields.map { f => getConverterForType(f.dataType) }

    override def toCatalystImpl(scalaValue: Any): Row = scalaValue match {
      case row: Row =>
        val ar = new Array[Any](row.size)
        var idx = 0
        while (idx < row.size) {
          ar(idx) = converters(idx).toCatalyst(row(idx))
          idx += 1
        }
        new GenericRowWithSchema(ar, structType)

      case p: Product =>
        val ar = new Array[Any](structType.size)
        val iter = p.productIterator
        var idx = 0
        while (idx < structType.size) {
          ar(idx) = converters(idx).toCatalyst(iter.next())
          idx += 1
        }
        new GenericRowWithSchema(ar, structType)
    }

    override def toScala(row: Row): Row = {
      if (row == null) {
        null
      } else {
        val ar = new Array[Any](row.size)
        var idx = 0
        while (idx < row.size) {
          ar(idx) = converters(idx).toScala(row, idx)
          idx += 1
        }
        new GenericRowWithSchema(ar, structType)
      }
    }

    override def toScalaImpl(row: Row, column: Int): Row = toScala(row(column).asInstanceOf[Row])
  }

  private object StringConverter extends CatalystTypeConverter[Any, Any] {
    override def toCatalystImpl(scalaValue: Any): UTF8String = scalaValue match {
      case str: String => UTF8String(str)
      case utf8: UTF8String => utf8
    }
    override def toScala(catalystValue: Any): String = catalystValue match {
      case null => null
      case str: String => str
      case utf8: UTF8String => utf8.toString()
    }
    override def toScalaImpl(row: Row, column: Int): String = row(column).toString
  }

  private object DateConverter extends CatalystTypeConverter[Date, Any] {
    override def toCatalystImpl(scalaValue: Date): Int = DateUtils.fromJavaDate(scalaValue)
    override def toScala(catalystValue: Any): Date =
      if (catalystValue == null) null else DateUtils.toJavaDate(catalystValue.asInstanceOf[Int])
    override def toScalaImpl(row: Row, column: Int): Date = toScala(row.getInt(column))
  }

  private object BigDecimalConverter extends CatalystTypeConverter[Any, Decimal] {
    override def toCatalystImpl(scalaValue: Any): Decimal = scalaValue match {
      case d: BigDecimal => Decimal(d)
      case d: JavaBigDecimal => Decimal(d)
      case d: Decimal => d
    }
    override def toScala(catalystValue: Decimal): JavaBigDecimal = catalystValue.toJavaBigDecimal
    override def toScalaImpl(row: Row, column: Int): JavaBigDecimal = row.get(column) match {
      case d: JavaBigDecimal => d
      case d: Decimal => d.toJavaBigDecimal
    }
  }

  private object BooleanConverter extends PrimitiveCatalystTypeConverter[Boolean] {
    override def toScalaImpl(row: Row, column: Int): Boolean = row.getBoolean(column)
  }

  private object ByteConverter extends PrimitiveCatalystTypeConverter[Byte] {
    override def toScalaImpl(row: Row, column: Int): Byte = row.getByte(column)
  }

  private object ShortConverter extends PrimitiveCatalystTypeConverter[Short] {
    override def toScalaImpl(row: Row, column: Int): Short = row.getShort(column)
  }

  private object IntConverter extends PrimitiveCatalystTypeConverter[Int] {
    override def toScalaImpl(row: Row, column: Int): Int = row.getInt(column)
  }

  private object LongConverter extends PrimitiveCatalystTypeConverter[Long] {
    override def toScalaImpl(row: Row, column: Int): Long = row.getLong(column)
  }

  private object FloatConverter extends PrimitiveCatalystTypeConverter[Float] {
    override def toScalaImpl(row: Row, column: Int): Float = row.getFloat(column)
  }

  private object DoubleConverter extends PrimitiveCatalystTypeConverter[Double] {
    override def toScalaImpl(row: Row, column: Int): Double = row.getDouble(column)
  }

  /**
   * Converts Scala objects to catalyst rows / types. This method is slow, and for batch
   * conversion you should be using converter produced by createToCatalystConverter.
   * Note: This is always called after schemaFor has been called.
   *       This ordering is important for UDT registration.
   */
  def convertToCatalyst(scalaValue: Any, dataType: DataType): Any = {
    // Check UDT first since UDTs can override other types
    dataType match {
      case udt: UserDefinedType[_] => udt.serialize(scalaValue)
      case option: Option[_] => option.map(convertToCatalyst(_, dataType)).orNull
      case _ => getConverterForType(dataType).toCatalyst(scalaValue)
    }
  }

  /**
   * Creates a converter function that will convert Scala objects to the specified catalyst type.
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  private[sql] def createToCatalystConverter(dataType: DataType): Any => Any = {
    getConverterForType(dataType).toCatalyst
  }

  /**
   *  Converts Scala objects to catalyst rows / types.
   *
   *  Note: This should be called before do evaluation on Row
   *        (It does not support UDT)
   *  This is used to create an RDD or test results with correct types for Catalyst.
   */
  def convertToCatalyst(a: Any): Any = a match {
    case s: String => StringConverter.toCatalyst(s)
    case d: Date => DateConverter.toCatalyst(d)
    case d: BigDecimal => BigDecimalConverter.toCatalyst(d)
    case d: JavaBigDecimal => BigDecimalConverter.toCatalyst(d)
    case seq: Seq[Any] => seq.map(convertToCatalyst)
    case r: Row => Row(r.toSeq.map(convertToCatalyst): _*)
    case arr: Array[Any] => arr.toSeq.map(convertToCatalyst).toArray
    case m: Map[Any, Any] =>
      m.map { case (k, v) => (convertToCatalyst(k), convertToCatalyst(v)) }.toMap
    case other => other
  }

  /** 
   * Converts Catalyst types used internally in rows to standard Scala types
   * This method is slow, and for batch conversion you should be using converter
   * produced by createToScalaConverter.
   */
  def convertToScala(catalystValue: Any, dataType: DataType): Any = {
    getConverterForType(dataType).toScala(catalystValue)
  }

  /**
   * Creates a converter function that will convert Catalyst types to Scala type.
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  private[sql] def createToScalaConverter(dataType: DataType): Any => Any = {
    getConverterForType(dataType).toScala
  }
}

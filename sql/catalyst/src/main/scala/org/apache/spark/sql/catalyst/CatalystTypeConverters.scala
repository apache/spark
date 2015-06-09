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
import java.sql.{Timestamp, Date}
import java.util.{Map => JavaMap}
import javax.annotation.Nullable

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

  private def isPrimitive(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case _ => false
    }
  }

  private def getConverterForType(dataType: DataType): CatalystTypeConverter[Any, Any, Any] = {
    val converter = dataType match {
      case udt: UserDefinedType[_] => UDTConverter(udt)
      case arrayType: ArrayType => ArrayConverter(arrayType.elementType)
      case mapType: MapType => MapConverter(mapType.keyType, mapType.valueType)
      case structType: StructType => StructConverter(structType)
      case StringType => StringConverter
      case DateType => DateConverter
      case TimestampType => TimestampConverter
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
    converter.asInstanceOf[CatalystTypeConverter[Any, Any, Any]]
  }

  /**
   * Converts a Scala type to its Catalyst equivalent (and vice versa).
   *
   * @tparam ScalaInputType The type of Scala values that can be converted to Catalyst.
   * @tparam ScalaOutputType The type of Scala values returned when converting Catalyst to Scala.
   * @tparam CatalystType The internal Catalyst type used to represent values of this Scala type.
   */
  private abstract class CatalystTypeConverter[ScalaInputType, ScalaOutputType, CatalystType]
    extends Serializable {

    /**
     * Converts a Scala type to its Catalyst equivalent while automatically handling nulls
     * and Options.
     */
    final def toCatalyst(@Nullable maybeScalaValue: Any): CatalystType = {
      if (maybeScalaValue == null) {
        null.asInstanceOf[CatalystType]
      } else if (maybeScalaValue.isInstanceOf[Option[ScalaInputType]]) {
        val opt = maybeScalaValue.asInstanceOf[Option[ScalaInputType]]
        if (opt.isDefined) {
          toCatalystImpl(opt.get)
        } else {
          null.asInstanceOf[CatalystType]
        }
      } else {
        toCatalystImpl(maybeScalaValue.asInstanceOf[ScalaInputType])
      }
    }

    /**
     * Given a Catalyst row, convert the value at column `column` to its Scala equivalent.
     */
    final def toScala(row: Row, column: Int): ScalaOutputType = {
      if (row.isNullAt(column)) null.asInstanceOf[ScalaOutputType] else toScalaImpl(row, column)
    }

    /**
     * Convert a Catalyst value to its Scala equivalent.
     */
    def toScala(@Nullable catalystValue: CatalystType): ScalaOutputType

    /**
     * Converts a Scala value to its Catalyst equivalent.
     * @param scalaValue the Scala value, guaranteed not to be null.
     * @return the Catalyst value.
     */
    protected def toCatalystImpl(scalaValue: ScalaInputType): CatalystType

    /**
     * Given a Catalyst row, convert the value at column `column` to its Scala equivalent.
     * This method will only be called on non-null columns.
     */
    protected def toScalaImpl(row: Row, column: Int): ScalaOutputType
  }

  private object IdentityConverter extends CatalystTypeConverter[Any, Any, Any] {
    override def toCatalystImpl(scalaValue: Any): Any = scalaValue
    override def toScala(catalystValue: Any): Any = catalystValue
    override def toScalaImpl(row: Row, column: Int): Any = row(column)
  }

  private case class UDTConverter(
      udt: UserDefinedType[_]) extends CatalystTypeConverter[Any, Any, Any] {
    override def toCatalystImpl(scalaValue: Any): Any = udt.serialize(scalaValue)
    override def toScala(catalystValue: Any): Any = udt.deserialize(catalystValue)
    override def toScalaImpl(row: Row, column: Int): Any = toScala(row(column))
  }

  /** Converter for arrays, sequences, and Java iterables. */
  private case class ArrayConverter(
      elementType: DataType) extends CatalystTypeConverter[Any, Seq[Any], Seq[Any]] {

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
      valueType: DataType)
    extends CatalystTypeConverter[Any, Map[Any, Any], Map[Any, Any]] {

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
      structType: StructType) extends CatalystTypeConverter[Any, Row, Row] {

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

  private object StringConverter extends CatalystTypeConverter[Any, String, Any] {
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

  private object DateConverter extends CatalystTypeConverter[Date, Date, Any] {
    override def toCatalystImpl(scalaValue: Date): Int = DateUtils.fromJavaDate(scalaValue)
    override def toScala(catalystValue: Any): Date =
      if (catalystValue == null) null else DateUtils.toJavaDate(catalystValue.asInstanceOf[Int])
    override def toScalaImpl(row: Row, column: Int): Date = toScala(row.getInt(column))
  }

  private object TimestampConverter extends CatalystTypeConverter[Timestamp, Timestamp, Any] {
    override def toCatalystImpl(scalaValue: Timestamp): Long = DateUtils.fromTimestamp(scalaValue)
    override def toScala(catalystValue: Any): Timestamp =
      if (catalystValue == null) null else DateUtils.toTimestamp(catalystValue.asInstanceOf[Long])
    override def toScalaImpl(row: Row, column: Int): Timestamp = toScala(row.getLong(column))
  }

  private object BigDecimalConverter extends CatalystTypeConverter[Any, JavaBigDecimal, Decimal] {
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

  private abstract class PrimitiveConverter[T] extends CatalystTypeConverter[T, Any, Any] {
    final override def toScala(catalystValue: Any): Any = catalystValue
    final override def toCatalystImpl(scalaValue: T): Any = scalaValue
  }

  private object BooleanConverter extends PrimitiveConverter[Boolean] {
    override def toScalaImpl(row: Row, column: Int): Boolean = row.getBoolean(column)
  }

  private object ByteConverter extends PrimitiveConverter[Byte] {
    override def toScalaImpl(row: Row, column: Int): Byte = row.getByte(column)
  }

  private object ShortConverter extends PrimitiveConverter[Short] {
    override def toScalaImpl(row: Row, column: Int): Short = row.getShort(column)
  }

  private object IntConverter extends PrimitiveConverter[Int] {
    override def toScalaImpl(row: Row, column: Int): Int = row.getInt(column)
  }

  private object LongConverter extends PrimitiveConverter[Long] {
    override def toScalaImpl(row: Row, column: Int): Long = row.getLong(column)
  }

  private object FloatConverter extends PrimitiveConverter[Float] {
    override def toScalaImpl(row: Row, column: Int): Float = row.getFloat(column)
  }

  private object DoubleConverter extends PrimitiveConverter[Double] {
    override def toScalaImpl(row: Row, column: Int): Double = row.getDouble(column)
  }

  /**
   * Converts Scala objects to catalyst rows / types. This method is slow, and for batch
   * conversion you should be using converter produced by createToCatalystConverter.
   * Note: This is always called after schemaFor has been called.
   *       This ordering is important for UDT registration.
   */
  def convertToCatalyst(scalaValue: Any, dataType: DataType): Any = {
    getConverterForType(dataType).toCatalyst(scalaValue)
  }

  /**
   * Creates a converter function that will convert Scala objects to the specified Catalyst type.
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  private[sql] def createToCatalystConverter(dataType: DataType): Any => Any = {
    if (isPrimitive(dataType)) {
      // Although the `else` branch here is capable of handling inbound conversion of primitives,
      // we add some special-case handling for those types here. The motivation for this relates to
      // Java method invocation costs: if we have rows that consist entirely of primitive columns,
      // then returning the same conversion function for all of the columns means that the call site
      // will be monomorphic instead of polymorphic. In microbenchmarks, this actually resulted in
      // a measurable performance impact. Note that this optimization will be unnecessary if we
      // use code generation to construct Scala Row -> Catalyst Row converters.
      def convert(maybeScalaValue: Any): Any = {
        if (maybeScalaValue.isInstanceOf[Option[Any]]) {
          maybeScalaValue.asInstanceOf[Option[Any]].orNull
        } else {
          maybeScalaValue
        }
      }
      convert
    } else {
      getConverterForType(dataType).toCatalyst
    }
  }

  /**
   *  Converts Scala objects to Catalyst rows / types.
   *
   *  Note: This should be called before do evaluation on Row
   *        (It does not support UDT)
   *  This is used to create an RDD or test results with correct types for Catalyst.
   */
  def convertToCatalyst(a: Any): Any = a match {
    case s: String => StringConverter.toCatalyst(s)
    case d: Date => DateConverter.toCatalyst(d)
    case t: Timestamp => TimestampConverter.toCatalyst(t)
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

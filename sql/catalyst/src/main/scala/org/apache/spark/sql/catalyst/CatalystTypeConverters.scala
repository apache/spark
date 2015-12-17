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
import java.sql.{Date, Timestamp}
import java.util.{Map => JavaMap}
import javax.annotation.Nullable

import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Functions to convert Scala types to Catalyst types and vice versa.
 */
object CatalystTypeConverters {
  // The Predef.Map is scala.collection.immutable.Map.
  // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
  import scala.collection.Map

  lazy val universe = ScalaReflection.universe

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
      case dt: DecimalType => new DecimalConverter(dt)
      case BooleanType => BooleanConverter
      case ByteType => ByteConverter
      case ShortType => ShortConverter
      case IntegerType => IntConverter
      case LongType => LongConverter
      case FloatType => FloatConverter
      case DoubleType => DoubleConverter
      case dataType: DataType => IdentityConverter(dataType)
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
    final def toScala(row: InternalRow, column: Int): ScalaOutputType = {
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
    protected def toScalaImpl(row: InternalRow, column: Int): ScalaOutputType
  }

  private case class IdentityConverter(dataType: DataType)
    extends CatalystTypeConverter[Any, Any, Any] {
    override def toCatalystImpl(scalaValue: Any): Any = scalaValue
    override def toScala(catalystValue: Any): Any = catalystValue
    override def toScalaImpl(row: InternalRow, column: Int): Any = row.get(column, dataType)
  }

  private case class UDTConverter(
      udt: UserDefinedType[_]) extends CatalystTypeConverter[Any, Any, Any] {
    // toCatalyst (it calls toCatalystImpl) will do null check.
    override def toCatalystImpl(scalaValue: Any): Any = udt.serialize(scalaValue)

    override def toScala(catalystValue: Any): Any = {
      if (catalystValue == null) null else udt.deserialize(catalystValue)
    }

    override def toScalaImpl(row: InternalRow, column: Int): Any =
      toScala(row.get(column, udt.sqlType))
  }

  /** Converter for arrays, sequences, and Java iterables. */
  private case class ArrayConverter(
      elementType: DataType) extends CatalystTypeConverter[Any, Seq[Any], ArrayData] {

    private[this] val elementConverter = getConverterForType(elementType)

    override def toCatalystImpl(scalaValue: Any): ArrayData = {
      scalaValue match {
        case a: Array[_] =>
          new GenericArrayData(a.map(elementConverter.toCatalyst))
        case s: Seq[_] =>
          new GenericArrayData(s.map(elementConverter.toCatalyst).toArray)
        case i: JavaIterable[_] =>
          val iter = i.iterator
          val convertedIterable = scala.collection.mutable.ArrayBuffer.empty[Any]
          while (iter.hasNext) {
            val item = iter.next()
            convertedIterable += elementConverter.toCatalyst(item)
          }
          new GenericArrayData(convertedIterable.toArray)
      }
    }

    override def toScala(catalystValue: ArrayData): Seq[Any] = {
      if (catalystValue == null) {
        null
      } else if (isPrimitive(elementType)) {
        catalystValue.toArray[Any](elementType)
      } else {
        val result = new Array[Any](catalystValue.numElements())
        catalystValue.foreach(elementType, (i, e) => {
          result(i) = elementConverter.toScala(e)
        })
        result
      }
    }

    override def toScalaImpl(row: InternalRow, column: Int): Seq[Any] =
      toScala(row.getArray(column))
  }

  private case class MapConverter(
      keyType: DataType,
      valueType: DataType)
    extends CatalystTypeConverter[Any, Map[Any, Any], MapData] {

    private[this] val keyConverter = getConverterForType(keyType)
    private[this] val valueConverter = getConverterForType(valueType)

    override def toCatalystImpl(scalaValue: Any): MapData = scalaValue match {
      case m: Map[_, _] =>
        val length = m.size
        val convertedKeys = new Array[Any](length)
        val convertedValues = new Array[Any](length)

        var i = 0
        for ((key, value) <- m) {
          convertedKeys(i) = keyConverter.toCatalyst(key)
          convertedValues(i) = valueConverter.toCatalyst(value)
          i += 1
        }
        ArrayBasedMapData(convertedKeys, convertedValues)

      case jmap: JavaMap[_, _] =>
        val length = jmap.size()
        val convertedKeys = new Array[Any](length)
        val convertedValues = new Array[Any](length)

        var i = 0
        val iter = jmap.entrySet.iterator
        while (iter.hasNext) {
          val entry = iter.next()
          convertedKeys(i) = keyConverter.toCatalyst(entry.getKey)
          convertedValues(i) = valueConverter.toCatalyst(entry.getValue)
          i += 1
        }
        ArrayBasedMapData(convertedKeys, convertedValues)
    }

    override def toScala(catalystValue: MapData): Map[Any, Any] = {
      if (catalystValue == null) {
        null
      } else {
        val keys = catalystValue.keyArray().toArray[Any](keyType)
        val values = catalystValue.valueArray().toArray[Any](valueType)
        val convertedKeys =
          if (isPrimitive(keyType)) keys else keys.map(keyConverter.toScala)
        val convertedValues =
          if (isPrimitive(valueType)) values else values.map(valueConverter.toScala)

        convertedKeys.zip(convertedValues).toMap
      }
    }

    override def toScalaImpl(row: InternalRow, column: Int): Map[Any, Any] =
      toScala(row.getMap(column))
  }

  private case class StructConverter(
      structType: StructType) extends CatalystTypeConverter[Any, Row, InternalRow] {

    private[this] val converters = structType.fields.map { f => getConverterForType(f.dataType) }

    override def toCatalystImpl(scalaValue: Any): InternalRow = scalaValue match {
      case row: Row =>
        val ar = new Array[Any](row.size)
        var idx = 0
        while (idx < row.size) {
          ar(idx) = converters(idx).toCatalyst(row(idx))
          idx += 1
        }
        new GenericInternalRow(ar)

      case p: Product =>
        val ar = new Array[Any](structType.size)
        val iter = p.productIterator
        var idx = 0
        while (idx < structType.size) {
          ar(idx) = converters(idx).toCatalyst(iter.next())
          idx += 1
        }
        new GenericInternalRow(ar)
    }

    override def toScala(row: InternalRow): Row = {
      if (row == null) {
        null
      } else {
        val ar = new Array[Any](row.numFields)
        var idx = 0
        while (idx < row.numFields) {
          ar(idx) = converters(idx).toScala(row, idx)
          idx += 1
        }
        new GenericRowWithSchema(ar, structType)
      }
    }

    override def toScalaImpl(row: InternalRow, column: Int): Row =
      toScala(row.getStruct(column, structType.size))
  }

  private object StringConverter extends CatalystTypeConverter[Any, String, UTF8String] {
    override def toCatalystImpl(scalaValue: Any): UTF8String = scalaValue match {
      case str: String => UTF8String.fromString(str)
      case utf8: UTF8String => utf8
    }
    override def toScala(catalystValue: UTF8String): String =
      if (catalystValue == null) null else catalystValue.toString
    override def toScalaImpl(row: InternalRow, column: Int): String =
      row.getUTF8String(column).toString
  }

  private object DateConverter extends CatalystTypeConverter[Date, Date, Any] {
    override def toCatalystImpl(scalaValue: Date): Int = DateTimeUtils.fromJavaDate(scalaValue)
    override def toScala(catalystValue: Any): Date =
      if (catalystValue == null) null else DateTimeUtils.toJavaDate(catalystValue.asInstanceOf[Int])
    override def toScalaImpl(row: InternalRow, column: Int): Date =
      DateTimeUtils.toJavaDate(row.getInt(column))
  }

  private object TimestampConverter extends CatalystTypeConverter[Timestamp, Timestamp, Any] {
    override def toCatalystImpl(scalaValue: Timestamp): Long =
      DateTimeUtils.fromJavaTimestamp(scalaValue)
    override def toScala(catalystValue: Any): Timestamp =
      if (catalystValue == null) null
      else DateTimeUtils.toJavaTimestamp(catalystValue.asInstanceOf[Long])
    override def toScalaImpl(row: InternalRow, column: Int): Timestamp =
      DateTimeUtils.toJavaTimestamp(row.getLong(column))
  }

  private class DecimalConverter(dataType: DecimalType)
    extends CatalystTypeConverter[Any, JavaBigDecimal, Decimal] {
    override def toCatalystImpl(scalaValue: Any): Decimal = {
      val decimal = scalaValue match {
        case d: BigDecimal => Decimal(d)
        case d: JavaBigDecimal => Decimal(d)
        case d: Decimal => d
      }
      if (decimal.changePrecision(dataType.precision, dataType.scale)) {
        decimal
      } else {
        null
      }
    }
    override def toScala(catalystValue: Decimal): JavaBigDecimal = {
      if (catalystValue == null) null
      else catalystValue.toJavaBigDecimal
    }
    override def toScalaImpl(row: InternalRow, column: Int): JavaBigDecimal =
      row.getDecimal(column, dataType.precision, dataType.scale).toJavaBigDecimal
  }

  private abstract class PrimitiveConverter[T] extends CatalystTypeConverter[T, Any, Any] {
    final override def toScala(catalystValue: Any): Any = catalystValue
    final override def toCatalystImpl(scalaValue: T): Any = scalaValue
  }

  private object BooleanConverter extends PrimitiveConverter[Boolean] {
    override def toScalaImpl(row: InternalRow, column: Int): Boolean = row.getBoolean(column)
  }

  private object ByteConverter extends PrimitiveConverter[Byte] {
    override def toScalaImpl(row: InternalRow, column: Int): Byte = row.getByte(column)
  }

  private object ShortConverter extends PrimitiveConverter[Short] {
    override def toScalaImpl(row: InternalRow, column: Int): Short = row.getShort(column)
  }

  private object IntConverter extends PrimitiveConverter[Int] {
    override def toScalaImpl(row: InternalRow, column: Int): Int = row.getInt(column)
  }

  private object LongConverter extends PrimitiveConverter[Long] {
    override def toScalaImpl(row: InternalRow, column: Int): Long = row.getLong(column)
  }

  private object FloatConverter extends PrimitiveConverter[Float] {
    override def toScalaImpl(row: InternalRow, column: Int): Float = row.getFloat(column)
  }

  private object DoubleConverter extends PrimitiveConverter[Double] {
    override def toScalaImpl(row: InternalRow, column: Int): Double = row.getDouble(column)
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
   * Creates a converter function that will convert Catalyst types to Scala type.
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  private[sql] def createToScalaConverter(dataType: DataType): Any => Any = {
    if (isPrimitive(dataType)) {
      identity
    } else {
      getConverterForType(dataType).toScala
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
    case d: BigDecimal => new DecimalConverter(DecimalType(d.precision, d.scale)).toCatalyst(d)
    case d: JavaBigDecimal => new DecimalConverter(DecimalType(d.precision, d.scale)).toCatalyst(d)
    case seq: Seq[Any] => new GenericArrayData(seq.map(convertToCatalyst).toArray)
    case r: Row => InternalRow(r.toSeq.map(convertToCatalyst): _*)
    case arr: Array[Any] => new GenericArrayData(arr.map(convertToCatalyst))
    case m: Map[_, _] =>
      val length = m.size
      val convertedKeys = new Array[Any](length)
      val convertedValues = new Array[Any](length)

      var i = 0
      for ((key, value) <- m) {
        convertedKeys(i) = convertToCatalyst(key)
        convertedValues(i) = convertToCatalyst(value)
        i += 1
      }
      ArrayBasedMapData(convertedKeys, convertedValues)
    case other => other
  }

  /**
   * Converts Catalyst types used internally in rows to standard Scala types
   * This method is slow, and for batch conversion you should be using converter
   * produced by createToScalaConverter.
   */
  def convertToScala(catalystValue: Any, dataType: DataType): Any = {
    createToScalaConverter(dataType)(catalystValue)
  }

  /**
   * Like createToScalaConverter(DataType), creates a function that converts a Catalyst object to a
   * Scala object; however, in this case, the Scala object is an instance of a subtype of Product
   * (e.g. a case class).
   *
   * If the given Scala type is not compatible with the given structType, this method ultimately
   * throws a ClassCastException when the converter is invoked.
   *
   * Typical use case would be converting a collection of rows that have the same schema. You will
   * call this function once to get a converter, and apply it to every row.
   */
  private[sql] def createToProductConverter[T <: Product](
    structType: StructType)(implicit classTag: ClassTag[T]): InternalRow => T = {

    // Use ScalaReflectionLock, to avoid reflection thread safety issues in 2.10.
    // https://issues.scala-lang.org/browse/SI-6240
    // http://docs.scala-lang.org/overviews/reflection/thread-safety.html
    ScalaReflectionLock.synchronized { createToProductConverter(classTag, structType) }
  }

  private[sql] def createToProductConverter[T <: Product](
    classTag: ClassTag[T], structType: StructType): InternalRow => T = {

    import universe._

    val constructorMirror = {
      val mirror = runtimeMirror(Utils.getContextOrSparkClassLoader)
      val classSymbol = mirror.classSymbol(classTag.runtimeClass)
      val classMirror = mirror.reflectClass(classSymbol)
      val constructorSymbol = {
        // Adapted from ScalaReflection to find primary constructor.
        // https://issues.apache.org/jira/browse/SPARK-4791
        val symbol = classSymbol.toType.declaration(nme.CONSTRUCTOR)
        if (symbol.isMethod) {
          symbol.asMethod
        } else {
          val candidateSymbol =
            symbol.asTerm.alternatives.find { s => s.isMethod && s.asMethod.isPrimaryConstructor }
          if (candidateSymbol.isDefined) {
            candidateSymbol.get.asMethod
          } else {
            throw new IllegalArgumentException(s"No primary constructor for ${symbol.name}")
          }
        }
      }
      classMirror.reflectConstructor(constructorSymbol)
    }

    val params = constructorMirror.symbol.paramss.head.toSeq
    val paramTypes = params.map { _.asTerm.typeSignature }
    val fields = structType.fields
    val dataTypes = fields.map { _.dataType }
    val converters: Seq[Any => Any] =
      paramTypes.zip(dataTypes).map { case (pt, dt) => createToScalaConverter(pt, dt) }

    (row: InternalRow) => if (row == null) {
      null.asInstanceOf[T]
    } else {
      val convertedArgs =
        converters.zip(row.toSeq(dataTypes)).map { case (converter, arg) => converter(arg) }
      try {
        constructorMirror.apply(convertedArgs: _*).asInstanceOf[T]
      } catch {
        case e: IllegalArgumentException => // argument type mismatch
          val message =
            s"""|Error constructing ${classTag.runtimeClass.getName}: ${e.getMessage};
                |paramTypes: ${paramTypes}, dataTypes: ${dataTypes},
                |convertedArgs: ${convertedArgs}""".stripMargin.replace("\n", " ")
          throw new ClassCastException(message)
      }
    }
  }

  /**
   * Like createToScalaConverter(DataType), but with a Scala type hint.
   *
   * Please keep in sync with createToScalaConverter(DataType) and ScalaReflection.schemaFor[T].
   */
  private[sql] def createToScalaConverter(
    universeType: universe.Type, dataType: DataType): Any => Any = {

    import universe._

    (universeType, dataType) match {
      case (t, dt) if t <:< typeOf[Option[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val converter: Any => Any = createToScalaConverter(elementType, dt)
          (catalystValue: Any) => Option(converter(catalystValue))

      case (t, udt: UserDefinedType[_]) =>
        (catalystValue: Any) => if (catalystValue == null) null else udt.deserialize(catalystValue)

      case (t, bt: BinaryType) => identity

      case (t, at: ArrayType) if t <:< typeOf[Array[_]] =>
        throw new UnsupportedOperationException("Array[_] is not supported; try using Seq instead.")

      case (t, at: ArrayType) if t <:< typeOf[Seq[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val converter: Any => Any = createToScalaConverter(elementType, at.elementType)
        (catalystValue: Any) => catalystValue match {
          case arrayData: ArrayData => arrayData.toArray[Any](at.elementType).map(converter).toSeq
          case o => o
        }

      case (t, mt: MapType) if t <:< typeOf[Map[_, _]] =>
        val TypeRef(_, _, Seq(keyType, valueType)) = t
        val keyConverter: Any => Any = createToScalaConverter(keyType, mt.keyType)
        val valueConverter: Any => Any = createToScalaConverter(valueType, mt.valueType)
        (catalystValue: Any) => catalystValue match {
          case mapData: MapData =>
            val keys = mapData.keyArray().toArray[Any](mt.keyType)
            val values = mapData.valueArray().toArray[Any](mt.valueType)
            keys.map(keyConverter).zip(values.map(valueConverter)).toMap
          case o => o
        }

      case (t, st: StructType) if t <:< typeOf[Product] =>
        val className = t.erasure.typeSymbol.asClass.fullName
        val classTag = if (Utils.classIsLoadable(className)) {
          scala.reflect.ClassTag(Utils.classForName(className))
        } else {
          throw new IllegalArgumentException(s"$className is not loadable")
        }
        createToProductConverter(classTag, st).asInstanceOf[Any => Any]

      case (t, StringType) if t <:< typeOf[String] =>
        (catalystValue: Any) => catalystValue match {
          case utf8: UTF8String => utf8.toString
          case o => o
        }

      case (t, DateType) if t <:< typeOf[Date] =>
        (catalystValue: Any) => catalystValue match {
          case i: Int => DateTimeUtils.toJavaDate(i)
          case o => o
        }

      case (t, TimestampType) if t <:< typeOf[Timestamp] =>
        (catalystValue: Any) => catalystValue match {
          case x: Long => DateTimeUtils.toJavaTimestamp(x)
          case o => o
        }

      case (t, _: DecimalType) if t <:< typeOf[BigDecimal] =>
        (catalystValue: Any) => catalystValue match {
          case d: Decimal => d.toBigDecimal
          case o => o
        }

      case (t, _: DecimalType) if t <:< typeOf[java.math.BigDecimal] =>
        (catalystValue: Any) => catalystValue match {
          case d: Decimal => d.toJavaBigDecimal
          case o => o
        }

      // Pass non-string primitives through. (Strings are converted from UTF8Strings above.)
      // For everything else, hope for the best.
      case (t, o) => identity
    }
  }
}

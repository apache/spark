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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.Base64

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.hashing.MurmurHash3

import org.json4s.{JArray, JBool, JDecimal, JDouble, JField, JLong, JNull, JObject, JString}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.{compact, pretty, render}

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.annotation.{Stable, Unstable}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.util.{DateFormatter, SparkDateTimeUtils, TimestampFormatter, UDTUtils}
import org.apache.spark.sql.errors.DataTypeErrors
import org.apache.spark.sql.errors.DataTypeErrors.{toSQLType, toSQLValue}
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.ArrayImplicits._

/**
 * @since 1.3.0
 */
@Stable
object Row {

  /**
   * This method can be used to extract fields from a [[Row]] object in a pattern match. Example:
   * {{{
   * import org.apache.spark.sql._
   *
   * val pairs = sql("SELECT key, value FROM src").rdd.map {
   *   case Row(key: Int, value: String) =>
   *     key -> value
   * }
   * }}}
   */
  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row.toSeq)

  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(values: Any*): Row = new GenericRow(values.toArray)

  /**
   * This method can be used to construct a [[Row]] from a `Seq` of values.
   */
  def fromSeq(values: Seq[Any]): Row = new GenericRow(values.toArray)

  def fromTuple(tuple: Product): Row = fromSeq(tuple.productIterator.toSeq)

  /**
   * Merge multiple rows into a single row, one after another.
   */
  @deprecated("This method is deprecated and will be removed in future versions.", "3.0.0")
  def merge(rows: Row*): Row = {
    // TODO: Improve the performance of this if used in performance critical part.
    new GenericRow(rows.flatMap(_.toSeq).toArray)
  }

  /** Returns an empty row. */
  val empty = apply()
}

/**
 * Represents one row of output from a relational operator. Allows both generic access by ordinal,
 * which will incur boxing overhead for primitives, as well as native primitive access.
 *
 * It is invalid to use the native primitive interface to retrieve a value that is null, instead a
 * user must check `isNullAt` before attempting to retrieve a value that might be null.
 *
 * To create a new Row, use `RowFactory.create()` in Java or `Row.apply()` in Scala.
 *
 * A [[Row]] object can be constructed by providing field values. Example:
 * {{{
 * import org.apache.spark.sql._
 *
 * // Create a Row from values.
 * Row(value1, value2, value3, ...)
 * // Create a Row from a Seq of values.
 * Row.fromSeq(Seq(value1, value2, ...))
 * }}}
 *
 * A value of a row can be accessed through both generic access by ordinal, which will incur
 * boxing overhead for primitives, as well as native primitive access. An example of generic
 * access by ordinal:
 * {{{
 * import org.apache.spark.sql._
 *
 * val row = Row(1, true, "a string", null)
 * // row: Row = [1,true,a string,null]
 * val firstValue = row(0)
 * // firstValue: Any = 1
 * val fourthValue = row(3)
 * // fourthValue: Any = null
 * }}}
 *
 * For native primitive access, it is invalid to use the native primitive interface to retrieve a
 * value that is null, instead a user must check `isNullAt` before attempting to retrieve a value
 * that might be null. An example of native primitive access:
 * {{{
 * // using the row from the previous example.
 * val firstValue = row.getInt(0)
 * // firstValue: Int = 1
 * val isNull = row.isNullAt(3)
 * // isNull: Boolean = true
 * }}}
 *
 * In Scala, fields in a [[Row]] object can be extracted in a pattern match. Example:
 * {{{
 * import org.apache.spark.sql._
 *
 * val pairs = sql("SELECT key, value FROM src").rdd.map {
 *   case Row(key: Int, value: String) =>
 *     key -> value
 * }
 * }}}
 *
 * @since 1.3.0
 */
@Stable
trait Row extends Serializable {

  /** Number of elements in the Row. */
  def size: Int = length

  /** Number of elements in the Row. */
  def length: Int

  /**
   * Schema for the row.
   */
  def schema: StructType = null

  /**
   * Returns the value at position i. If the value is null, null is returned. The following is a
   * mapping between Spark SQL types and return types:
   *
   * {{{
   *   BooleanType -> java.lang.Boolean
   *   ByteType -> java.lang.Byte
   *   ShortType -> java.lang.Short
   *   IntegerType -> java.lang.Integer
   *   LongType -> java.lang.Long
   *   FloatType -> java.lang.Float
   *   DoubleType -> java.lang.Double
   *   StringType -> String
   *   DecimalType -> java.math.BigDecimal
   *
   *   DateType -> java.sql.Date if spark.sql.datetime.java8API.enabled is false
   *   DateType -> java.time.LocalDate if spark.sql.datetime.java8API.enabled is true
   *
   *   TimestampType -> java.sql.Timestamp if spark.sql.datetime.java8API.enabled is false
   *   TimestampType -> java.time.Instant if spark.sql.datetime.java8API.enabled is true
   *
   *   BinaryType -> byte array
   *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
   *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
   *   StructType -> org.apache.spark.sql.Row
   * }}}
   */
  def apply(i: Int): Any = get(i)

  /**
   * Returns the value at position i. If the value is null, null is returned. The following is a
   * mapping between Spark SQL types and return types:
   *
   * {{{
   *   BooleanType -> java.lang.Boolean
   *   ByteType -> java.lang.Byte
   *   ShortType -> java.lang.Short
   *   IntegerType -> java.lang.Integer
   *   LongType -> java.lang.Long
   *   FloatType -> java.lang.Float
   *   DoubleType -> java.lang.Double
   *   StringType -> String
   *   DecimalType -> java.math.BigDecimal
   *
   *   DateType -> java.sql.Date if spark.sql.datetime.java8API.enabled is false
   *   DateType -> java.time.LocalDate if spark.sql.datetime.java8API.enabled is true
   *
   *   TimestampType -> java.sql.Timestamp if spark.sql.datetime.java8API.enabled is false
   *   TimestampType -> java.time.Instant if spark.sql.datetime.java8API.enabled is true
   *
   *   BinaryType -> byte array
   *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
   *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
   *   StructType -> org.apache.spark.sql.Row
   * }}}
   */
  def get(i: Int): Any

  /** Checks whether the value at position i is null. */
  def isNullAt(i: Int): Boolean = get(i) == null

  /**
   * Returns the value at position i as a primitive boolean.
   *
   * @throws ClassCastException
   *   when data type does not match.
   * @throws org.apache.spark.SparkRuntimeException
   *   when value is null.
   */
  def getBoolean(i: Int): Boolean = getAnyValAs[Boolean](i)

  /**
   * Returns the value at position i as a primitive byte.
   *
   * @throws ClassCastException
   *   when data type does not match.
   * @throws org.apache.spark.SparkRuntimeException
   *   when value is null.
   */
  def getByte(i: Int): Byte = getAnyValAs[Byte](i)

  /**
   * Returns the value at position i as a primitive short.
   *
   * @throws ClassCastException
   *   when data type does not match.
   * @throws org.apache.spark.SparkRuntimeException
   *   when value is null.
   */
  def getShort(i: Int): Short = getAnyValAs[Short](i)

  /**
   * Returns the value at position i as a primitive int.
   *
   * @throws ClassCastException
   *   when data type does not match.
   * @throws org.apache.spark.SparkRuntimeException
   *   when value is null.
   */
  def getInt(i: Int): Int = getAnyValAs[Int](i)

  /**
   * Returns the value at position i as a primitive long.
   *
   * @throws ClassCastException
   *   when data type does not match.
   * @throws org.apache.spark.SparkRuntimeException
   *   when value is null.
   */
  def getLong(i: Int): Long = getAnyValAs[Long](i)

  /**
   * Returns the value at position i as a primitive float. Throws an exception if the type
   * mismatches or if the value is null.
   *
   * @throws ClassCastException
   *   when data type does not match.
   * @throws org.apache.spark.SparkRuntimeException
   *   when value is null.
   */
  def getFloat(i: Int): Float = getAnyValAs[Float](i)

  /**
   * Returns the value at position i as a primitive double.
   *
   * @throws ClassCastException
   *   when data type does not match.
   * @throws org.apache.spark.SparkRuntimeException
   *   when value is null.
   */
  def getDouble(i: Int): Double = getAnyValAs[Double](i)

  /**
   * Returns the value at position i as a String object.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getString(i: Int): String = getAs[String](i)

  /**
   * Returns the value at position i of decimal type as java.math.BigDecimal.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getDecimal(i: Int): java.math.BigDecimal = getAs[java.math.BigDecimal](i)

  /**
   * Returns the value at position i of date type as java.sql.Date.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getDate(i: Int): java.sql.Date = getAs[java.sql.Date](i)

  /**
   * Returns the value at position i of date type as java.time.LocalDate.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getLocalDate(i: Int): java.time.LocalDate = getAs[java.time.LocalDate](i)

  /**
   * Returns the value at position i of date type as java.sql.Timestamp.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getTimestamp(i: Int): java.sql.Timestamp = getAs[java.sql.Timestamp](i)

  /**
   * Returns the value at position i of date type as java.time.Instant.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getInstant(i: Int): java.time.Instant = getAs[java.time.Instant](i)

  /**
   * Returns the value at position i of array type as a Scala Seq.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getSeq[T](i: Int): Seq[T] = {
    getAs[scala.collection.Seq[T]](i) match {
      // SPARK-49178: When the type of `Seq[T]` is `mutable.ArraySeq[T]`,
      // rewrap `mutable.ArraySeq[T].array` as `immutable.ArraySeq[T]`
      // to avoid a collection copy.
      case seq: mutable.ArraySeq[T] =>
        seq.array.toImmutableArraySeq.asInstanceOf[Seq[T]]
      case other if other != null => other.toSeq
      case _ => null
    }
  }

  /**
   * Returns the value at position i of array type as `java.util.List`.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getList[T](i: Int): java.util.List[T] =
    getSeq[T](i).asJava

  /**
   * Returns the value at position i of map type as a Scala Map.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getMap[K, V](i: Int): scala.collection.Map[K, V] = getAs[Map[K, V]](i)

  /**
   * Returns the value at position i of array type as a `java.util.Map`.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getJavaMap[K, V](i: Int): java.util.Map[K, V] =
    getMap[K, V](i).asJava

  /**
   * Returns the value at position i of struct type as a [[Row]] object.
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getStruct(i: Int): Row = getAs[Row](i)

  /**
   * Returns the value at position i. For primitive types if value is null it returns 'zero value'
   * specific for primitive i.e. 0 for Int - use isNullAt to ensure that value is not null
   *
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getAs[T](i: Int): T = get(i).asInstanceOf[T]

  /**
   * Returns the value of a given fieldName. For primitive types if value is null it returns 'zero
   * value' specific for primitive i.e. 0 for Int - use isNullAt to ensure that value is not null
   *
   * @throws UnsupportedOperationException
   *   when schema is not defined.
   * @throws IllegalArgumentException
   *   when fieldName do not exist.
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getAs[T](fieldName: String): T = getAs[T](fieldIndex(fieldName))

  /**
   * Returns the index of a given field name.
   *
   * @throws UnsupportedOperationException
   *   when schema is not defined.
   * @throws IllegalArgumentException
   *   when a field `name` does not exist.
   */
  def fieldIndex(name: String): Int = {
    throw DataTypeErrors.fieldIndexOnRowWithoutSchemaError(fieldName = name)
  }

  /**
   * Returns a Map consisting of names and values for the requested fieldNames For primitive types
   * if value is null it returns 'zero value' specific for primitive i.e. 0 for Int - use isNullAt
   * to ensure that value is not null
   *
   * @throws UnsupportedOperationException
   *   when schema is not defined.
   * @throws IllegalArgumentException
   *   when fieldName do not exist.
   * @throws ClassCastException
   *   when data type does not match.
   */
  def getValuesMap[T](fieldNames: Seq[String]): Map[String, T] = {
    fieldNames.map { name =>
      name -> getAs[T](name)
    }.toMap
  }

  override def toString: String = this.mkString("[", ",", "]")

  /**
   * Make a copy of the current [[Row]] object.
   */
  def copy(): Row

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean = {
    val len = length
    var i = 0
    while (i < len) {
      if (isNullAt(i)) { return true }
      i += 1
    }
    false
  }

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[Row]) return false
    val other = o.asInstanceOf[Row]

    if (other eq null) return false

    if (length != other.length) {
      return false
    }

    var i = 0
    while (i < length) {
      if (isNullAt(i) != other.isNullAt(i)) {
        return false
      }
      if (!isNullAt(i)) {
        val o1 = get(i)
        val o2 = other.get(i)
        o1 match {
          case b1: Array[Byte] =>
            if (!o2.isInstanceOf[Array[Byte]] ||
              !java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
              return false
            }
          case f1: Float if java.lang.Float.isNaN(f1) =>
            if (!o2.isInstanceOf[Float] || !java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
              return false
            }
          case d1: Double if java.lang.Double.isNaN(d1) =>
            if (!o2.isInstanceOf[Double] || !java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
              return false
            }
          case d1: java.math.BigDecimal if o2.isInstanceOf[java.math.BigDecimal] =>
            if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) {
              return false
            }
          case _ =>
            if (o1 != o2) {
              return false
            }
        }
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    // Using Scala's Seq hash code implementation.
    var n = 0
    var h = MurmurHash3.seqSeed
    val len = length
    while (n < len) {
      h = MurmurHash3.mix(h, apply(n).##)
      n += 1
    }
    MurmurHash3.finalizeHash(h, n)
  }

  /* ---------------------- utility methods for Scala ---------------------- */

  /**
   * Return a Scala Seq representing the row. Elements are placed in the same order in the Seq.
   */
  def toSeq: Seq[Any] = {
    val n = length
    val values = new Array[Any](n)
    var i = 0
    while (i < n) {
      values.update(i, get(i))
      i += 1
    }
    values.toImmutableArraySeq
  }

  /** Displays all elements of this sequence in a string (without a separator). */
  def mkString: String = mkString("")

  /** Displays all elements of this sequence in a string using a separator string. */
  def mkString(sep: String): String = mkString("", sep, "")

  /**
   * Displays all elements of this traversable or iterator in a string using start, end, and
   * separator strings.
   */
  def mkString(start: String, sep: String, end: String): String = {
    val n = length
    val builder = new StringBuilder
    builder.append(start)
    if (n > 0) {
      builder.append(get(0))
      var i = 1
      while (i < n) {
        builder.append(sep)
        builder.append(get(i))
        i += 1
      }
    }
    builder.append(end)
    builder.toString()
  }

  /**
   * Returns the value at position i.
   *
   * @throws UnsupportedOperationException
   *   when schema is not defined.
   * @throws ClassCastException
   *   when data type does not match.
   * @throws org.apache.spark.SparkRuntimeException
   *   when value is null.
   */
  private def getAnyValAs[T <: AnyVal](i: Int): T =
    if (isNullAt(i)) throw DataTypeErrors.valueIsNullError(i)
    else getAs[T](i)

  /**
   * The compact JSON representation of this row.
   * @since 3.0
   */
  @Unstable
  def json: String = compact(jsonValue)

  /**
   * The pretty (i.e. indented) JSON representation of this row.
   * @since 3.0
   */
  @Unstable
  def prettyJson: String = pretty(render(jsonValue))

  /**
   * JSON representation of the row.
   *
   * Note that this only supports the data types that are also supported by
   * [[org.apache.spark.sql.catalyst.encoders.RowEncoder]].
   *
   * @return
   *   the JSON representation of the row.
   */
  private[sql] def jsonValue: JValue = {
    require(schema != null, "JSON serialization requires a non-null schema.")

    lazy val zoneId = SparkDateTimeUtils.getZoneId(SqlApiConf.get.sessionLocalTimeZone)
    lazy val dateFormatter = DateFormatter()
    lazy val timestampFormatter = TimestampFormatter(zoneId)

    // Convert an iterator of values to a json array
    def iteratorToJsonArray(iterator: Iterator[_], elementType: DataType): JArray = {
      JArray(iterator.map(toJson(_, elementType)).toList)
    }

    // Convert a value to json.
    def toJson(value: Any, dataType: DataType): JValue = (value, dataType) match {
      case (null, _) => JNull
      case (b: Boolean, _) => JBool(b)
      case (b: Byte, _) => JLong(b)
      case (s: Short, _) => JLong(s)
      case (i: Int, _) => JLong(i)
      case (l: Long, _) => JLong(l)
      case (f: Float, _) => JDouble(f)
      case (d: Double, _) => JDouble(d)
      case (d: BigDecimal, _) => JDecimal(d)
      case (d: java.math.BigDecimal, _) => JDecimal(d)
      case (d: Decimal, _) => JDecimal(d.toBigDecimal)
      case (s: String, _) => JString(s)
      case (b: Array[Byte], BinaryType) =>
        JString(Base64.getEncoder.encodeToString(b))
      case (d: LocalDate, _) => JString(dateFormatter.format(d))
      case (d: Date, _) => JString(dateFormatter.format(d))
      case (i: Instant, _) => JString(timestampFormatter.format(i))
      case (t: Timestamp, _) => JString(timestampFormatter.format(t))
      case (i: CalendarInterval, _) => JString(i.toString)
      case (a: Array[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(a.iterator, elementType)
      case (a: mutable.ArraySeq[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(a.iterator, elementType)
      case (s: Seq[_], ArrayType(elementType, _)) =>
        iteratorToJsonArray(s.iterator, elementType)
      case (m: Map[String @unchecked, _], MapType(StringType, valueType, _)) =>
        new JObject(m.toList.sortBy(_._1).map { case (k, v) =>
          k -> toJson(v, valueType)
        })
      case (m: Map[_, _], MapType(keyType, valueType, _)) =>
        new JArray(m.iterator.map { case (k, v) =>
          new JObject("key" -> toJson(k, keyType) :: "value" -> toJson(v, valueType) :: Nil)
        }.toList)
      case (row: Row, schema: StructType) =>
        var n = 0
        val elements = new mutable.ListBuffer[JField]
        val len = row.length
        while (n < len) {
          val field = schema(n)
          elements += (field.name -> toJson(row(n), field.dataType))
          n += 1
        }
        new JObject(elements.toList)
      case (v: Any, udt: UserDefinedType[Any @unchecked]) =>
        toJson(UDTUtils.toRow(v, udt), udt.sqlType)
      case _ =>
        throw new SparkIllegalArgumentException(
          errorClass = "FAILED_ROW_TO_JSON",
          messageParameters = Map(
            "value" -> toSQLValue(value.toString),
            "class" -> value.getClass.toString,
            "sqlType" -> toSQLType(dataType.toString)))
    }
    toJson(this, schema)
  }
}

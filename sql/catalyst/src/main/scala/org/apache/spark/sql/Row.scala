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

import org.apache.spark.sql.catalyst.expressions.GenericRow


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
  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row)

  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(values: Any*): Row = new GenericRow(values.toArray)

  /**
   * This method can be used to construct a [[Row]] from a [[Seq]] of values.
   */
  def fromSeq(values: Seq[Any]): Row = new GenericRow(values.toArray)
}


/**
 * Represents one row of output from a relational operator.  Allows both generic access by ordinal,
 * which will incur boxing overhead for primitives, as well as native primitive access.
 *
 * It is invalid to use the native primitive interface to retrieve a value that is null, instead a
 * user must check `isNullAt` before attempting to retrieve a value that might be null.
 *
 * To create a new Row, use [[RowFactory.create()]] in Java or [[Row.apply()]] in Scala.
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
 * A value of a row can be accessed through both generic access by ordinal,
 * which will incur boxing overhead for primitives, as well as native primitive access.
 * An example of generic access by ordinal:
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
 * For native primitive access, it is invalid to use the native primitive interface to retrieve
 * a value that is null, instead a user must check `isNullAt` before attempting to retrieve a
 * value that might be null.
 * An example of native primitive access:
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
 * @group row
 */
trait Row extends Seq[Any] with Serializable {
  /**
   * Returns the value at position i. If the value is null, null is returned. The following
   * is a mapping between Spark SQL types and return types:
   *
   * {{{
   *   BooleanType -> java.lang.Boolean
   *   ByteType -> java.lang.Byte
   *   ShortType -> java.lang.Short
   *   IntegerType -> java.lang.Integer
   *   FloatType -> java.lang.Float
   *   DoubleType -> java.lang.Double
   *   StringType -> String
   *   DecimalType -> scala.math.BigDecimal
   *
   *   DateType -> java.sql.Date
   *   TimestampType -> java.sql.Timestamp
   *
   *   BinaryType -> byte array
   *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
   *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
   *   StructType -> org.apache.spark.sql.Row
   * }}}
   */
  def apply(i: Int): Any

  /**
   * Returns the value at position i. If the value is null, null is returned. The following
   * is a mapping between Spark SQL types and return types:
   *
   * {{{
   *   BooleanType -> java.lang.Boolean
   *   ByteType -> java.lang.Byte
   *   ShortType -> java.lang.Short
   *   IntegerType -> java.lang.Integer
   *   FloatType -> java.lang.Float
   *   DoubleType -> java.lang.Double
   *   StringType -> String
   *   DecimalType -> scala.math.BigDecimal
   *
   *   DateType -> java.sql.Date
   *   TimestampType -> java.sql.Timestamp
   *
   *   BinaryType -> byte array
   *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
   *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
   *   StructType -> org.apache.spark.sql.Row
   * }}}
   */
  def get(i: Int): Any = apply(i)

  /** Checks whether the value at position i is null. */
  def isNullAt(i: Int): Boolean

  /**
   * Returns the value at position i as a primitive boolean.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getBoolean(i: Int): Boolean

  /**
   * Returns the value at position i as a primitive byte.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getByte(i: Int): Byte

  /**
   * Returns the value at position i as a primitive short.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getShort(i: Int): Short

  /**
   * Returns the value at position i as a primitive int.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getInt(i: Int): Int

  /**
   * Returns the value at position i as a primitive long.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getLong(i: Int): Long

  /**
   * Returns the value at position i as a primitive float.
   * Throws an exception if the type mismatches or if the value is null.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getFloat(i: Int): Float

  /**
   * Returns the value at position i as a primitive double.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getDouble(i: Int): Double

  /**
   * Returns the value at position i as a String object.
   *
   * @throws ClassCastException when data type does not match.
   * @throws NullPointerException when value is null.
   */
  def getString(i: Int): String

  /**
   * Returns the value at position i of decimal type as java.math.BigDecimal.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getDecimal(i: Int): BigDecimal = apply(i).asInstanceOf[BigDecimal]

  /**
   * Returns the value at position i of date type as java.sql.Date.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getDate(i: Int): java.sql.Date = apply(i).asInstanceOf[java.sql.Date]

  /**
   * Returns the value at position i of array type as a Scala Seq.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getSeq[T](i: Int): Seq[T] = apply(i).asInstanceOf[Seq[T]]

  /**
   * Returns the value at position i of array type as [[java.util.List]].
   *
   * @throws ClassCastException when data type does not match.
   */
  def getList[T](i: Int): java.util.List[T] = {
    scala.collection.JavaConversions.seqAsJavaList(getSeq[T](i))
  }

  /**
   * Returns the value at position i of map type as a Scala Map.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getMap[K, V](i: Int): scala.collection.Map[K, V] = apply(i).asInstanceOf[Map[K, V]]

  /**
   * Returns the value at position i of array type as a [[java.util.Map]].
   *
   * @throws ClassCastException when data type does not match.
   */
  def getJavaMap[K, V](i: Int): java.util.Map[K, V] = {
    scala.collection.JavaConversions.mapAsJavaMap(getMap[K, V](i))
  }

  /**
   * Returns the value at position i of struct type as an [[Row]] object.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getStruct(i: Int): Row = getAs[Row](i)

  /**
   * Returns the value at position i.
   *
   * @throws ClassCastException when data type does not match.
   */
  def getAs[T](i: Int): T = apply(i).asInstanceOf[T]

  override def toString(): String = s"[${this.mkString(",")}]"

  /**
   * Make a copy of the current [[Row]] object.
   */
  def copy(): Row

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean = {
    val l = length
    var i = 0
    while (i < l) {
      if (isNullAt(i)) { return true }
      i += 1
    }
    false
  }
}

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

package org.apache.spark.sql.api.java

import scala.annotation.varargs
import scala.collection.convert.Wrappers.{JListWrapper, JMapWrapper}
import scala.collection.JavaConversions
import scala.math.BigDecimal

import org.apache.spark.api.java.JavaUtils.mapAsSerializableJavaMap
import org.apache.spark.sql.catalyst.expressions.{Row => ScalaRow}

/**
 * A result row from a Spark SQL query.
 */
class Row(private[spark] val row: ScalaRow) extends Serializable {

  /** Returns the number of columns present in this Row. */
  def length: Int = row.length

  /** Returns the value of column `i`. */
  def get(i: Int): Any =
    Row.toJavaValue(row(i))

  /** Returns true if value at column `i` is NULL. */
  def isNullAt(i: Int) = get(i) == null

  /**
   * Returns the value of column `i` as an int.  This function will throw an exception if the value
   * is at `i` is not an integer, or if it is null.
   */
  def getInt(i: Int): Int =
    row.getInt(i)

  /**
   * Returns the value of column `i` as a long.  This function will throw an exception if the value
   * is at `i` is not a long, or if it is null.
   */
  def getLong(i: Int): Long =
    row.getLong(i)

  /**
   * Returns the value of column `i` as a double.  This function will throw an exception if the
   * value is at `i` is not a double, or if it is null.
   */
  def getDouble(i: Int): Double =
    row.getDouble(i)

  /**
   * Returns the value of column `i` as a bool.  This function will throw an exception if the value
   * is at `i` is not a boolean, or if it is null.
   */
  def getBoolean(i: Int): Boolean =
    row.getBoolean(i)

  /**
   * Returns the value of column `i` as a short.  This function will throw an exception if the value
   * is at `i` is not a short, or if it is null.
   */
  def getShort(i: Int): Short =
    row.getShort(i)

  /**
   * Returns the value of column `i` as a byte.  This function will throw an exception if the value
   * is at `i` is not a byte, or if it is null.
   */
  def getByte(i: Int): Byte =
    row.getByte(i)

  /**
   * Returns the value of column `i` as a float.  This function will throw an exception if the value
   * is at `i` is not a float, or if it is null.
   */
  def getFloat(i: Int): Float =
    row.getFloat(i)

  /**
   * Returns the value of column `i` as a String.  This function will throw an exception if the
   * value is at `i` is not a String.
   */
  def getString(i: Int): String =
    row.getString(i)

  def canEqual(other: Any): Boolean = other.isInstanceOf[Row]

  override def equals(other: Any): Boolean = other match {
    case that: Row =>
      (that canEqual this) &&
        row == that.row
    case _ => false
  }

  override def hashCode(): Int = row.hashCode()
}

object Row {

  private def toJavaValue(value: Any): Any = value match {
    // For values of this ScalaRow, we will do the conversion when
    // they are actually accessed.
    case row: ScalaRow => new Row(row)
    case map: scala.collection.Map[_, _] =>
      mapAsSerializableJavaMap(
        map.map {
          case (key, value) => (toJavaValue(key), toJavaValue(value))
        }
      )
    case seq: scala.collection.Seq[_] =>
      JavaConversions.seqAsJavaList(seq.map(toJavaValue))
    case decimal: BigDecimal => decimal.underlying()
    case other => other
  }

  // TODO: Consolidate the toScalaValue at here with the scalafy in JsonRDD?
  private def toScalaValue(value: Any): Any = value match {
    // Values of this row have been converted to Scala values.
    case row: Row => row.row
    case map: java.util.Map[_, _] =>
      JMapWrapper(map).map {
        case (key, value) => (toScalaValue(key), toScalaValue(value))
      }
    case list: java.util.List[_] =>
      JListWrapper(list).map(toScalaValue)
    case decimal: java.math.BigDecimal => BigDecimal(decimal)
    case other => other
  }

  /**
   * Creates a Row with the given values.
   */
  @varargs def create(values: Any*): Row = {
    // Right now, we cannot use @varargs to annotate the constructor of
    // org.apache.spark.sql.api.java.Row. See https://issues.scala-lang.org/browse/SI-8383.
    new Row(ScalaRow(values.map(toScalaValue):_*))
  }
}

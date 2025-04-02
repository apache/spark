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

package org.apache.spark.sql.util

import scala.reflect._

import org.apache.spark.annotation.Private
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}
import org.apache.spark.util.collection.OpenHashSet

// A wrap of OpenHashSet that can handle null, Double.NaN and Float.NaN w.r.t. the SQL semantic.
@Private
class SQLOpenHashSet[@specialized(Long, Int, Double, Float) T: ClassTag](
    initialCapacity: Int,
    loadFactor: Double) {

  def this(initialCapacity: Int) = this(initialCapacity, 0.7)

  def this() = this(64)

  private val hashSet = new OpenHashSet[T](initialCapacity, loadFactor)

  private var containNull = false
  private var containNaN = false

  def addNull(): Unit = {
    containNull = true
  }

  def addNaN(): Unit = {
    containNaN = true
  }

  def add(k: T): Unit = {
    hashSet.add(k)
  }

  def contains(k: T): Boolean = {
    hashSet.contains(k)
  }

  def containsNull(): Boolean = containNull

  def containsNaN(): Boolean = containNaN
}

object SQLOpenHashSet {
  def withNullCheckFunc(
      dataType: DataType,
      hashSet: SQLOpenHashSet[Any],
      handleNotNull: Any => Unit,
      handleNull: () => Unit): (ArrayData, Int) => Unit = {
    (array: ArrayData, index: Int) =>
      if (array.isNullAt(index)) {
        if (!hashSet.containsNull()) {
          hashSet.addNull()
          handleNull()
        }
      } else {
        val elem = array.get(index, dataType)
        handleNotNull(elem)
      }
  }

  def withNullCheckCode(
      array1ElementNullable: Boolean,
      array2ElementNullable: Boolean,
      array: String,
      index: String,
      hashSet: String,
      handleNotNull: (String, String) => String,
      handleNull: String): String = {
    if (array1ElementNullable) {
      if (array2ElementNullable) {
        s"""
           |if ($array.isNullAt($index)) {
           |  if (!$hashSet.containsNull()) {
           |    $hashSet.addNull();
           |    $handleNull
           |  }
           |} else {
           |  ${handleNotNull(array, index)}
           |}
         """.stripMargin
      } else {
        s"""
           |if (!$array.isNullAt($index)) {
           | ${handleNotNull(array, index)}
           |}
         """.stripMargin
      }
    } else {
      handleNotNull(array, index)
    }
  }

  def withNaNCheckFunc(
      dataType: DataType,
      hashSet: SQLOpenHashSet[Any],
      handleNotNaN: Any => Unit,
      handleNaN: Any => Unit): Any => Unit = {
    val (isNaN, valueNaN) = dataType match {
      case DoubleType =>
        ((value: Any) => java.lang.Double.isNaN(value.asInstanceOf[java.lang.Double]),
          java.lang.Double.NaN)
      case FloatType =>
        ((value: Any) => java.lang.Float.isNaN(value.asInstanceOf[java.lang.Float]),
          java.lang.Float.NaN)
      case _ => ((_: Any) => false, null)
    }
    (value: Any) =>
      if (isNaN(value)) {
        if (!hashSet.containsNaN()) {
          hashSet.addNaN()
          handleNaN(valueNaN)
        }
      } else {
        handleNotNaN(value)
      }
  }

  def withNaNCheckCode(
      dataType: DataType,
      valueName: String,
      hashSet: String,
      handleNotNaN: String,
      handleNaN: String => String): String = {
    val ret = dataType match {
      case DoubleType =>
        Some((s"java.lang.Double.isNaN((double)$valueName)", "java.lang.Double.NaN"))
      case FloatType =>
        Some((s"java.lang.Float.isNaN((float)$valueName)", "java.lang.Float.NaN"))
      case _ => None
    }
    ret.map { case (isNaN, valueNaN) =>
      s"""
         |if ($isNaN) {
         |  if (!$hashSet.containsNaN()) {
         |     $hashSet.addNaN();
         |     ${handleNaN(valueNaN)}
         |  }
         |} else {
         |  $handleNotNaN
         |}
       """.stripMargin
    }.getOrElse(handleNotNaN)
  }
}

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
  def isNaN(dataType: DataType): Any => Boolean = {
    dataType match {
      case DoubleType =>
        (value: Any) => java.lang.Double.isNaN(value.asInstanceOf[java.lang.Double])
      case FloatType =>
        (value: Any) => java.lang.Float.isNaN(value.asInstanceOf[java.lang.Float])
      case _ => (_: Any) => false
    }
  }

  def valueNaN(dataType: DataType): Any = {
    dataType match {
      case DoubleType => java.lang.Double.NaN
      case FloatType => java.lang.Float.NaN
      case _ => null
    }
  }
}

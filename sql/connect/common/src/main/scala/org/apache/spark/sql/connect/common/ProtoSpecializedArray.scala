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
package org.apache.spark.sql.connect.common

import org.apache.spark.connect.proto

private[spark] object ProtoSpecializedArray {

  def toArray(array: proto.Bools): Array[Boolean] = {
    val size = array.getValuesCount
    if (size > 0) {
      val a = Array.ofDim[Boolean](size)
      var i = 0
      while (i < size) {
        a(i) = array.getValues(i)
        i += 1
      }
      a
    } else {
      Array.emptyBooleanArray
    }
  }

  def toArray(array: proto.Ints): Array[Int] = {
    val size = array.getValuesCount
    if (size > 0) {
      val a = Array.ofDim[Int](size)
      var i = 0
      while (i < size) {
        a(i) = array.getValues(i)
        i += 1
      }
      a
    } else {
      Array.emptyIntArray
    }
  }

  def toArray(array: proto.Longs): Array[Long] = {
    val size = array.getValuesCount
    if (size > 0) {
      val a = Array.ofDim[Long](size)
      var i = 0
      while (i < size) {
        a(i) = array.getValues(i)
        i += 1
      }
      a
    } else {
      Array.emptyLongArray
    }
  }

  def toArray(array: proto.Floats): Array[Float] = {
    val size = array.getValuesCount
    if (size > 0) {
      val a = Array.ofDim[Float](size)
      var i = 0
      while (i < size) {
        a(i) = array.getValues(i)
        i += 1
      }
      a
    } else {
      Array.emptyFloatArray
    }
  }

  def toArray(array: proto.Doubles): Array[Double] = {
    val size = array.getValuesCount
    if (size > 0) {
      val a = Array.ofDim[Double](size)
      var i = 0
      while (i < size) {
        a(i) = array.getValues(i)
        i += 1
      }
      a
    } else {
      Array.emptyDoubleArray
    }
  }

  def toArray(array: proto.Strings): Array[String] = {
    val size = array.getValuesCount
    if (size > 0) {
      val a = Array.ofDim[String](size)
      var i = 0
      while (i < size) {
        a(i) = array.getValues(i)
        i += 1
      }
      a
    } else {
      Array.empty[String]
    }
  }

  def fromArray(array: Array[Boolean]): proto.Bools = {
    if (array.nonEmpty) {
      val builder = proto.Bools.newBuilder()
      array.foreach(builder.addValues)
      builder.build()
    } else {
      proto.Bools.getDefaultInstance
    }
  }

  def fromArray(array: Array[Int]): proto.Ints = {
    if (array.nonEmpty) {
      val builder = proto.Ints.newBuilder()
      array.foreach(builder.addValues)
      builder.build()
    } else {
      proto.Ints.getDefaultInstance
    }
  }

  def fromArray(array: Array[Long]): proto.Longs = {
    if (array.nonEmpty) {
      val builder = proto.Longs.newBuilder()
      array.foreach(builder.addValues)
      builder.build()
    } else {
      proto.Longs.getDefaultInstance
    }
  }

  def fromArray(array: Array[Float]): proto.Floats = {
    if (array.nonEmpty) {
      val builder = proto.Floats.newBuilder()
      array.foreach(builder.addValues)
      builder.build()
    } else {
      proto.Floats.getDefaultInstance
    }
  }

  def fromArray(array: Array[Double]): proto.Doubles = {
    if (array.nonEmpty) {
      val builder = proto.Doubles.newBuilder()
      array.foreach(builder.addValues)
      builder.build()
    } else {
      proto.Doubles.getDefaultInstance
    }
  }

  def fromArray(array: Array[String]): proto.Strings = {
    if (array.nonEmpty) {
      val builder = proto.Strings.newBuilder()
      array.foreach(builder.addValues)
      builder.build()
    } else {
      proto.Strings.getDefaultInstance
    }
  }
}

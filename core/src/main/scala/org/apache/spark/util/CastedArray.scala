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

package org.apache.spark.util

/**
 * Provides a wrapper around an object that is known to be an array, but the specific
 * type for the array is unknown.
 *
 * Normally, in situations when such an array is to be accessed reflectively, one would use
 * {@link java.lang.reflect.Array} using getLength() and get() methods. However, it turns
 * out that such methods are ill-performant.
 *
 * It turns out it is better to just use instanceOf and lots of casting over calling through
 * to the native C implementation. There is some discussion and a sample code snippet in
 * <a href=https://bugs.openjdk.java.net/browse/JDK-8051447>an open JDK ticket</a>. In this
 * class, that approach is implemented in an alternative way: creating a wrapper object to
 * wrap the array allows the cast to be done once, so the overhead of casting multiple times
 * is also avoided. It turns out we invoke the get() method to get the value of the array
 * numerous times, so doing the cast just once is worth the cost of constructing the wrapper
 * object for larger arrays.
 */
sealed trait CastedArray extends Any {
  def get(i: Int): AnyRef
  def getLength(): Int
}

object CastedArray {
  def castAndWrap(arr: AnyRef): CastedArray = {
    if (arr.isInstanceOf[Array[Boolean]]) {
      return new BooleanCastedArray(arr.asInstanceOf[Array[Boolean]])
    } else if (arr.isInstanceOf[Array[Byte]]) {
      return new ByteCastedArray(arr.asInstanceOf[Array[Byte]])
    } else if (arr.isInstanceOf[Array[Char]]) {
      return new CharCastedArray(arr.asInstanceOf[Array[Char]])
    } else if (arr.isInstanceOf[Array[Double]]) {
      return new DoubleCastedArray(arr.asInstanceOf[Array[Double]])
    } else if (arr.isInstanceOf[Array[Float]]) {
      return new FloatCastedArray(arr.asInstanceOf[Array[Float]])
    } else if (arr.isInstanceOf[Array[Int]]) {
      return new IntCastedArray(arr.asInstanceOf[Array[Int]])
    } else if (arr.isInstanceOf[Array[Long]]) {
      return new LongCastedArray(arr.asInstanceOf[Array[Long]])
    } else if (arr.isInstanceOf[Array[Object]]) {
      return new ObjectCastedArray(arr.asInstanceOf[Array[Object]])
    } else if (arr.isInstanceOf[Array[Short]]) {
      return new ShortCastedArray(arr.asInstanceOf[Array[Short]])
    } else {
      throw createBadArrayException(arr)
    }
  }

  // Boxing is not ideal, but we want to return AnyRef here. An alternative implementation
  // that used Java wouldn't force explicitly boxing... but returning Object there would
  // make the boxing happen implicitly anyways. In practice this tends to be okay
  // in terms of performance.
  private class BooleanCastedArray(val arr: Array[Boolean]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Boolean.box(arr(i))
    override def getLength(): Int = arr.length
  }

  private class ByteCastedArray(val arr: Array[Byte]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Byte.box(arr(i))
    override def getLength(): Int = arr.length
  }

  private class CharCastedArray(val arr: Array[Char]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Char.box(arr(i))
    override def getLength(): Int = arr.length
  }

  private class DoubleCastedArray(val arr: Array[Double]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Double.box(arr(i))
    override def getLength(): Int = arr.length
  }

  private class FloatCastedArray(val arr: Array[Float]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Float.box(arr(i))
    override def getLength(): Int = arr.length
  }

  private class IntCastedArray(val arr: Array[Int]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Int.box(arr(i))
    override def getLength(): Int = arr.length
  }

  private class LongCastedArray(val arr: Array[Long]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Long.box(arr(i))
    override def getLength(): Int = arr.length
  }

  private class ObjectCastedArray(val arr: Array[Object]) extends AnyVal with CastedArray {
    override def get(i: Int): Object = arr(i)
    override def getLength(): Int = arr.length
  }

  private class ShortCastedArray(val arr: Array[Short]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Short.box(arr(i))
    override def getLength(): Int = arr.length
  }

  private def createBadArrayException(badArray : Object): RuntimeException = {
    if (badArray == null) {
      return new NullPointerException("Array argument is null");
    } else if (!badArray.getClass().isArray()) {
      return new IllegalArgumentException("Argument is not an array");
    } else {
      return new IllegalArgumentException("Array is of incompatible type");
    }
  }
}


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
 *
 * In general, these classes were designed to avoid the need to cast as much as possible. As
 * soon as the type of the array is known, it is casted immediately once and all of its metadata
 * (primitive type size, length, and whether or not it is a primitive array) is available
 * immediately without any further reflection or introspecting on class objects.
 */
sealed trait CastedArray extends Any {
  def get(i: Int): AnyRef
  def getLength(): Int
  def isPrimitiveArray(): Boolean
  def getElementSize(): Int
}

object CastedArray {
  // Sizes of primitive types

  def castAndWrap(obj: AnyRef): CastedArray = {
    obj match {
      case arr: Array[Boolean] => new BooleanCastedArray(arr)
      case arr: Array[Byte] => new ByteCastedArray(arr)
      case arr: Array[Char] => new CharCastedArray(arr)
      case arr: Array[Double] => new DoubleCastedArray(arr)
      case arr: Array[Float] => new FloatCastedArray(arr)
      case arr: Array[Int] => new IntCastedArray(arr)
      case arr: Array[Long] => new LongCastedArray(arr)
      case arr: Array[Object] => new ObjectCastedArray(arr)
      case arr: Array[Short] => new ShortCastedArray(arr)
      case default => throw createBadArrayException(obj)
    }
  }

  // Boxing is not ideal, but we want to return AnyRef here. An alternative implementation
  // that used Java wouldn't force explicitly boxing... but returning Object there would
  // make the boxing happen implicitly anyways. In practice this tends to be okay
  // in terms of performance.
  private class BooleanCastedArray(val arr: Array[Boolean]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Boolean.box(arr(i))
    override def getLength(): Int = arr.length
    override def isPrimitiveArray(): Boolean = true
    override def getElementSize(): Int = PrimitiveSizes.BOOLEAN_SIZE
  }

  private class ByteCastedArray(val arr: Array[Byte]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Byte.box(arr(i))
    override def getLength(): Int = arr.length
    override def isPrimitiveArray(): Boolean = true
    override def getElementSize(): Int = PrimitiveSizes.BYTE_SIZE
  }

  private class CharCastedArray(val arr: Array[Char]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Char.box(arr(i))
    override def getLength(): Int = arr.length
    override def isPrimitiveArray(): Boolean = true
    override def getElementSize(): Int = PrimitiveSizes.CHAR_SIZE
  }

  private class DoubleCastedArray(val arr: Array[Double]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Double.box(arr(i))
    override def getLength(): Int = arr.length
    override def isPrimitiveArray(): Boolean = true
    override def getElementSize(): Int = PrimitiveSizes.DOUBLE_SIZE
  }

  private class FloatCastedArray(val arr: Array[Float]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Float.box(arr(i))
    override def getLength(): Int = arr.length
    override def isPrimitiveArray(): Boolean = true
    override def getElementSize(): Int = PrimitiveSizes.FLOAT_SIZE
  }

  private class IntCastedArray(val arr: Array[Int]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Int.box(arr(i))
    override def getLength(): Int = arr.length
    override def isPrimitiveArray(): Boolean = true
    override def getElementSize(): Int = PrimitiveSizes.INT_SIZE
  }

  private class LongCastedArray(val arr: Array[Long]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Long.box(arr(i))
    override def getLength(): Int = arr.length
    override def isPrimitiveArray(): Boolean = true
    override def getElementSize(): Int = PrimitiveSizes.LONG_SIZE
  }

  private class ObjectCastedArray(val arr: Array[Object]) extends AnyVal with CastedArray {
    override def get(i: Int): Object = arr(i)
    override def getLength(): Int = arr.length
    override def isPrimitiveArray(): Boolean = false
    override def getElementSize(): Int = {
      throw new UnsupportedOperationException("Cannot introspect " +
        " the size of an element in an object array.")
    }
  }

  private class ShortCastedArray(val arr: Array[Short]) extends AnyVal with CastedArray {
    override def get(i: Int): AnyRef = Short.box(arr(i))
    override def getLength(): Int = arr.length
    override def isPrimitiveArray(): Boolean = true
    override def getElementSize(): Int = PrimitiveSizes.SHORT_SIZE
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


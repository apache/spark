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

package org.apache.spark.sql.catalyst.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object GenericArrayData {
  def allocate(seq: Seq[Any]): GenericArrayData = new GenericArrayData(seq)
  def allocate(list: java.util.List[Any]): GenericArrayData = new GenericArrayData(list)
  def allocate(seqOrArray: Any): GenericArrayData = new GenericArrayData(seqOrArray)
  def allocate(primitiveArray: Array[Int]): GenericArrayData =
    new GenericIntArrayData(primitiveArray)
  def allocate(primitiveArray: Array[Long]): GenericArrayData =
    new GenericLongArrayData(primitiveArray)
  def allocate(primitiveArray: Array[Float]): GenericArrayData =
    new GenericFloatArrayData(primitiveArray)
  def allocate(primitiveArray: Array[Double]): GenericArrayData =
    new GenericDoubleArrayData(primitiveArray)
  def allocate(primitiveArray: Array[Short]): GenericArrayData =
    new GenericShortArrayData(primitiveArray)
  def allocate(primitiveArray: Array[Byte]): GenericArrayData =
    new GenericByteArrayData(primitiveArray)
  def allocate(primitiveArray: Array[Boolean]): GenericArrayData =
    new GenericBooleanArrayData(primitiveArray)
}

private object GenericArrayData {

  // SPARK-16634: Workaround for JVM bug present in some 1.7 versions.
  def anyToSeq(seqOrArray: Any): Seq[Any] = seqOrArray match {
    case seq: Seq[Any] => seq
    case array: Array[_] => array.toSeq
    case _ => Seq.empty
  }

}

class GenericArrayData(private val _array: Array[Any]) extends ArrayData {

  def this(seq: Seq[Any]) = this(seq.toArray)
  def this(list: java.util.List[Any]) = this(list.asScala)

  // TODO: This is boxing.  We should specialize.
  def this(primitiveArray: Array[Int]) = this(primitiveArray.toSeq)
  def this(primitiveArray: Array[Long]) = this(primitiveArray.toSeq)
  def this(primitiveArray: Array[Float]) = this(primitiveArray.toSeq)
  def this(primitiveArray: Array[Double]) = this(primitiveArray.toSeq)
  def this(primitiveArray: Array[Short]) = this(primitiveArray.toSeq)
  def this(primitiveArray: Array[Byte]) = this(primitiveArray.toSeq)
  def this(primitiveArray: Array[Boolean]) = this(primitiveArray.toSeq)

  def this(seqOrArray: Any) = this(GenericArrayData.anyToSeq(seqOrArray))

  override def array(): Array[Any] = _array

  override def copy(): ArrayData = new GenericArrayData(array.clone())

  override def numElements(): Int = array.length

  private def getAs[T](ordinal: Int) = _array(ordinal).asInstanceOf[T]
  override def isNullAt(ordinal: Int): Boolean = getAs[AnyRef](ordinal) eq null
  override def get(ordinal: Int, elementType: DataType): AnyRef = getAs(ordinal)
  override def getBoolean(ordinal: Int): Boolean = getAs(ordinal)
  override def getByte(ordinal: Int): Byte = getAs(ordinal)
  override def getShort(ordinal: Int): Short = getAs(ordinal)
  override def getInt(ordinal: Int): Int = getAs(ordinal)
  override def getLong(ordinal: Int): Long = getAs(ordinal)
  override def getFloat(ordinal: Int): Float = getAs(ordinal)
  override def getDouble(ordinal: Int): Double = getAs(ordinal)
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = getAs(ordinal)
  override def getUTF8String(ordinal: Int): UTF8String = getAs(ordinal)
  override def getBinary(ordinal: Int): Array[Byte] = getAs(ordinal)
  override def getInterval(ordinal: Int): CalendarInterval = getAs(ordinal)
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = getAs(ordinal)
  override def getArray(ordinal: Int): ArrayData = getAs(ordinal)
  override def getMap(ordinal: Int): MapData = getAs(ordinal)

  override def toString(): String = array.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GenericArrayData]) {
      return false
    }

    val other = o.asInstanceOf[GenericArrayData]
    if (other eq null) {
      return false
    }

    val len = numElements()
    if (len != other.numElements()) {
      return false
    }

    var i = 0
    while (i < len) {
      if (isNullAt(i) != other.isNullAt(i)) {
        return false
      }
      if (!isNullAt(i)) {
        val o1 = _array(i)
        val o2 = other._array(i)
        o1 match {
          case b1: Array[Byte] =>
            if (!o2.isInstanceOf[Array[Byte]] ||
              !java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
              return false
            }
          case f1: Float if java.lang.Float.isNaN(f1) =>
            if (!o2.isInstanceOf[Float] || ! java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
              return false
            }
          case d1: Double if java.lang.Double.isNaN(d1) =>
            if (!o2.isInstanceOf[Double] || ! java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
              return false
            }
          case _ => if (o1 != o2) {
            return false
          }
        }
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numElements()
    while (i < len) {
      val update: Int =
        if (isNullAt(i)) {
          0
        } else {
          _array(i) match {
            case b: Boolean => if (b) 0 else 1
            case b: Byte => b.toInt
            case s: Short => s.toInt
            case i: Int => i
            case l: Long => (l ^ (l >>> 32)).toInt
            case f: Float => java.lang.Float.floatToIntBits(f)
            case d: Double =>
              val b = java.lang.Double.doubleToLongBits(d)
              (b ^ (b >>> 32)).toInt
            case a: Array[Byte] => java.util.Arrays.hashCode(a)
            case other => other.hashCode()
          }
        }
      result = 37 * result + update
      i += 1
    }
    result
  }
}

final class GenericIntArrayData(private val primitiveArray: Array[Int]) extends GenericArrayData {
  override def array(): Array[Any] = primitiveArray.toArray

  override def copy(): ArrayData = new GenericIntArrayData(primitiveArray)

  override def numElements(): Int = primitiveArray.length

  override def isNullAt(ordinal: Int): Boolean = false
  override def getInt(ordinal: Int): Int = primitiveArray(ordinal)
  override def toIntArray(): Array[Int] = {
    val array = new Array[Int](numElements)
    System.arraycopy(primitiveArray, 0, array, 0, numElements)
    array
  }
  override def toString(): String = primitiveArray.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GenericIntArrayData]) {
      return false
    }

    val other = o.asInstanceOf[GenericIntArrayData]
    if (other eq null) {
      return false
    }

    val len = numElements()
    if (len != other.numElements()) {
      return false
    }

    var i = 0
    while (i < len) {
      val o1 = primitiveArray(i)
      val o2 = other.primitiveArray(i)
      if (o1 != o2) {
        return false
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numElements()
    while (i < len) {
      val update: Int = primitiveArray(i)
      result = 37 * result + update
      i += 1
    }
    result
  }
}

final class GenericLongArrayData(private val primitiveArray: Array[Long])
  extends GenericArrayData {
  override def array(): Array[Any] = primitiveArray.toArray

  override def copy(): ArrayData = new GenericLongArrayData(primitiveArray)

  override def numElements(): Int = primitiveArray.length

  override def isNullAt(ordinal: Int): Boolean = false
  override def getLong(ordinal: Int): Long = primitiveArray(ordinal)
  override def toLongArray(): Array[Long] = {
    val array = new Array[Long](numElements)
    System.arraycopy(primitiveArray, 0, array, 0, numElements)
    array
  }
  override def toString(): String = primitiveArray.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GenericLongArrayData]) {
      return false
    }

    val other = o.asInstanceOf[GenericLongArrayData]
    if (other eq null) {
      return false
    }

    val len = numElements()
    if (len != other.numElements()) {
      return false
    }

    var i = 0
    while (i < len) {
      val o1 = primitiveArray(i)
      val o2 = other.primitiveArray(i)
      if (o1 != o2) {
        return false
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numElements()
    while (i < len) {
      val l = primitiveArray(i)
      val update: Int = (l ^ (l >>> 32)).toInt
      result = 37 * result + update
      i += 1
    }
    result
  }
}

final class GenericFloatArrayData(private val primitiveArray: Array[Float])
  extends GenericArrayData {
  override def array(): Array[Any] = primitiveArray.toArray

  override def copy(): ArrayData = new GenericFloatArrayData(primitiveArray)

  override def numElements(): Int = primitiveArray.length

  override def isNullAt(ordinal: Int): Boolean = false
  override def getFloat(ordinal: Int): Float = primitiveArray(ordinal)
  override def toFloatArray(): Array[Float] = {
    val array = new Array[Float](numElements)
    System.arraycopy(primitiveArray, 0, array, 0, numElements)
    array
  }
  override def toString(): String = primitiveArray.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GenericFloatArrayData]) {
      return false
    }

    val other = o.asInstanceOf[GenericFloatArrayData]
    if (other eq null) {
      return false
    }

    val len = numElements()
    if (len != other.numElements()) {
      return false
    }

    var i = 0
    while (i < len) {
      val o1 = primitiveArray(i)
      val o2 = other.primitiveArray(i)
      if (java.lang.Float.isNaN(o1)) {
        if (!java.lang.Float.isNaN(o2)) {
          return false;
        }
      } else if (o1 != o2) {
        return false
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numElements()
    while (i < len) {
      val f = primitiveArray(i)
      val update: Int = java.lang.Float.floatToIntBits(f)
      result = 37 * result + update
      i += 1
    }
    result
  }
}

final class GenericDoubleArrayData(private val primitiveArray: Array[Double])
  extends GenericArrayData {
  override def array(): Array[Any] = primitiveArray.toArray

  override def copy(): ArrayData = new GenericDoubleArrayData(primitiveArray)

  override def numElements(): Int = primitiveArray.length

  override def isNullAt(ordinal: Int): Boolean = false
  override def getDouble(ordinal: Int): Double = primitiveArray(ordinal)
  override def toDoubleArray(): Array[Double] = {
    val array = new Array[Double](numElements)
    System.arraycopy(primitiveArray, 0, array, 0, numElements)
    array
  }
  override def toString(): String = primitiveArray.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GenericDoubleArrayData]) {
      return false
    }

    val other = o.asInstanceOf[GenericDoubleArrayData]
    if (other eq null) {
      return false
    }

    val len = numElements()
    if (len != other.numElements()) {
      return false
    }

    var i = 0
    while (i < len) {
      val o1 = primitiveArray(i)
      val o2 = other.primitiveArray(i)
      if (java.lang.Double.isNaN(o1)) {
        if (!java.lang.Double.isNaN(o2)) {
          return false;
        }
      } else if (o1 != o2) {
        return false
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numElements()
    while (i < len) {
      val d = primitiveArray(i)
      val b = java.lang.Double.doubleToLongBits(d)
      val update: Int = (b ^ (b >>> 32)).toInt
      result = 37 * result + update
      i += 1
    }
    result
  }
}

final class GenericShortArrayData(private val primitiveArray: Array[Short])
  extends GenericArrayData {
  override def array(): Array[Any] = primitiveArray.toArray

  override def copy(): ArrayData = new GenericShortArrayData(primitiveArray)

  override def numElements(): Int = primitiveArray.length

  override def isNullAt(ordinal: Int): Boolean = false
  override def getShort(ordinal: Int): Short = primitiveArray(ordinal)
  override def toShortArray(): Array[Short] = {
    val array = new Array[Short](numElements)
    System.arraycopy(primitiveArray, 0, array, 0, numElements)
    array
  }
  override def toString(): String = primitiveArray.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GenericShortArrayData]) {
      return false
    }

    val other = o.asInstanceOf[GenericShortArrayData]
    if (other eq null) {
      return false
    }

    val len = numElements()
    if (len != other.numElements()) {
      return false
    }

    var i = 0
    while (i < len) {
      val o1 = primitiveArray(i)
      val o2 = other.primitiveArray(i)
      if (o1 != o2) {
        return false
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numElements()
    while (i < len) {
      val update: Int = primitiveArray(i).toInt
      result = 37 * result + update
      i += 1
    }
    result
  }
}

final class GenericByteArrayData(private val primitiveArray: Array[Byte])
  extends GenericArrayData {
  override def array(): Array[Any] = primitiveArray.toArray

  override def copy(): ArrayData = new GenericByteArrayData(primitiveArray)

  override def numElements(): Int = primitiveArray.length

  override def isNullAt(ordinal: Int): Boolean = false
  override def getByte(ordinal: Int): Byte = primitiveArray(ordinal)
  override def toByteArray(): Array[Byte] = {
    val array = new Array[Byte](numElements)
    System.arraycopy(primitiveArray, 0, array, 0, numElements)
    array
  }
  override def toString(): String = primitiveArray.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GenericByteArrayData]) {
      return false
    }

    val other = o.asInstanceOf[GenericByteArrayData]
    if (other eq null) {
      return false
    }

    val len = numElements()
    if (len != other.numElements()) {
      return false
    }

    var i = 0
    while (i < len) {
      val o1 = primitiveArray(i)
      val o2 = other.primitiveArray(i)
      if (o1 != o2) {
        return false
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numElements()
    while (i < len) {
      val update: Int = primitiveArray(i).toInt
      result = 37 * result + update
      i += 1
    }
    result
  }
}

final class GenericBooleanArrayData(private val primitiveArray: Array[Boolean])
  extends GenericArrayData {
  override def array(): Array[Any] = primitiveArray.toArray

  override def copy(): ArrayData = new GenericBooleanArrayData(primitiveArray)

  override def numElements(): Int = primitiveArray.length

  override def isNullAt(ordinal: Int): Boolean = false
  override def getBoolean(ordinal: Int): Boolean = primitiveArray(ordinal)
  override def toBooleanArray(): Array[Boolean] = {
    val array = new Array[Boolean](numElements)
    System.arraycopy(primitiveArray, 0, array, 0, numElements)
    array
  }
  override def toString(): String = primitiveArray.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[GenericBooleanArrayData]) {
      return false
    }

    val other = o.asInstanceOf[GenericBooleanArrayData]
    if (other eq null) {
      return false
    }

    val len = numElements()
    if (len != other.numElements()) {
      return false
    }

    var i = 0
    while (i < len) {
      val o1 = primitiveArray(i)
      val o2 = other.primitiveArray(i)
      if (o1 != o2) {
        return false
      }
      i += 1
    }
    true
  }

  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numElements()
    while (i < len) {
      val update: Int = if (primitiveArray(i)) 1 else 0
      result = 37 * result + update
      i += 1
    }
    result
  }
}

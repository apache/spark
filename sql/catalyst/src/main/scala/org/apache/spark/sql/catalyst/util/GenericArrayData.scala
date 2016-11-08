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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

private object GenericArrayData {

  // SPARK-16634: Workaround for JVM bug present in some 1.7 versions.
  def anyToSeq(seqOrArray: Any): Seq[Any] = seqOrArray match {
    case seq: Seq[Any] => seq
    case array: Array[_] => array.toSeq
  }

}

class GenericArrayData(val array: Array[Any],
    val booleanArray: Array[Boolean], val byteArray: Array[Byte], val shortArray: Array[Short],
    val intArray: Array[Int], val longArray: Array[Long], val floatArray: Array[Float],
    val doubleArray: Array[Double]) extends ArrayData {

  def this(seq: Seq[Any]) = this(seq.toArray, null, null, null, null, null, null, null)
  def this(list: java.util.List[Any]) = this(list.asScala)

  // TODO: This is boxing.  We should specialize.
  def this(primitiveArray: Array[Boolean]) =
  this(null, primitiveArray, null, null, null, null, null, null)
  def this(primitiveArray: Array[Byte]) =
    this(null, null, primitiveArray, null, null, null, null, null)
  def this(primitiveArray: Array[Short]) =
    this(null, null, null, primitiveArray, null, null, null, null)
  def this(primitiveArray: Array[Int]) =
    this(null, null, null, null, primitiveArray, null, null, null)
  def this(primitiveArray: Array[Long]) =
    this(null, null, null, null, null, primitiveArray, null, null)
  def this(primitiveArray: Array[Float]) =
    this(null, null, null, null, null, null, primitiveArray, null)
  def this(primitiveArray: Array[Double]) =
    this(null, null, null, null, null, null, null, primitiveArray)

  def this(array: Array[Any]) = this(array, null, null, null, null, null, null, null)

  def this(seqOrArray: Any) = this(GenericArrayData.anyToSeq(seqOrArray))

  override def copy(): ArrayData = {
    if (booleanArray != null) new GenericArrayData(booleanArray.clone())
    else if (byteArray != null) new GenericArrayData(byteArray.clone())
    else if (shortArray != null) new GenericArrayData(shortArray.clone())
    else if (intArray != null) new GenericArrayData(intArray.clone())
    else if (longArray != null) new GenericArrayData(longArray.clone())
    else if (floatArray != null) new GenericArrayData(floatArray.clone())
    else if (doubleArray != null) new GenericArrayData(doubleArray.clone())
    else new GenericArrayData(array.clone())
  }

  override def numElements(): Int = {
    if (booleanArray != null) booleanArray.length
    else if (byteArray != null) byteArray.length
    else if (shortArray != null) shortArray.length
    else if (intArray != null) intArray.length
    else if (longArray != null) longArray.length
    else if (floatArray != null) floatArray.length
    else if (doubleArray != null) doubleArray.length
    else array.length
  }

  private def getAs[T](ordinal: Int) = array(ordinal).asInstanceOf[T]
  override def get(ordinal: Int, elementType: DataType): AnyRef = getAs(ordinal)
  override def getBoolean(ordinal: Int): Boolean =
    if (booleanArray != null) booleanArray(ordinal) else getAs(ordinal)
  override def getByte(ordinal: Int): Byte =
    if (byteArray != null) byteArray(ordinal) else getAs(ordinal)
  override def getShort(ordinal: Int): Short =
    if (shortArray != null) shortArray(ordinal) else getAs(ordinal)
  override def getInt(ordinal: Int): Int =
    if (intArray != null) intArray(ordinal) else getAs(ordinal)
  override def getLong(ordinal: Int): Long =
    if (longArray != null) longArray(ordinal) else getAs(ordinal)
  override def getFloat(ordinal: Int): Float =
    if (floatArray != null) floatArray(ordinal) else getAs(ordinal)
  override def getDouble(ordinal: Int): Double =
    if (doubleArray != null) doubleArray(ordinal) else getAs(ordinal)
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = getAs(ordinal)
  override def getUTF8String(ordinal: Int): UTF8String = getAs(ordinal)
  override def getBinary(ordinal: Int): Array[Byte] = getAs(ordinal)
  override def getInterval(ordinal: Int): CalendarInterval = getAs(ordinal)
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = getAs(ordinal)
  override def getArray(ordinal: Int): ArrayData = getAs(ordinal)
  override def getMap(ordinal: Int): MapData = getAs(ordinal)

  override def isNullAt(ordinal: Int): Boolean = {
    if (booleanArray != null || byteArray != null || shortArray != null || intArray != null ||
      longArray != null || floatArray != null || doubleArray != null) {
      false
    } else {
      getAs[AnyRef](ordinal) eq null
    }
  }

  override def toBooleanArray(): Array[Boolean] = {
    if (booleanArray != null) {
      val len = booleanArray.length
      val array = new Array[Boolean](len)
      System.arraycopy(booleanArray, 0, array, 0, len)
      array
    } else {
      super.toBooleanArray
    }
  }

  override def toByteArray(): Array[Byte] = {
    if (byteArray != null) {
      val len = byteArray.length
      val array = new Array[Byte](len)
      System.arraycopy(byteArray, 0, array, 0, len)
      array
    } else {
      super.toByteArray
    }
  }

  override def toShortArray(): Array[Short] = {
    if (shortArray != null) {
      val len = shortArray.length
      val array = new Array[Short](len)
      System.arraycopy(shortArray, 0, array, 0, len)
      array
    } else {
      super.toShortArray
    }
  }

  override def toIntArray(): Array[Int] = {
    if (intArray != null) {
      val len = intArray.length
      val array = new Array[Int](len)
      System.arraycopy(intArray, 0, array, 0, len)
      array
    } else {
      super.toIntArray
    }
  }

  override def toLongArray(): Array[Long] = {
    if (longArray != null) {
      val len = longArray.length
      val array = new Array[Long](len)
      System.arraycopy(longArray, 0, array, 0, len)
      array
    } else {
      super.toLongArray
    }
  }

  override def toFloatArray(): Array[Float] = {
    if (floatArray != null) {
      val len = floatArray.length
      val array = new Array[Float](len)
      System.arraycopy(floatArray, 0, array, 0, len)
      array
    } else {
      super.toFloatArray
    }
  }

  override def toDoubleArray(): Array[Double] = {
    if (doubleArray != null) {
      val len = doubleArray.length
      val array = new Array[Double](len)
      System.arraycopy(doubleArray, 0, array, 0, len)
      array
    } else {
      super.toDoubleArray
    }
  }

  override def toString(): String = {
    if (booleanArray != null) booleanArray.mkString("[", ",", "]")
    else if (byteArray != null) byteArray.mkString("[", ",", "]")
    else if (shortArray != null) shortArray.mkString("[", ",", "]")
    else if (intArray != null) intArray.mkString("[", ",", "]")
    else if (longArray != null) longArray.mkString("[", ",", "]")
    else if (floatArray != null) floatArray.mkString("[", ",", "]")
    else if (doubleArray != null) doubleArray.mkString("[", ",", "]")
    else array.mkString("[", ",", "]")
  }

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

    if ((booleanArray != null) && (other.booleanArray != null)) {
      return java.util.Arrays.equals(booleanArray, other.booleanArray)
    } else if ((byteArray != null) && (other.byteArray != null)) {
      return java.util.Arrays.equals(byteArray, other.byteArray)
    } else if ((shortArray != null) && (other.shortArray != null)) {
      return java.util.Arrays.equals(shortArray, other.shortArray)
    } else if ((intArray != null) && (other.intArray != null)) {
      return java.util.Arrays.equals(intArray, other.intArray)
    } else if ((longArray != null) && (other.longArray != null)) {
      return java.util.Arrays.equals(longArray, other.longArray)
    } else if ((floatArray != null) && (other.floatArray != null)) {
      return java.util.Arrays.equals(floatArray, other.floatArray)
    } else if ((doubleArray != null) && (other.doubleArray != null)) {
      return java.util.Arrays.equals(doubleArray, other.doubleArray)
    }

    if ((array == null) || (other.array == null)) {
      return false
    }

    var i = 0
    while (i < len) {
      if (isNullAt(i) != other.isNullAt(i)) {
        return false
      }
      if (!isNullAt(i)) {
        val o1 = array(i)
        val o2 = other.array(i)
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
    if (booleanArray != null) return java.util.Arrays.hashCode(booleanArray)
    else if (byteArray != null) return java.util.Arrays.hashCode(byteArray)
    else if (shortArray != null) return java.util.Arrays.hashCode(shortArray)
    else if (intArray != null) return java.util.Arrays.hashCode(intArray)
    else if (longArray != null) return java.util.Arrays.hashCode(longArray)
    else if (floatArray != null) return java.util.Arrays.hashCode(floatArray)
    else if (doubleArray != null) return java.util.Arrays.hashCode(doubleArray)

    var result: Int = 37
    var i = 0
    val len = numElements()
    while (i < len) {
      val update: Int =
        if (isNullAt(i)) {
          0
        } else {
          array(i) match {
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

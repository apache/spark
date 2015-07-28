package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.unsafe.types.{UTF8String, Interval}

abstract class ArrayData extends SpecializedGetters with Serializable {
  //todo: remove this after we handle all types.(map type need special getter)
  def get(ordinal: Int): Any

  def numElements(): Int

  def sizeInBytes(): Int

  // todo: need a more efficient way to iterate array type.
  def toArray(): Array[Any] = {
    val n = numElements()
    val values = new Array[Any](n)
    var i = 0
    while (i < n) {
      if (isNullAt(i)) {
        values(i) = null
      } else {
        values(i) = get(i)
      }
      i += 1
    }
    values
  }

  override def toString(): String = toArray.mkString("[", ",", "]")

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[ArrayData]) {
      return false
    }

    val other = o.asInstanceOf[ArrayData]
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
        val o1 = get(i)
        val o2 = other.get(i)
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
          get(i) match {
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

class GenericArrayData(array: Array[Any]) extends ArrayData {
  private def getAs[T](ordinal: Int) = get(ordinal).asInstanceOf[T]

  override def toArray(): Array[Any] = array

  override def get(ordinal: Int): Any = array(ordinal)

  override def isNullAt(ordinal: Int): Boolean = get(ordinal) == null

  override def getBoolean(ordinal: Int): Boolean = getAs(ordinal)

  override def getByte(ordinal: Int): Byte = getAs(ordinal)

  override def getShort(ordinal: Int): Short = getAs(ordinal)

  override def getInt(ordinal: Int): Int = getAs(ordinal)

  override def getLong(ordinal: Int): Long = getAs(ordinal)

  override def getFloat(ordinal: Int): Float = getAs(ordinal)

  override def getDouble(ordinal: Int): Double = getAs(ordinal)

  override def getDecimal(ordinal: Int): Decimal = getAs(ordinal)

  override def getUTF8String(ordinal: Int): UTF8String = getAs(ordinal)

  override def getBinary(ordinal: Int): Array[Byte] = getAs(ordinal)

  override def getInterval(ordinal: Int): Interval = getAs(ordinal)

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = getAs(ordinal)

  override def getArray(ordinal: Int, elementType: DataType): ArrayData = getAs(ordinal)

  override def numElements(): Int = array.length

  // todo: figure out how to implement this.
  override def sizeInBytes(): Int = ???
}

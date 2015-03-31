package org.apache.spark.sql.types

/**
 *  A mutable UTF-8 String
 */

final class UTF8String extends Ordered[UTF8String] with Serializable {
  private var bytes: Array[Byte] = _

  def set(str: String): UTF8String = {
    this.bytes = str.getBytes("utf-8")
    this
  }

  def set(bytes: Array[Byte]): UTF8String = {
    this.bytes = bytes.clone()
    this
  }

  def length(): Int = {
    var len = 0
    var i: Int = 0
    while (i < bytes.length) {
      val b = bytes(i)
      i += 1
      if (b >= 196) {
        i += UTF8String.bytesFromUTF8(b - 196)
      }
      len += 1
    }
    len
  }

  def getBytes: Array[Byte] = {
    bytes
  }

  def slice(start: Int, end: Int): UTF8String = {
    UTF8String(toString().slice(start, end))
  }

  def contains(sub: UTF8String): Boolean = {
    bytes.containsSlice(sub.bytes)
  }

  def startsWith(prefix: UTF8String): Boolean = {
    bytes.startsWith(prefix.bytes)
  }

  def endsWith(suffix: UTF8String): Boolean = {
    bytes.endsWith(suffix.bytes)
  }

  def toUpperCase(): UTF8String = {
    UTF8String(toString().toUpperCase)
  }

  def toLowerCase(): UTF8String = {
    UTF8String(toString().toLowerCase)
  }

  override def toString(): String = {
    new String(bytes, "utf-8")
  }

  override def clone(): UTF8String = new UTF8String().set(this.bytes)

  override def compare(other: UTF8String): Int = {
    var i: Int = 0
    while (i < bytes.length && i < other.bytes.length) {
      val res = bytes(i).compareTo(other.bytes(i))
      if (res != 0) return res
      i += 1
    }
    bytes.length - other.bytes.length
  }

  override def compareTo(other: UTF8String): Int = {
    compare(other)
  }

  override def equals(other: Any): Boolean = other match {
    case s: UTF8String =>
      bytes.length == s.bytes.length && compare(s) == 0
    case s: String =>
      toString() == s
    case _ =>
      false
  }

  override def hashCode(): Int = {
    var h: Int = 1
    var i: Int = 0
    while (i < bytes.length) {
      h = h * 31 + bytes(i)
      i += 1
    }
    h
  }
}

object UTF8String {
  val bytesFromUTF8: Array[Int] = Array(1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3,
    3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5)

  def apply(s: String): UTF8String  = new UTF8String().set(s)
  def apply(bytes: Array[Byte]): UTF8String = new UTF8String().set(bytes)
}
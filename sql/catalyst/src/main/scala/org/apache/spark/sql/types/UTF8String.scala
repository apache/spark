package org.apache.spark.sql.types

/**
 *  A mutable UTF-8 String
 */

final class UTF8String extends Ordered[UTF8String] with Serializable {
  private var s: String = _
  def set(str: String): UTF8String = {
    this.s = str
    this
  }

  def set(bytes: Array[Byte]): UTF8String = {
    this.s = new String(bytes, "utf-8")
    this
  }

  def set(a: UTF8String): UTF8String = {
    this.s = a.s
    this
  }

  def length(): Int = {
    this.s.length
  }

  def getBytes(): Array[Byte] = {
    this.s.getBytes("utf-8")
  }

  def getBytes(encoding: String): Array[Byte] = {
    this.s.getBytes(encoding)
  }

  def slice(start: Int, end: Int): UTF8String = {
    UTF8String(this.s.slice(start, end))
  }

  def contains(sub: UTF8String): Boolean = {
    this.s.contains(sub.s)
  }

  def startsWith(prefix: UTF8String): Boolean = {
    this.s.startsWith(prefix.s)
  }

  def endsWith(suffix: UTF8String): Boolean = {
    this.s.endsWith(suffix.s)
  }

  def toUpperCase(): UTF8String = {
    UTF8String(s.toUpperCase)
  }

  def toLowerCase(): UTF8String = {
    UTF8String(s.toLowerCase)
  }

  override def toString(): String = {
    this.s
  }

  override def clone(): UTF8String = new UTF8String().set(this)

  override def compare(other: UTF8String): Int = {
    this.s.compare(other.s)
  }

  def compare(other: String): Int = {
    this.s.compare(other)
  }

  override def compareTo(other: UTF8String): Int = {
    this.s.compareTo(other.s)
  }

  def compareTo(other: String): Int = {
    this.s.compareTo(other)
  }

  override def equals(other: Any): Boolean = other match {
    case s: UTF8String =>
      compare(s) == 0
    case s: String =>
      this.s.compare(s) == 0
    case _ =>
      false
  }

  override def hashCode(): Int = {
    this.s.hashCode
  }
}

object UTF8String {
  implicit def apply(s: String): UTF8String  = new UTF8String().set(s)
  implicit def toString(utf: UTF8String): String = utf.toString
  def apply(bytes: Array[Byte]): UTF8String = new UTF8String().set(bytes)
  def apply(utf8: UTF8String): UTF8String = utf8
  def apply(o: Any): UTF8String = o match {
    case null => null
    case utf8: UTF8String => utf8
    case s: String => new UTF8String().set(s)
    case bytes: Array[Byte]=> new UTF8String().set(bytes)
    case other => new UTF8String().set(other.toString)
  }
}
package org.apache.spark.sql.catalyst.expressions

object UnresolvedOrdering extends Ordering[Any] {
  override def compare(x: Any, y: Any): Int = sys.error(s"Type does not support ordered operations")
}

object UnresolvedNumeric extends Numeric[Any] {
  override def plus(x: Any, y: Any): Any = error

  override def toDouble(x: Any): Double = error

  override def toFloat(x: Any): Float = error

  override def toInt(x: Any): Int = error

  override def negate(x: Any): Any = error

  override def fromInt(x: Int): Any = error

  override def toLong(x: Any): Long = error

  override def times(x: Any, y: Any): Any = error

  override def minus(x: Any, y: Any): Any = error

  override def compare(x: Any, y: Any): Int = UnresolvedOrdering.compare(x, y)

  def div(x: Any, y: Any): Any = error

  private[this] def error = sys.error(s"Type does not support numeric operations")
}

object UnresolvedIntegral extends Integral[Any] {
  override def quot(x: Any, y: Any): Any = error

  override def rem(x: Any, y: Any): Any = error

  override def toDouble(x: Any): Double = error

  override def plus(x: Any, y: Any): Any = error

  override def toFloat(x: Any): Float = error

  override def toInt(x: Any): Int = error

  override def negate(x: Any): Any = error

  override def fromInt(x: Int): Any = error

  override def toLong(x: Any): Long = error

  override def times(x: Any, y: Any): Any = error

  override def minus(x: Any, y: Any): Any = error

  override def compare(x: Any, y: Any): Int = UnresolvedOrdering.compare(x, y)

  def bitwiseAnd(x: Any, y: Any): Any = ???
  def bitwiseOr(x: Any, y: Any): Any = ???
  def bitwiseXor(x: Any, y: Any): Any = ???
  def bitwiseNot(x: Any): Any = ???

  private[this] def error = sys.error(s"Type does not support numeric operations")
}
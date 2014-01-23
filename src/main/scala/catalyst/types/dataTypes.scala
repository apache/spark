package catalyst
package types

import expressions.Expression

abstract class DataType {
  /** Matches any expression that evaluates to this DataType */
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType == this => true
    case _ => false
  }
}

case object NullType extends DataType

abstract class NativeType extends DataType {
  type JvmType
  val ordering: Ordering[JvmType]
}

case object StringType extends NativeType {
  type JvmType = String
  val ordering = implicitly[Ordering[JvmType]]
}
case object BinaryType extends DataType {
  type JvmType = Array[Byte]
}
case object BooleanType extends NativeType {
  type JvmType = Boolean
  val ordering = implicitly[Ordering[JvmType]]
}

abstract class NumericType extends NativeType {
  // Unfortunately we can't get this implicitly as that breaks Spark Serialization.
  val numeric: Numeric[JvmType]
}

/** Matcher for any expressions that evaluate to [[IntegralType]]s */
object IntegralType {
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType.isInstanceOf[IntegralType] => true
    case _ => false
  }
}
abstract class IntegralType extends NumericType {
  val integral: Integral[JvmType]
}

case object LongType extends IntegralType {
  type JvmType = Long
  val numeric = implicitly[Numeric[Long]]
  val integral = implicitly[Integral[Long]]
  val ordering = implicitly[Ordering[JvmType]]
}

case object IntegerType extends IntegralType {
  type JvmType = Int
  val numeric = implicitly[Numeric[Int]]
  val integral = implicitly[Integral[Int]]
  val ordering = implicitly[Ordering[JvmType]]
}

case object ShortType extends IntegralType {
  type JvmType = Short
  val numeric = implicitly[Numeric[Short]]
  val integral = implicitly[Integral[Short]]
  val ordering = implicitly[Ordering[JvmType]]
}

case object ByteType extends IntegralType {
  type JvmType = Byte
  val numeric = implicitly[Numeric[Byte]]
  val integral = implicitly[Integral[Byte]]
  val ordering = implicitly[Ordering[JvmType]]
}

/** Matcher for any expressions that evaluate to [[FractionalType]]s */
object FractionalType {
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType.isInstanceOf[FractionalType] => true
    case _ => false
  }
}
abstract class FractionalType extends NumericType {
  val fractional: Fractional[JvmType]
}

case object DecimalType extends FractionalType {
  type JvmType = BigDecimal
  val numeric = implicitly[Numeric[BigDecimal]]
  val fractional = implicitly[Fractional[BigDecimal]]
  val ordering = implicitly[Ordering[JvmType]]
}

case object DoubleType extends FractionalType {
  type JvmType = Double
  val numeric = implicitly[Numeric[Double]]
  val fractional = implicitly[Fractional[Double]]
  val ordering = implicitly[Ordering[JvmType]]
}

case object FloatType extends FractionalType {
  type JvmType = Float
  val numeric = implicitly[Numeric[Float]]
  val fractional = implicitly[Fractional[Float]]
  val ordering = implicitly[Ordering[JvmType]]
}

case class ArrayType(elementType: DataType) extends DataType

case class StructField(name: String, dataType: DataType, nullable: Boolean)
case class StructType(fields: Seq[StructField]) extends DataType

case class MapType(keyType: DataType, valueType: DataType) extends DataType
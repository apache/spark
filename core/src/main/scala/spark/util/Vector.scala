package spark.util

class Vector(val elements: Array[Double]) extends Serializable {
  def length = elements.length

  def apply(index: Int) = elements(index)

  def + (other: Vector): Vector = {
    if (length != other.length)
      throw new IllegalArgumentException("Vectors of different length")
    return Vector(length, i => this(i) + other(i))
  }

  def add(other: Vector) = this + other

  def - (other: Vector): Vector = {
    if (length != other.length)
      throw new IllegalArgumentException("Vectors of different length")
    return Vector(length, i => this(i) - other(i))
  }

  def subtract(other: Vector) = this - other

  def dot(other: Vector): Double = {
    if (length != other.length)
      throw new IllegalArgumentException("Vectors of different length")
    var ans = 0.0
    var i = 0
    while (i < length) {
      ans += this(i) * other(i)
      i += 1
    }
    return ans
  }

  /**
   * return (this + plus) dot other, but without creating any intermediate storage
   * @param plus
   * @param other
   * @return
   */
  def plusDot(plus: Vector, other: Vector): Double = {
    if (length != other.length)
      throw new IllegalArgumentException("Vectors of different length")
    if (length != plus.length)
      throw new IllegalArgumentException("Vectors of different length")
    var ans = 0.0
    var i = 0
    while (i < length) {
      ans += (this(i) + plus(i)) * other(i)
      i += 1
    }
    return ans
  }

  def += (other: Vector): Vector = {
    if (length != other.length)
      throw new IllegalArgumentException("Vectors of different length")
    var ans = 0.0
    var i = 0
    while (i < length) {
      elements(i) += other(i)
      i += 1
    }
    this
  }

  def addInPlace(other: Vector) = this +=other

  def * (scale: Double): Vector = Vector(length, i => this(i) * scale)

  def multiply (d: Double) = this * d

  def / (d: Double): Vector = this * (1 / d)

  def divide (d: Double) = this / d

  def unary_- = this * -1

  def sum = elements.reduceLeft(_ + _)

  def squaredDist(other: Vector): Double = {
    var ans = 0.0
    var i = 0
    while (i < length) {
      ans += (this(i) - other(i)) * (this(i) - other(i))
      i += 1
    }
    return ans
  }

  def dist(other: Vector): Double = math.sqrt(squaredDist(other))

  override def toString = elements.mkString("(", ", ", ")")
}

object Vector {
  def apply(elements: Array[Double]) = new Vector(elements)

  def apply(elements: Double*) = new Vector(elements.toArray)

  def apply(length: Int, initializer: Int => Double): Vector = {
    val elements = new Array[Double](length)
    for (i <- 0 until length)
      elements(i) = initializer(i)
    return new Vector(elements)
  }

  def zeros(length: Int) = new Vector(new Array[Double](length))

  def ones(length: Int) = Vector(length, _ => 1)

  class Multiplier(num: Double) {
    def * (vec: Vector) = vec * num
  }

  implicit def doubleToMultiplier(num: Double) = new Multiplier(num)

  implicit object VectorAccumParam extends spark.AccumulatorParam[Vector] {
    def addInPlace(t1: Vector, t2: Vector) = t1 + t2

    def zero(initialValue: Vector) = Vector.zeros(initialValue.length)
  }

}

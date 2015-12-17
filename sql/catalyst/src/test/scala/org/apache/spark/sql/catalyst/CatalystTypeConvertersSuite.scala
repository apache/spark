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

package org.apache.spark.sql.catalyst

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

class CatalystTypeConvertersSuite extends SparkFunSuite {

  private val simpleTypes: Seq[DataType] = Seq(
    StringType,
    DateType,
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType.SYSTEM_DEFAULT,
    DecimalType.USER_DEFAULT)

  test("null handling in rows") {
    val schema = StructType(simpleTypes.map(t => StructField(t.getClass.getName, t)))
    val convertToCatalyst = CatalystTypeConverters.createToCatalystConverter(schema)
    val convertToScala = CatalystTypeConverters.createToScalaConverter(schema)

    val scalaRow = Row.fromSeq(Seq.fill(simpleTypes.length)(null))
    assert(convertToScala(convertToCatalyst(scalaRow)) === scalaRow)
  }

  test("null handling for individual values") {
    for (dataType <- simpleTypes) {
      assert(CatalystTypeConverters.createToScalaConverter(dataType)(null) === null)
    }
  }

  test("option handling in convertToCatalyst") {
    // convertToCatalyst doesn't handle unboxing from Options. This is inconsistent with
    // createToCatalystConverter but it may not actually matter as this is only called internally
    // in a handful of places where we don't expect to receive Options.
    assert(CatalystTypeConverters.convertToCatalyst(Some(123)) === Some(123))
  }

  test("option handling in createToCatalystConverter") {
    assert(CatalystTypeConverters.createToCatalystConverter(IntegerType)(Some(123)) === 123)
  }

  import ScalaReflection._

  test("createToProductConverter[T] with a case class") {
    val a = A(1)
    assert(a === rebuildWithProductConverter(a))
  }

  test("createToProductConverter[T] with a case class and null value") {
    val a = A(null.asInstanceOf[Int])
    assert(a === rebuildWithProductConverter(a))
  }

  test("createToProductConverter[T] with a case class dependent on another case class") {
    val b = B(A(1), 2.0)
    assert(b === rebuildWithProductConverter(b))
  }

  test("createToProductConverter[T] with a case class dep. on case class dep. on case class") {
    val c = C(B(A(1), 2.0), "hi everybody")
    assert(c === rebuildWithProductConverter(c))
  }

  test("createToProductConverter[T] with T having Seqs") {
    val d = D(B(A(1), 2.0), Seq(A(1), A(5)), Seq(3, 8), 10L)
    assert(d === rebuildWithProductConverter(d))
  }

  test("createToProductConverter[T] with T having Maps") {
    val e = E(B(A(1), 2.0),
      Map(A(1) -> A(5), A(11) -> A(15)),
      Map(A(1) -> 5, A(11) -> 15),
      Map(1 -> A(5), 11 -> A(15)),
      Map(1 -> 5, 11 -> 15))
    assert(e === rebuildWithProductConverter(e))
  }

  test("createToProductConverter[T] with T having multiple constructors") {
    val f = new F(1)
    assert(F(1, "hi everybody") === rebuildWithProductConverter(f))
  }

  test("createToProductConverter[T] with T having String") {
    val g = new G("hi everybody!")
    assert(g === rebuildWithProductConverter(g))
  }

  test("createToProductConverter[T] with an incompatible case class fails at conversion") {
    val a = A(1)
    val dataType = schemaFor[A].dataType
    val row = CatalystTypeConverters.createToCatalystConverter(dataType)(a)
    val converter =
      CatalystTypeConverters.createToProductConverter[G](dataType.asInstanceOf[StructType])
    intercept[ClassCastException] { converter(row.asInstanceOf[InternalRow]) }
  }

  test("createToProductConverter[T] with T having Some[U]") {
    val h = H(Some(1))
    assert(h === rebuildWithProductConverter(h))
  }

  test("createToProductConverter[T] with T having None") {
    val h = H(None)
    assert(h === rebuildWithProductConverter(h))
  }

  test("createToProductConverter[T] with T having Option[Option[U]]") {
    val i = I(Some(H(Some(1))))
    assert(i === rebuildWithProductConverter(i))
  }

  test("createToProductConverter[T] with T having Option[Seq[U]]") {
    val j = J(Some(Seq(A(1), A(2))))
    assert(j === rebuildWithProductConverter(j))
  }

  test("createToProductConverter[T] with T having BigDecimal") {
    val k = K(BigDecimal("123.0"))
    assert(k === rebuildWithProductConverter(k))
  }

  test("createToProductConverter[T] with T having java.math.BigDecimal") {
    // NOTE: As currently implemented, Decimal.apply(java.math.BigDecimal) triggers the implicit
    // BigDecimal.javaBigDecimal2bigDecimal, creating a Scala BigDecimal with
    // BigDecimal.defaultMathContext. So a given java.math.BigDecimal ends up taking maximum
    // precision, and the scale is truncated to Decimal.MAX_LONG_DIGITS when converting back. This
    // unit test accounts for that current behavior.
    val l = L(new java.math.BigDecimal(new java.math.BigInteger("123"), Decimal.MAX_LONG_DIGITS))
    assert(l === rebuildWithProductConverter(l))
  }

  test("createToProductConverter[T] with T having java.sql.Date") {
    val m = M(DateTimeUtils.toJavaDate(daysSinceEpoch = 16000))
    assert(m === rebuildWithProductConverter(m))
  }

  test("createToProductConverter[T] with T having java.sql.Timestamp") {
    val n = N(DateTimeUtils.toJavaTimestamp(System.currentTimeMillis * 1000L))
    assert(n === rebuildWithProductConverter(n))
  }

  test("createToProductConverter[T] with T having Array[Byte] (treated as BinaryType)") {
    // Use different objects to simulate not having the same byte array ref.
    val p0 = P("hi everybody!".getBytes)
    val p1 = P("hi everybody!".getBytes)
    val dataType = schemaFor[P].dataType
    val row = CatalystTypeConverters.createToCatalystConverter(dataType)(p1)
    val converter =
      CatalystTypeConverters.createToProductConverter[P](dataType.asInstanceOf[StructType])
    val rebuildWithProductConverterP1 = converter(row.asInstanceOf[InternalRow])
    assert(p0 === rebuildWithProductConverterP1)
  }

  test("createToProductConverter[T] with T having Array[_ != Byte] isn't supported") {
    intercept[UnsupportedOperationException] {
      val q = Q(Array(1, 2, 3))
      val dataType = schemaFor[Q].dataType
      val row = CatalystTypeConverters.createToCatalystConverter(dataType)(q)
      val converter =
        CatalystTypeConverters.createToProductConverter[Q](dataType.asInstanceOf[StructType])
      converter(row.asInstanceOf[InternalRow])
    }
  }

  test("createToProductConverter[T] with T having UDT") {
    val r = R(Point(3.0, 8.0))
    assert(r === rebuildWithProductConverter(r))
  }

  def rebuildWithProductConverter[T <: Product : TypeTag : ClassTag](obj: T): T = {
    val dataType = schemaFor[T].dataType
    val row = CatalystTypeConverters.createToCatalystConverter(dataType)(obj)
    val converter =
      CatalystTypeConverters.createToProductConverter[T](dataType.asInstanceOf[StructType])
    converter(row.asInstanceOf[InternalRow])
  }
}

case class A(x: Int)
case class B(a: A, y: Double)
case class C(b: B, z: String)
case class D(b: B, d: Seq[A], p: Seq[Int], q: Long)
case class E(b: B, r: Map[A, A], s: Map[A, Int], t: Map[Int, A], u: Map[Int, Int])

case class F(f: Int, v: String) {
  def this(f: Int) = this(f, "hi everybody")
}

case class G(g: String)
case class H(x: Option[Int])
case class I(h: Option[H])
case class J(h: Option[Seq[A]])
case class K(bd: BigDecimal)
case class L(bd: java.math.BigDecimal)
case class M(d: java.sql.Date)
case class N(t: java.sql.Timestamp)

case class P(ba: Array[Byte]) {
  override def equals(o: Any): Boolean = o match {
    case p: P => java.util.Arrays.equals(ba, p.ba)
    case _ => false
  }

  override def hashCode(): Int = java.util.Arrays.hashCode(ba)
}

case class Q(a: Array[Int])

case class R(p: Point)

// UDT-related test classes. They are case classes to make testing equality easier.

@SQLUserDefinedType(udt = classOf[PointUDT])
case class Point(val x: Double, val y: Double)

case class PointUDT() extends UserDefinedType[Point] {

  override def sqlType: DataType = ArrayType(DoubleType, false)

  override def serialize(obj: Any): Seq[Double] = {
    obj match {
      case p: Point => Seq(p.x, p.y)
      case _ => throw new IllegalArgumentException(s"${obj} not serializable")
    }
  }

  override def deserialize(datum: Any): Point = {
    datum match {
      case values: Seq[_] =>
        val xy = values.asInstanceOf[Seq[Double]]
        assert(xy.length == 2)
        new Point(xy(0), xy(1))
    }
  }

  override def userClass: Class[Point] = classOf[Point]

  override def asNullable: PointUDT = this
}

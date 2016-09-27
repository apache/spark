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

package org.apache.spark.sql.catalyst.expressions

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util
import java.util.Objects
import javax.xml.bind.DatatypeConverter

import org.json4s.JsonAST._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types._

object Literal {
  val TrueLiteral: Literal = Literal(true, BooleanType)

  val FalseLiteral: Literal = Literal(false, BooleanType)

  def apply(v: Any): Literal = v match {
    case i: Int => Literal(i, IntegerType)
    case l: Long => Literal(l, LongType)
    case d: Double => Literal(d, DoubleType)
    case f: Float => Literal(f, FloatType)
    case b: Byte => Literal(b, ByteType)
    case s: Short => Literal(s, ShortType)
    case s: String => Literal(UTF8String.fromString(s), StringType)
    case b: Boolean => Literal(b, BooleanType)
    case d: BigDecimal => Literal(Decimal(d), DecimalType(Math.max(d.precision, d.scale), d.scale))
    case d: java.math.BigDecimal =>
      Literal(Decimal(d), DecimalType(Math.max(d.precision, d.scale), d.scale()))
    case d: Decimal => Literal(d, DecimalType(Math.max(d.precision, d.scale), d.scale))
    case t: Timestamp => Literal(DateTimeUtils.fromJavaTimestamp(t), TimestampType)
    case d: Date => Literal(DateTimeUtils.fromJavaDate(d), DateType)
    case a: Array[Byte] => Literal(a, BinaryType)
    case a: Array[_] =>
      val elementType = componentTypeToDataType(a.getClass.getComponentType())
      val dataType = ArrayType(elementType)
      val convert = CatalystTypeConverters.createToCatalystConverter(dataType)
      Literal(convert(a), dataType)
    case i: CalendarInterval => Literal(i, CalendarIntervalType)
    case null => Literal(null, NullType)
    case v: Literal => v
    case _ =>
      throw new RuntimeException("Unsupported literal type " + v.getClass + " " + v)
  }

  private def componentTypeToDataType(clz: Class[_]): DataType = clz match {
    // primitive type
    case c: Class[_] if c == java.lang.Short.TYPE => ShortType
    case c: Class[_] if c == java.lang.Integer.TYPE => IntegerType
    case c: Class[_] if c == java.lang.Long.TYPE => LongType
    case c: Class[_] if c == java.lang.Double.TYPE => DoubleType
    case c: Class[_] if c == java.lang.Byte.TYPE => ByteType
    case c: Class[_] if c == java.lang.Float.TYPE => FloatType
    case c: Class[_] if c == java.lang.Boolean.TYPE => BooleanType

    // java class
    case c: Class[_] if c == classOf[java.lang.String] => StringType
    case c: Class[_] if c == classOf[java.sql.Date] => DateType
    case c: Class[_] if c == classOf[java.sql.Timestamp] => TimestampType
    case c: Class[_] if c == classOf[java.math.BigDecimal] => DecimalType.SYSTEM_DEFAULT
    case c: Class[_] if c == classOf[Array[Byte]] => BinaryType
    case c: Class[_] if c == classOf[java.lang.Short] => ShortType
    case c: Class[_] if c == classOf[java.lang.Integer] => IntegerType
    case c: Class[_] if c == classOf[java.lang.Long] => LongType
    case c: Class[_] if c == classOf[java.lang.Double] => DoubleType
    case c: Class[_] if c == classOf[java.lang.Byte] => ByteType
    case c: Class[_] if c == classOf[java.lang.Float] => FloatType
    case c: Class[_] if c == classOf[java.lang.Boolean] => BooleanType

    // scala class
    case c: Class[_] if c == classOf[scala.math.BigInt] => DecimalType.SYSTEM_DEFAULT
    case c: Class[_] if c == classOf[scala.math.BigDecimal] => DecimalType.SYSTEM_DEFAULT

    case c: Class[_] if c.isArray => ArrayType(componentTypeToDataType(c.getComponentType))

    case c => throw new AnalysisException(s"Unsupported component type $c in arrays")
  }

  /**
   * Constructs a [[Literal]] of [[ObjectType]], for example when you need to pass an object
   * into code generation.
   */
  def fromObject(obj: Any, objType: DataType): Literal = new Literal(obj, objType)
  def fromObject(obj: Any): Literal = new Literal(obj, ObjectType(obj.getClass))

  def fromJSON(json: JValue): Literal = {
    val dataType = DataType.parseDataType(json \ "dataType")
    json \ "value" match {
      case JNull => Literal.create(null, dataType)
      case JString(str) =>
        val value = dataType match {
          case BooleanType => str.toBoolean
          case ByteType => str.toByte
          case ShortType => str.toShort
          case IntegerType => str.toInt
          case LongType => str.toLong
          case FloatType => str.toFloat
          case DoubleType => str.toDouble
          case StringType => UTF8String.fromString(str)
          case DateType => java.sql.Date.valueOf(str)
          case TimestampType => java.sql.Timestamp.valueOf(str)
          case CalendarIntervalType => CalendarInterval.fromString(str)
          case t: DecimalType =>
            val d = Decimal(str)
            assert(d.changePrecision(t.precision, t.scale))
            d
          case _ => null
        }
        Literal.create(value, dataType)
      case other => sys.error(s"$other is not a valid Literal json value")
    }
  }

  def create(v: Any, dataType: DataType): Literal = {
    Literal(CatalystTypeConverters.convertToCatalyst(v), dataType)
  }

  /**
   * Create a literal with default value for given DataType
   */
  def default(dataType: DataType): Literal = dataType match {
    case NullType => create(null, NullType)
    case BooleanType => Literal(false)
    case ByteType => Literal(0.toByte)
    case ShortType => Literal(0.toShort)
    case IntegerType => Literal(0)
    case LongType => Literal(0L)
    case FloatType => Literal(0.0f)
    case DoubleType => Literal(0.0)
    case dt: DecimalType => Literal(Decimal(0, dt.precision, dt.scale))
    case DateType => create(0, DateType)
    case TimestampType => create(0L, TimestampType)
    case StringType => Literal("")
    case BinaryType => Literal("".getBytes(StandardCharsets.UTF_8))
    case CalendarIntervalType => Literal(new CalendarInterval(0, 0))
    case arr: ArrayType => create(Array(), arr)
    case map: MapType => create(Map(), map)
    case struct: StructType =>
      create(InternalRow.fromSeq(struct.fields.map(f => default(f.dataType).value)), struct)
    case other =>
      throw new RuntimeException(s"no default for type $dataType")
  }
}

/**
 * An extractor that matches non-null literal values
 */
object NonNullLiteral {
  def unapply(literal: Literal): Option[(Any, DataType)] = {
    Option(literal.value).map(_ => (literal.value, literal.dataType))
  }
}

/**
 * Extractor for retrieving Int literals.
 */
object IntegerLiteral {
  def unapply(a: Any): Option[Int] = a match {
    case Literal(a: Int, IntegerType) => Some(a)
    case _ => None
  }
}

/**
 * Extractor for and other utility methods for decimal literals.
 */
object DecimalLiteral {
  def apply(v: Long): Literal = Literal(Decimal(v))

  def apply(v: Double): Literal = Literal(Decimal(v))

  def unapply(e: Expression): Option[Decimal] = e match {
    case Literal(v, _: DecimalType) => Some(v.asInstanceOf[Decimal])
    case _ => None
  }

  def largerThanLargestLong(v: Decimal): Boolean = v > Decimal(Long.MaxValue)

  def smallerThanSmallestLong(v: Decimal): Boolean = v < Decimal(Long.MinValue)
}

/**
 * In order to do type checking, use Literal.create() instead of constructor
 */
case class Literal (value: Any, dataType: DataType) extends LeafExpression with CodegenFallback {

  override def foldable: Boolean = true
  override def nullable: Boolean = value == null

  override def toString: String = value match {
    case null => "null"
    case binary: Array[Byte] => s"0x" + DatatypeConverter.printHexBinary(binary)
    case other => other.toString
  }

  override def hashCode(): Int = {
    val valueHashCode = value match {
      case null => 0
      case binary: Array[Byte] => util.Arrays.hashCode(binary)
      case other => other.hashCode()
    }
    31 * Objects.hashCode(dataType) + valueHashCode
  }

  override def equals(other: Any): Boolean = other match {
    case o: Literal if !dataType.equals(o.dataType) => false
    case o: Literal =>
      (value, o.value) match {
        case (null, null) => true
        case (a: Array[Byte], b: Array[Byte]) => util.Arrays.equals(a, b)
        case (a, b) => a != null && a.equals(b)
      }
    case _ => false
  }

  override protected def jsonFields: List[JField] = {
    // Turns all kinds of literal values to string in json field, as the type info is hard to
    // retain in json format, e.g. {"a": 123} can be an int, or double, or decimal, etc.
    val jsonValue = (value, dataType) match {
      case (null, _) => JNull
      case (i: Int, DateType) => JString(DateTimeUtils.toJavaDate(i).toString)
      case (l: Long, TimestampType) => JString(DateTimeUtils.toJavaTimestamp(l).toString)
      case (other, _) => JString(other.toString)
    }
    ("value" -> jsonValue) :: ("dataType" -> dataType.jsonValue) :: Nil
  }

  override def eval(input: InternalRow): Any = value

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // change the isNull and primitive to consts, to inline them
    if (value == null) {
      ev.isNull = "true"
      ev.copy(s"final ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};")
    } else {
      dataType match {
        case BooleanType =>
          ev.isNull = "false"
          ev.value = value.toString
          ev.copy("")
        case FloatType =>
          val v = value.asInstanceOf[Float]
          if (v.isNaN || v.isInfinite) {
            super[CodegenFallback].doGenCode(ctx, ev)
          } else {
            ev.isNull = "false"
            ev.value = s"${value}f"
            ev.copy("")
          }
        case DoubleType =>
          val v = value.asInstanceOf[Double]
          if (v.isNaN || v.isInfinite) {
            super[CodegenFallback].doGenCode(ctx, ev)
          } else {
            ev.isNull = "false"
            ev.value = s"${value}D"
            ev.copy("")
          }
        case ByteType | ShortType =>
          ev.isNull = "false"
          ev.value = s"(${ctx.javaType(dataType)})$value"
          ev.copy("")
        case IntegerType | DateType =>
          ev.isNull = "false"
          ev.value = value.toString
          ev.copy("")
        case TimestampType | LongType =>
          ev.isNull = "false"
          ev.value = s"${value}L"
          ev.copy("")
        // eval() version may be faster for non-primitive types
        case other =>
          super[CodegenFallback].doGenCode(ctx, ev)
      }
    }
  }

  override def sql: String = (value, dataType) match {
    case (_, NullType | _: ArrayType | _: MapType | _: StructType) if value == null => "NULL"
    case _ if value == null => s"CAST(NULL AS ${dataType.sql})"
    case (v: UTF8String, StringType) =>
      // Escapes all backslashes and single quotes.
      "'" + v.toString.replace("\\", "\\\\").replace("'", "\\'") + "'"
    case (v: Byte, ByteType) => v + "Y"
    case (v: Short, ShortType) => v + "S"
    case (v: Long, LongType) => v + "L"
    // Float type doesn't have a suffix
    case (v: Float, FloatType) =>
      val castedValue = v match {
        case _ if v.isNaN => "'NaN'"
        case Float.PositiveInfinity => "'Infinity'"
        case Float.NegativeInfinity => "'-Infinity'"
        case _ => v
      }
      s"CAST($castedValue AS ${FloatType.sql})"
    case (v: Double, DoubleType) =>
      v match {
        case _ if v.isNaN => s"CAST('NaN' AS ${DoubleType.sql})"
        case Double.PositiveInfinity => s"CAST('Infinity' AS ${DoubleType.sql})"
        case Double.NegativeInfinity => s"CAST('-Infinity' AS ${DoubleType.sql})"
        case _ => v + "D"
      }
    case (v: Decimal, t: DecimalType) => v + "BD"
    case (v: Int, DateType) => s"DATE '${DateTimeUtils.toJavaDate(v)}'"
    case (v: Long, TimestampType) => s"TIMESTAMP('${DateTimeUtils.toJavaTimestamp(v)}')"
    case (v: Array[Byte], BinaryType) => s"X'${DatatypeConverter.printHexBinary(v)}'"
    case _ => value.toString
  }
}

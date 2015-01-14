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

package org.apache.spark.sql.types

import java.sql.{Date, Timestamp}

import scala.math.Numeric.{FloatAsIfIntegral, DoubleAsIfIntegral}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, runtimeMirror, typeTag}
import scala.util.parsing.combinator.RegexParsers

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.util.Utils


object DataType {
  def fromJson(json: String): DataType = parseDataType(parse(json))

  private object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) => Some(seq.toList.sortBy(_._1))
      case _ => None
    }
  }

  // NOTE: Map fields must be sorted in alphabetical order to keep consistent with the Python side.
  private def parseDataType(json: JValue): DataType = json match {
    case JString(name) =>
      PrimitiveType.nameToType(name)

    case JSortedObject(
        ("containsNull", JBool(n)),
        ("elementType", t: JValue),
        ("type", JString("array"))) =>
      ArrayType(parseDataType(t), n)

    case JSortedObject(
        ("keyType", k: JValue),
        ("type", JString("map")),
        ("valueContainsNull", JBool(n)),
        ("valueType", v: JValue)) =>
      MapType(parseDataType(k), parseDataType(v), n)

    case JSortedObject(
        ("fields", JArray(fields)),
        ("type", JString("struct"))) =>
      StructType(fields.map(parseStructField))

    case JSortedObject(
        ("class", JString(udtClass)),
        ("pyClass", _),
        ("sqlType", _),
        ("type", JString("udt"))) =>
      Class.forName(udtClass).newInstance().asInstanceOf[UserDefinedType[_]]
  }

  private def parseStructField(json: JValue): StructField = json match {
    case JSortedObject(
        ("metadata", metadata: JObject),
        ("name", JString(name)),
        ("nullable", JBool(nullable)),
        ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable, Metadata.fromJObject(metadata))
    // Support reading schema when 'metadata' is missing.
    case JSortedObject(
        ("name", JString(name)),
        ("nullable", JBool(nullable)),
        ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable)
  }

  @deprecated("Use DataType.fromJson instead", "1.2.0")
  def fromCaseClassString(string: String): DataType = CaseClassStringParser(string)

  private object CaseClassStringParser extends RegexParsers {
    protected lazy val primitiveType: Parser[DataType] =
      ( "StringType" ^^^ StringType
      | "FloatType" ^^^ FloatType
      | "IntegerType" ^^^ IntegerType
      | "ByteType" ^^^ ByteType
      | "ShortType" ^^^ ShortType
      | "DoubleType" ^^^ DoubleType
      | "LongType" ^^^ LongType
      | "BinaryType" ^^^ BinaryType
      | "BooleanType" ^^^ BooleanType
      | "DateType" ^^^ DateType
      | "DecimalType()" ^^^ DecimalType.Unlimited
      | fixedDecimalType
      | "TimestampType" ^^^ TimestampType
      )

    protected lazy val fixedDecimalType: Parser[DataType] =
      ("DecimalType(" ~> "[0-9]+".r) ~ ("," ~> "[0-9]+".r <~ ")") ^^ {
        case precision ~ scale => DecimalType(precision.toInt, scale.toInt)
      }

    protected lazy val arrayType: Parser[DataType] =
      "ArrayType" ~> "(" ~> dataType ~ "," ~ boolVal <~ ")" ^^ {
        case tpe ~ _ ~ containsNull => ArrayType(tpe, containsNull)
      }

    protected lazy val mapType: Parser[DataType] =
      "MapType" ~> "(" ~> dataType ~ "," ~ dataType ~ "," ~ boolVal <~ ")" ^^ {
        case t1 ~ _ ~ t2 ~ _ ~ valueContainsNull => MapType(t1, t2, valueContainsNull)
      }

    protected lazy val structField: Parser[StructField] =
      ("StructField(" ~> "[a-zA-Z0-9_]*".r) ~ ("," ~> dataType) ~ ("," ~> boolVal <~ ")") ^^ {
        case name ~ tpe ~ nullable  =>
          StructField(name, tpe, nullable = nullable)
      }

    protected lazy val boolVal: Parser[Boolean] =
      ( "true" ^^^ true
      | "false" ^^^ false
      )

    protected lazy val structType: Parser[DataType] =
      "StructType\\([A-zA-z]*\\(".r ~> repsep(structField, ",") <~ "))" ^^ {
        case fields => StructType(fields)
      }

    protected lazy val dataType: Parser[DataType] =
      ( arrayType
      | mapType
      | structType
      | primitiveType
      )

    /**
     * Parses a string representation of a DataType.
     *
     * TODO: Generate parser as pickler...
     */
    def apply(asString: String): DataType = parseAll(dataType, asString) match {
      case Success(result, _) => result
      case failure: NoSuccess =>
        throw new IllegalArgumentException(s"Unsupported dataType: $asString, $failure")
    }

  }

  protected[types] def buildFormattedString(
      dataType: DataType,
      prefix: String,
      builder: StringBuilder): Unit = {
    dataType match {
      case array: ArrayType =>
        array.buildFormattedString(prefix, builder)
      case struct: StructType =>
        struct.buildFormattedString(prefix, builder)
      case map: MapType =>
        map.buildFormattedString(prefix, builder)
      case _ =>
    }
  }

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType.
   */
  private[sql] def equalsIgnoreNullability(left: DataType, right: DataType): Boolean = {
    (left, right) match {
      case (ArrayType(leftElementType, _), ArrayType(rightElementType, _)) =>
        equalsIgnoreNullability(leftElementType, rightElementType)
      case (MapType(leftKeyType, leftValueType, _), MapType(rightKeyType, rightValueType, _)) =>
        equalsIgnoreNullability(leftKeyType, rightKeyType) &&
        equalsIgnoreNullability(leftValueType, rightValueType)
      case (StructType(leftFields), StructType(rightFields)) =>
        leftFields.size == rightFields.size &&
        leftFields.zip(rightFields)
          .forall{
            case (left, right) =>
              left.name == right.name && equalsIgnoreNullability(left.dataType, right.dataType)
          }
      case (left, right) => left == right
    }
  }
}


/**
 * :: DeveloperApi ::
 *
 * The base type of all Spark SQL data types.
 *
 * @group dataType
 */
@DeveloperApi
abstract class DataType {
  /** Matches any expression that evaluates to this DataType */
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType == this => true
    case _ => false
  }

  def isPrimitive: Boolean = false

  def typeName: String = this.getClass.getSimpleName.stripSuffix("$").dropRight(4).toLowerCase

  private[sql] def jsonValue: JValue = typeName

  def json: String = compact(render(jsonValue))

  def prettyJson: String = pretty(render(jsonValue))
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `NULL` values. Please use the singleton [[DataTypes.NullType]].
 *
 * @group dataType
 */
@DeveloperApi
case object NullType extends DataType


object NativeType {
  val all = Seq(
    IntegerType, BooleanType, LongType, DoubleType, FloatType, ShortType, ByteType, StringType)

  def unapply(dt: DataType): Boolean = all.contains(dt)

  val defaultSizeOf: Map[NativeType, Int] = Map(
    IntegerType -> 4,
    BooleanType -> 1,
    LongType -> 8,
    DoubleType -> 8,
    FloatType -> 4,
    ShortType -> 2,
    ByteType -> 1,
    StringType -> 4096)
}


trait PrimitiveType extends DataType {
  override def isPrimitive = true
}


object PrimitiveType {
  private val nonDecimals = Seq(NullType, DateType, TimestampType, BinaryType) ++ NativeType.all
  private val nonDecimalNameToType = nonDecimals.map(t => t.typeName -> t).toMap

  /** Given the string representation of a type, return its DataType */
  private[sql] def nameToType(name: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r
    name match {
      case "decimal" => DecimalType.Unlimited
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case other => nonDecimalNameToType(other)
    }
  }
}

abstract class NativeType extends DataType {
  private[sql] type JvmType
  @transient private[sql] val tag: TypeTag[JvmType]
  private[sql] val ordering: Ordering[JvmType]

  @transient private[sql] val classTag = ScalaReflectionLock.synchronized {
    val mirror = runtimeMirror(Utils.getSparkClassLoader)
    ClassTag[JvmType](mirror.runtimeClass(tag.tpe))
  }
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `String` values. Please use the singleton [[DataTypes.StringType]].
 *
 * @group dataType
 */
@DeveloperApi
case object StringType extends NativeType with PrimitiveType {
  private[sql] type JvmType = String
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `Array[Byte]` values.
 * Please use the singleton [[DataTypes.BinaryType]].
 *
 * @group dataType
 */
@DeveloperApi
case object BinaryType extends NativeType with PrimitiveType {
  private[sql] type JvmType = Array[Byte]
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val ordering = new Ordering[JvmType] {
    def compare(x: Array[Byte], y: Array[Byte]): Int = {
      for (i <- 0 until x.length; if i < y.length) {
        val res = x(i).compareTo(y(i))
        if (res != 0) return res
      }
      x.length - y.length
    }
  }
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `Boolean` values. Please use the singleton [[DataTypes.BooleanType]].
 *
 *@group dataType
 */
@DeveloperApi
case object BooleanType extends NativeType with PrimitiveType {
  private[sql] type JvmType = Boolean
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `java.sql.Timestamp` values.
 * Please use the singleton [[DataTypes.TimestampType]].
 *
 * @group dataType
 */
@DeveloperApi
case object TimestampType extends NativeType {
  private[sql] type JvmType = Timestamp

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }

  private[sql] val ordering = new Ordering[JvmType] {
    def compare(x: Timestamp, y: Timestamp) = x.compareTo(y)
  }
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `java.sql.Date` values.
 * Please use the singleton [[DataTypes.DateType]].
 *
 * @group dataType
 */
@DeveloperApi
case object DateType extends NativeType {
  private[sql] type JvmType = Date

  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }

  private[sql] val ordering = new Ordering[JvmType] {
    def compare(x: Date, y: Date) = x.compareTo(y)
  }
}


abstract class NumericType extends NativeType with PrimitiveType {
  // Unfortunately we can't get this implicitly as that breaks Spark Serialization. In order for
  // implicitly[Numeric[JvmType]] to be valid, we have to change JvmType from a type variable to a
  // type parameter and and add a numeric annotation (i.e., [JvmType : Numeric]). This gets
  // desugared by the compiler into an argument to the objects constructor. This means there is no
  // longer an no argument constructor and thus the JVM cannot serialize the object anymore.
  private[sql] val numeric: Numeric[JvmType]
}


object NumericType {
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[NumericType]
}


/** Matcher for any expressions that evaluate to [[IntegralType]]s */
object IntegralType {
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType.isInstanceOf[IntegralType] => true
    case _ => false
  }
}


sealed abstract class IntegralType extends NumericType {
  private[sql] val integral: Integral[JvmType]
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `Long` values. Please use the singleton [[DataTypes.LongType]].
 *
 * @group dataType
 */
@DeveloperApi
case object LongType extends IntegralType {
  private[sql] type JvmType = Long
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Long]]
  private[sql] val integral = implicitly[Integral[Long]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `Int` values. Please use the singleton [[DataTypes.IntegerType]].
 *
 * @group dataType
 */
@DeveloperApi
case object IntegerType extends IntegralType {
  private[sql] type JvmType = Int
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Int]]
  private[sql] val integral = implicitly[Integral[Int]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `Short` values. Please use the singleton [[DataTypes.ShortType]].
 *
 * @group dataType
 */
@DeveloperApi
case object ShortType extends IntegralType {
  private[sql] type JvmType = Short
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Short]]
  private[sql] val integral = implicitly[Integral[Short]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `Byte` values. Please use the singleton [[DataTypes.ByteType]].
 *
 * @group dataType
 */
@DeveloperApi
case object ByteType extends IntegralType {
  private[sql] type JvmType = Byte
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Byte]]
  private[sql] val integral = implicitly[Integral[Byte]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
}


/** Matcher for any expressions that evaluate to [[FractionalType]]s */
object FractionalType {
  def unapply(a: Expression): Boolean = a match {
    case e: Expression if e.dataType.isInstanceOf[FractionalType] => true
    case _ => false
  }
}


sealed abstract class FractionalType extends NumericType {
  private[sql] val fractional: Fractional[JvmType]
  private[sql] val asIntegral: Integral[JvmType]
}


/** Precision parameters for a Decimal */
case class PrecisionInfo(precision: Int, scale: Int)


/**
 * :: DeveloperApi ::
 *
 * The data type representing `scala.math.BigDecimal` values.
 * A Decimal that might have fixed precision and scale, or unlimited values for these.
 *
 * Please use [[DataTypes.createDecimalType()]] to create a specific instance.
 *
 * @group dataType
 */
@DeveloperApi
case class DecimalType(precisionInfo: Option[PrecisionInfo]) extends FractionalType {
  private[sql] type JvmType = Decimal
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = Decimal.DecimalIsFractional
  private[sql] val fractional = Decimal.DecimalIsFractional
  private[sql] val ordering = Decimal.DecimalIsFractional
  private[sql] val asIntegral = Decimal.DecimalAsIfIntegral

  def precision: Int = precisionInfo.map(_.precision).getOrElse(-1)

  def scale: Int = precisionInfo.map(_.scale).getOrElse(-1)

  override def typeName: String = precisionInfo match {
    case Some(PrecisionInfo(precision, scale)) => s"decimal($precision,$scale)"
    case None => "decimal"
  }

  override def toString: String = precisionInfo match {
    case Some(PrecisionInfo(precision, scale)) => s"DecimalType($precision,$scale)"
    case None => "DecimalType()"
  }
}


/** Extra factory methods and pattern matchers for Decimals */
object DecimalType {
  val Unlimited: DecimalType = DecimalType(None)

  object Fixed {
    def unapply(t: DecimalType): Option[(Int, Int)] =
      t.precisionInfo.map(p => (p.precision, p.scale))
  }

  object Expression {
    def unapply(e: Expression): Option[(Int, Int)] = e.dataType match {
      case t: DecimalType => t.precisionInfo.map(p => (p.precision, p.scale))
      case _ => None
    }
  }

  def apply(): DecimalType = Unlimited

  def apply(precision: Int, scale: Int): DecimalType =
    DecimalType(Some(PrecisionInfo(precision, scale)))

  def unapply(t: DataType): Boolean = t.isInstanceOf[DecimalType]

  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[DecimalType]

  def isFixed(dataType: DataType): Boolean = dataType match {
    case DecimalType.Fixed(_, _) => true
    case _ => false
  }
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `Double` values. Please use the singleton [[DataTypes.DoubleType]].
 *
 * @group dataType
 */
@DeveloperApi
case object DoubleType extends FractionalType {
  private[sql] type JvmType = Double
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Double]]
  private[sql] val fractional = implicitly[Fractional[Double]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
  private[sql] val asIntegral = DoubleAsIfIntegral
}


/**
 * :: DeveloperApi ::
 *
 * The data type representing `Float` values. Please use the singleton [[DataTypes.FloatType]].
 *
 * @group dataType
 */
@DeveloperApi
case object FloatType extends FractionalType {
  private[sql] type JvmType = Float
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  private[sql] val numeric = implicitly[Numeric[Float]]
  private[sql] val fractional = implicitly[Fractional[Float]]
  private[sql] val ordering = implicitly[Ordering[JvmType]]
  private[sql] val asIntegral = FloatAsIfIntegral
}


object ArrayType {
  /** Construct a [[ArrayType]] object with the given element type. The `containsNull` is true. */
  def apply(elementType: DataType): ArrayType = ArrayType(elementType, true)
}


/**
 * :: DeveloperApi ::
 *
 * The data type for collections of multiple values.
 * Internally these are represented as columns that contain a ``scala.collection.Seq``.
 *
 * Please use [[DataTypes.createArrayType()]] to create a specific instance.
 *
 * An [[ArrayType]] object comprises two fields, `elementType: [[DataType]]` and
 * `containsNull: Boolean`. The field of `elementType` is used to specify the type of
 * array elements. The field of `containsNull` is used to specify if the array has `null` values.
 *
 * @param elementType The data type of values.
 * @param containsNull Indicates if values have `null` values
 *
 * @group dataType
 */
@DeveloperApi
case class ArrayType(elementType: DataType, containsNull: Boolean) extends DataType {
  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(
      s"$prefix-- element: ${elementType.typeName} (containsNull = $containsNull)\n")
    DataType.buildFormattedString(elementType, s"$prefix    |", builder)
  }

  override private[sql] def jsonValue =
    ("type" -> typeName) ~
      ("elementType" -> elementType.jsonValue) ~
      ("containsNull" -> containsNull)
}


/**
 * A field inside a StructType.
 *
 * @param name The name of this field.
 * @param dataType The data type of this field.
 * @param nullable Indicates if values of this field can be `null` values.
 * @param metadata The metadata of this field. The metadata should be preserved during
 *                 transformation if the content of the column is not modified, e.g, in selection.
 */
case class StructField(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    metadata: Metadata = Metadata.empty) {

  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(s"$prefix-- $name: ${dataType.typeName} (nullable = $nullable)\n")
    DataType.buildFormattedString(dataType, s"$prefix    |", builder)
  }

  // override the default toString to be compatible with legacy parquet files.
  override def toString: String = s"StructField($name,$dataType,$nullable)"

  private[sql] def jsonValue: JValue = {
    ("name" -> name) ~
      ("type" -> dataType.jsonValue) ~
      ("nullable" -> nullable) ~
      ("metadata" -> metadata.jsonValue)
  }
}


object StructType {
  protected[sql] def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  def apply(fields: Seq[StructField]): StructType = StructType(fields.toArray)

  def apply(fields: java.util.List[StructField]): StructType = {
    StructType(fields.toArray.asInstanceOf[Array[StructField]])
  }
}


/**
 * :: DeveloperApi ::
 *
 * A [[StructType]] object can be constructed by
 * {{{
 * StructType(fields: Seq[StructField])
 * }}}
 * For a [[StructType]] object, one or multiple [[StructField]]s can be extracted by names.
 * If multiple [[StructField]]s are extracted, a [[StructType]] object will be returned.
 * If a provided name does not have a matching field, it will be ignored. For the case
 * of extracting a single StructField, a `null` will be returned.
 * Example:
 * {{{
 * import org.apache.spark.sql._
 *
 * val struct =
 *   StructType(
 *     StructField("a", IntegerType, true) ::
 *     StructField("b", LongType, false) ::
 *     StructField("c", BooleanType, false) :: Nil)
 *
 * // Extract a single StructField.
 * val singleField = struct("b")
 * // singleField: StructField = StructField(b,LongType,false)
 *
 * // This struct does not have a field called "d". null will be returned.
 * val nonExisting = struct("d")
 * // nonExisting: StructField = null
 *
 * // Extract multiple StructFields. Field names are provided in a set.
 * // A StructType object will be returned.
 * val twoFields = struct(Set("b", "c"))
 * // twoFields: StructType =
 * //   StructType(List(StructField(b,LongType,false), StructField(c,BooleanType,false)))
 *
 * // Any names without matching fields will be ignored.
 * // For the case shown below, "d" will be ignored and
 * // it is treated as struct(Set("b", "c")).
 * val ignoreNonExisting = struct(Set("b", "c", "d"))
 * // ignoreNonExisting: StructType =
 * //   StructType(List(StructField(b,LongType,false), StructField(c,BooleanType,false)))
 * }}}
 *
 * A [[org.apache.spark.sql.Row]] object is used as a value of the StructType.
 * Example:
 * {{{
 * import org.apache.spark.sql._
 *
 * val innerStruct =
 *   StructType(
 *     StructField("f1", IntegerType, true) ::
 *     StructField("f2", LongType, false) ::
 *     StructField("f3", BooleanType, false) :: Nil)
 *
 * val struct = StructType(
 *   StructField("a", innerStruct, true) :: Nil)
 *
 * // Create a Row with the schema defined by struct
 * val row = Row(Row(1, 2, true))
 * // row: Row = [[1,2,true]]
 * }}}
 *
 * @group dataType
 */
@DeveloperApi
case class StructType(fields: Array[StructField]) extends DataType with Seq[StructField] {

  /** Returns all field names in an array. */
  def fieldNames: Array[String] = fields.map(_.name)

  private lazy val fieldNamesSet: Set[String] = fieldNames.toSet
  private lazy val nameToField: Map[String, StructField] = fields.map(f => f.name -> f).toMap

  /**
   * Extracts a [[StructField]] of the given name. If the [[StructType]] object does not
   * have a name matching the given name, `null` will be returned.
   */
  def apply(name: String): StructField = {
    nameToField.getOrElse(name, throw new IllegalArgumentException(s"Field $name does not exist."))
  }

  /**
   * Returns a [[StructType]] containing [[StructField]]s of the given names, preserving the
   * original order of fields. Those names which do not have matching fields will be ignored.
   */
  def apply(names: Set[String]): StructType = {
    val nonExistFields = names -- fieldNamesSet
    if (nonExistFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"Field ${nonExistFields.mkString(",")} does not exist.")
    }
    // Preserve the original order of fields.
    StructType(fields.filter(f => names.contains(f.name)))
  }

  protected[sql] def toAttributes: Seq[AttributeReference] =
    map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  def treeString: String = {
    val builder = new StringBuilder
    builder.append("root\n")
    val prefix = " |"
    fields.foreach(field => field.buildFormattedString(prefix, builder))

    builder.toString()
  }

  def printTreeString(): Unit = println(treeString)

  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    fields.foreach(field => field.buildFormattedString(prefix, builder))
  }

  override private[sql] def jsonValue =
    ("type" -> typeName) ~
      ("fields" -> map(_.jsonValue))

  override def apply(fieldIndex: Int): StructField = fields(fieldIndex)

  override def length: Int = fields.length

  override def iterator: Iterator[StructField] = fields.iterator
}


object MapType {
  /**
   * Construct a [[MapType]] object with the given key type and value type.
   * The `valueContainsNull` is true.
   */
  def apply(keyType: DataType, valueType: DataType): MapType =
    MapType(keyType: DataType, valueType: DataType, valueContainsNull = true)
}


/**
 * :: DeveloperApi ::
 *
 * The data type for Maps. Keys in a map are not allowed to have `null` values.
 *
 * Please use [[DataTypes.createMapType()]] to create a specific instance.
 *
 * @param keyType The data type of map keys.
 * @param valueType The data type of map values.
 * @param valueContainsNull Indicates if map values have `null` values.
 *
 * @group dataType
 */
case class MapType(
    keyType: DataType,
    valueType: DataType,
    valueContainsNull: Boolean) extends DataType {
  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(s"$prefix-- key: ${keyType.typeName}\n")
    builder.append(s"$prefix-- value: ${valueType.typeName} " +
      s"(valueContainsNull = $valueContainsNull)\n")
    DataType.buildFormattedString(keyType, s"$prefix    |", builder)
    DataType.buildFormattedString(valueType, s"$prefix    |", builder)
  }

  override private[sql] def jsonValue: JValue =
    ("type" -> typeName) ~
      ("keyType" -> keyType.jsonValue) ~
      ("valueType" -> valueType.jsonValue) ~
      ("valueContainsNull" -> valueContainsNull)
}


/**
 * ::DeveloperApi::
 * The data type for User Defined Types (UDTs).
 *
 * This interface allows a user to make their own classes more interoperable with SparkSQL;
 * e.g., by creating a [[UserDefinedType]] for a class X, it becomes possible to create
 * a SchemaRDD which has class X in the schema.
 *
 * For SparkSQL to recognize UDTs, the UDT must be annotated with
 * [[SQLUserDefinedType]].
 *
 * The conversion via `serialize` occurs when instantiating a `SchemaRDD` from another RDD.
 * The conversion via `deserialize` occurs when reading from a `SchemaRDD`.
 */
@DeveloperApi
abstract class UserDefinedType[UserType] extends DataType with Serializable {

  /** Underlying storage type for this UDT */
  def sqlType: DataType

  /** Paired Python UDT class, if exists. */
  def pyUDT: String = null

  /**
   * Convert the user type to a SQL datum
   *
   * TODO: Can we make this take obj: UserType?  The issue is in ScalaReflection.convertToCatalyst,
   *       where we need to convert Any to UserType.
   */
  def serialize(obj: Any): Any

  /** Convert a SQL datum to the user type */
  def deserialize(datum: Any): UserType

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
      ("class" -> this.getClass.getName) ~
      ("pyClass" -> pyUDT) ~
      ("sqlType" -> sqlType.jsonValue)
  }

  /**
   * Class object for the UserType
   */
  def userClass: java.lang.Class[UserType]
}

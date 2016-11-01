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

import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.util.zip.CRC32

import scala.annotation.tailrec

import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.unsafe.Platform

/**
 * A function that calculates an MD5 128-bit checksum and returns it as a hex string
 * For input of type [[BinaryType]]
 */
@ExpressionDescription(
  usage = "_FUNC_(input) - Returns an MD5 128-bit checksum as a hex string of the input",
  extended = "> SELECT _FUNC_('Spark');\n '8cde774d6f7333752ed72cacddb05126'")
case class Md5(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.fromString(DigestUtils.md5Hex(input.asInstanceOf[Array[Byte]]))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c =>
      s"UTF8String.fromString(org.apache.commons.codec.digest.DigestUtils.md5Hex($c))")
  }
}

/**
 * A function that calculates the SHA-2 family of functions (SHA-224, SHA-256, SHA-384, and SHA-512)
 * and returns it as a hex string. The first argument is the string or binary to be hashed. The
 * second argument indicates the desired bit length of the result, which must have a value of 224,
 * 256, 384, 512, or 0 (which is equivalent to 256). SHA-224 is supported starting from Java 8. If
 * asking for an unsupported SHA function, the return value is NULL. If either argument is NULL or
 * the hash length is not one of the permitted values, the return value is NULL.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_(input, bitLength) - Returns a checksum of SHA-2 family as a hex string of the input.
            SHA-224, SHA-256, SHA-384, and SHA-512 are supported. Bit length of 0 is equivalent to 256.""",
  extended = """> SELECT _FUNC_('Spark', 0);
               '529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b'""")
// scalastyle:on line.size.limit
case class Sha2(left: Expression, right: Expression)
  extends BinaryExpression with Serializable with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def nullable: Boolean = true

  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val bitLength = input2.asInstanceOf[Int]
    val input = input1.asInstanceOf[Array[Byte]]
    bitLength match {
      case 224 =>
        // DigestUtils doesn't support SHA-224 now
        try {
          val md = MessageDigest.getInstance("SHA-224")
          md.update(input)
          UTF8String.fromBytes(md.digest())
        } catch {
          // SHA-224 is not supported on the system, return null
          case noa: NoSuchAlgorithmException => null
        }
      case 256 | 0 =>
        UTF8String.fromString(DigestUtils.sha256Hex(input))
      case 384 =>
        UTF8String.fromString(DigestUtils.sha384Hex(input))
      case 512 =>
        UTF8String.fromString(DigestUtils.sha512Hex(input))
      case _ => null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val digestUtils = "org.apache.commons.codec.digest.DigestUtils"
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"""
        if ($eval2 == 224) {
          try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-224");
            md.update($eval1);
            ${ev.value} = UTF8String.fromBytes(md.digest());
          } catch (java.security.NoSuchAlgorithmException e) {
            ${ev.isNull} = true;
          }
        } else if ($eval2 == 256 || $eval2 == 0) {
          ${ev.value} =
            UTF8String.fromString($digestUtils.sha256Hex($eval1));
        } else if ($eval2 == 384) {
          ${ev.value} =
            UTF8String.fromString($digestUtils.sha384Hex($eval1));
        } else if ($eval2 == 512) {
          ${ev.value} =
            UTF8String.fromString($digestUtils.sha512Hex($eval1));
        } else {
          ${ev.isNull} = true;
        }
      """
    })
  }
}

/**
 * A function that calculates a sha1 hash value and returns it as a hex string
 * For input of type [[BinaryType]] or [[StringType]]
 */
@ExpressionDescription(
  usage = "_FUNC_(input) - Returns a sha1 hash value as a hex string of the input",
  extended = "> SELECT _FUNC_('Spark');\n '85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c'")
case class Sha1(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.fromString(DigestUtils.sha1Hex(input.asInstanceOf[Array[Byte]]))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c =>
      s"UTF8String.fromString(org.apache.commons.codec.digest.DigestUtils.sha1Hex($c))"
    )
  }
}

/**
 * A function that computes a cyclic redundancy check value and returns it as a bigint
 * For input of type [[BinaryType]]
 */
@ExpressionDescription(
  usage = "_FUNC_(input) - Returns a cyclic redundancy check value as a bigint of the input",
  extended = "> SELECT _FUNC_('Spark');\n '1557323817'")
case class Crc32(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = LongType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(input: Any): Any = {
    val checksum = new CRC32
    checksum.update(input.asInstanceOf[Array[Byte]], 0, input.asInstanceOf[Array[Byte]].length)
    checksum.getValue
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val CRC32 = "java.util.zip.CRC32"
    val checksum = ctx.freshName("checksum")
    nullSafeCodeGen(ctx, ev, value => {
      s"""
        $CRC32 $checksum = new $CRC32();
        $checksum.update($value, 0, $value.length);
        ${ev.value} = $checksum.getValue();
      """
    })
  }
}


/**
 * A function that calculates hash value for a group of expressions.  Note that the `seed` argument
 * is not exposed to users and should only be set inside spark SQL.
 *
 * The hash value for an expression depends on its type and seed:
 *  - null:               seed
 *  - boolean:            turn boolean into int, 1 for true, 0 for false, and then use murmur3 to
 *                        hash this int with seed.
 *  - byte, short, int:   use murmur3 to hash the input as int with seed.
 *  - long:               use murmur3 to hash the long input with seed.
 *  - float:              turn it into int: java.lang.Float.floatToIntBits(input), and hash it.
 *  - double:             turn it into long: java.lang.Double.doubleToLongBits(input), and hash it.
 *  - decimal:            if it's a small decimal, i.e. precision <= 18, turn it into long and hash
 *                        it. Else, turn it into bytes and hash it.
 *  - calendar interval:  hash `microseconds` first, and use the result as seed to hash `months`.
 *  - binary:             use murmur3 to hash the bytes with seed.
 *  - string:             get the bytes of string and hash it.
 *  - array:              The `result` starts with seed, then use `result` as seed, recursively
 *                        calculate hash value for each element, and assign the element hash value
 *                        to `result`.
 *  - map:                The `result` starts with seed, then use `result` as seed, recursively
 *                        calculate hash value for each key-value, and assign the key-value hash
 *                        value to `result`.
 *  - struct:             The `result` starts with seed, then use `result` as seed, recursively
 *                        calculate hash value for each field, and assign the field hash value to
 *                        `result`.
 *
 * Finally we aggregate the hash values for each expression by the same way of struct.
 */
abstract class HashExpression[E] extends Expression {
  /** Seed of the HashExpression. */
  val seed: E

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.isEmpty) {
      TypeCheckResult.TypeCheckFailure("function hash requires at least one argument")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def eval(input: InternalRow): Any = {
    var hash = seed
    var i = 0
    val len = children.length
    while (i < len) {
      hash = computeHash(children(i).eval(input), children(i).dataType, hash)
      i += 1
    }
    hash
  }

  protected def computeHash(value: Any, dataType: DataType, seed: E): E

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.isNull = "false"
    val childrenHash = children.map { child =>
      val childGen = child.genCode(ctx)
      childGen.code + ctx.nullSafeExec(child.nullable, childGen.isNull) {
        computeHash(childGen.value, child.dataType, ev.value, ctx)
      }
    }.mkString("\n")

    ev.copy(code = s"""
      ${ctx.javaType(dataType)} ${ev.value} = $seed;
      $childrenHash""")
  }

  protected def nullSafeElementHash(
      input: String,
      index: String,
      nullable: Boolean,
      elementType: DataType,
      result: String,
      ctx: CodegenContext): String = {
    val element = ctx.freshName("element")

    ctx.nullSafeExec(nullable, s"$input.isNullAt($index)") {
      s"""
        final ${ctx.javaType(elementType)} $element = ${ctx.getValue(input, elementType, index)};
        ${computeHash(element, elementType, result, ctx)}
      """
    }
  }

  protected def genHashInt(i: String, result: String): String =
    s"$result = $hasherClassName.hashInt($i, $result);"

  protected def genHashLong(l: String, result: String): String =
    s"$result = $hasherClassName.hashLong($l, $result);"

  protected def genHashBytes(b: String, result: String): String = {
    val offset = "Platform.BYTE_ARRAY_OFFSET"
    s"$result = $hasherClassName.hashUnsafeBytes($b, $offset, $b.length, $result);"
  }

  protected def genHashBoolean(input: String, result: String): String =
    genHashInt(s"$input ? 1 : 0", result)

  protected def genHashFloat(input: String, result: String): String =
    genHashInt(s"Float.floatToIntBits($input)", result)

  protected def genHashDouble(input: String, result: String): String =
    genHashLong(s"Double.doubleToLongBits($input)", result)

  protected def genHashDecimal(
      ctx: CodegenContext,
      d: DecimalType,
      input: String,
      result: String): String = {
    if (d.precision <= Decimal.MAX_LONG_DIGITS) {
      genHashLong(s"$input.toUnscaledLong()", result)
    } else {
      val bytes = ctx.freshName("bytes")
      s"""
            final byte[] $bytes = $input.toJavaBigDecimal().unscaledValue().toByteArray();
            ${genHashBytes(bytes, result)}
          """
    }
  }

  protected def genHashCalendarInterval(input: String, result: String): String = {
    val microsecondsHash = s"$hasherClassName.hashLong($input.microseconds, $result)"
    s"$result = $hasherClassName.hashInt($input.months, $microsecondsHash);"
  }

  protected def genHashString(input: String, result: String): String = {
    val baseObject = s"$input.getBaseObject()"
    val baseOffset = s"$input.getBaseOffset()"
    val numBytes = s"$input.numBytes()"
    s"$result = $hasherClassName.hashUnsafeBytes($baseObject, $baseOffset, $numBytes, $result);"
  }

  protected def genHashForMap(
      ctx: CodegenContext,
      input: String,
      result: String,
      keyType: DataType,
      valueType: DataType,
      valueContainsNull: Boolean): String = {
    val index = ctx.freshName("index")
    val keys = ctx.freshName("keys")
    val values = ctx.freshName("values")
    s"""
        final ArrayData $keys = $input.keyArray();
        final ArrayData $values = $input.valueArray();
        for (int $index = 0; $index < $input.numElements(); $index++) {
          ${nullSafeElementHash(keys, index, false, keyType, result, ctx)}
          ${nullSafeElementHash(values, index, valueContainsNull, valueType, result, ctx)}
        }
      """
  }

  protected def genHashForArray(
      ctx: CodegenContext,
      input: String,
      result: String,
      elementType: DataType,
      containsNull: Boolean): String = {
    val index = ctx.freshName("index")
    s"""
        for (int $index = 0; $index < $input.numElements(); $index++) {
          ${nullSafeElementHash(input, index, containsNull, elementType, result, ctx)}
        }
      """
  }

  protected def genHashForStruct(
      ctx: CodegenContext,
      input: String,
      result: String,
      fields: Array[StructField]): String = {
    fields.zipWithIndex.map { case (field, index) =>
      nullSafeElementHash(input, index.toString, field.nullable, field.dataType, result, ctx)
    }.mkString("\n")
  }

  @tailrec
  private def computeHashWithTailRec(
      input: String,
      dataType: DataType,
      result: String,
      ctx: CodegenContext): String = dataType match {
    case NullType => ""
    case BooleanType => genHashBoolean(input, result)
    case ByteType | ShortType | IntegerType | DateType => genHashInt(input, result)
    case LongType | TimestampType => genHashLong(input, result)
    case FloatType => genHashFloat(input, result)
    case DoubleType => genHashDouble(input, result)
    case d: DecimalType => genHashDecimal(ctx, d, input, result)
    case CalendarIntervalType => genHashCalendarInterval(input, result)
    case BinaryType => genHashBytes(input, result)
    case StringType => genHashString(input, result)
    case ArrayType(et, containsNull) => genHashForArray(ctx, input, result, et, containsNull)
    case MapType(kt, vt, valueContainsNull) =>
      genHashForMap(ctx, input, result, kt, vt, valueContainsNull)
    case StructType(fields) => genHashForStruct(ctx, input, result, fields)
    case udt: UserDefinedType[_] => computeHashWithTailRec(input, udt.sqlType, result, ctx)
  }

  protected def computeHash(
      input: String,
      dataType: DataType,
      result: String,
      ctx: CodegenContext): String = computeHashWithTailRec(input, dataType, result, ctx)

  protected def hasherClassName: String
}

/**
 * Base class for interpreted hash functions.
 */
abstract class InterpretedHashFunction {
  protected def hashInt(i: Int, seed: Long): Long

  protected def hashLong(l: Long, seed: Long): Long

  protected def hashUnsafeBytes(base: AnyRef, offset: Long, length: Int, seed: Long): Long

  def hash(value: Any, dataType: DataType, seed: Long): Long = {
    value match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case d: Decimal =>
        val precision = dataType.asInstanceOf[DecimalType].precision
        if (precision <= Decimal.MAX_LONG_DIGITS) {
          hashLong(d.toUnscaledLong, seed)
        } else {
          val bytes = d.toJavaBigDecimal.unscaledValue().toByteArray
          hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length, seed)
        }
      case c: CalendarInterval => hashInt(c.months, hashLong(c.microseconds, seed))
      case a: Array[Byte] =>
        hashUnsafeBytes(a, Platform.BYTE_ARRAY_OFFSET, a.length, seed)
      case s: UTF8String =>
        hashUnsafeBytes(s.getBaseObject, s.getBaseOffset, s.numBytes(), seed)

      case array: ArrayData =>
        val elementType = dataType match {
          case udt: UserDefinedType[_] => udt.sqlType.asInstanceOf[ArrayType].elementType
          case ArrayType(et, _) => et
        }
        var result = seed
        var i = 0
        while (i < array.numElements()) {
          result = hash(array.get(i, elementType), elementType, result)
          i += 1
        }
        result

      case map: MapData =>
        val (kt, vt) = dataType match {
          case udt: UserDefinedType[_] =>
            val mapType = udt.sqlType.asInstanceOf[MapType]
            mapType.keyType -> mapType.valueType
          case MapType(kt, vt, _) => kt -> vt
        }
        val keys = map.keyArray()
        val values = map.valueArray()
        var result = seed
        var i = 0
        while (i < map.numElements()) {
          result = hash(keys.get(i, kt), kt, result)
          result = hash(values.get(i, vt), vt, result)
          i += 1
        }
        result

      case struct: InternalRow =>
        val types: Array[DataType] = dataType match {
          case udt: UserDefinedType[_] =>
            udt.sqlType.asInstanceOf[StructType].map(_.dataType).toArray
          case StructType(fields) => fields.map(_.dataType)
        }
        var result = seed
        var i = 0
        val len = struct.numFields
        while (i < len) {
          result = hash(struct.get(i, types(i)), types(i), result)
          i += 1
        }
        result
    }
  }
}

/**
 * A MurMur3 Hash expression.
 *
 * We should use this hash function for both shuffle and bucket, so that we can guarantee shuffle
 * and bucketing have same data distribution.
 */
@ExpressionDescription(
  usage = "_FUNC_(a1, a2, ...) - Returns a hash value of the arguments.")
case class Murmur3Hash(children: Seq[Expression], seed: Int) extends HashExpression[Int] {
  def this(arguments: Seq[Expression]) = this(arguments, 42)

  override def dataType: DataType = IntegerType

  override def prettyName: String = "hash"

  override protected def hasherClassName: String = classOf[Murmur3_x86_32].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Int): Int = {
    Murmur3HashFunction.hash(value, dataType, seed).toInt
  }
}

object Murmur3HashFunction extends InterpretedHashFunction {
  override protected def hashInt(i: Int, seed: Long): Long = {
    Murmur3_x86_32.hashInt(i, seed.toInt)
  }

  override protected def hashLong(l: Long, seed: Long): Long = {
    Murmur3_x86_32.hashLong(l, seed.toInt)
  }

  override protected def hashUnsafeBytes(base: AnyRef, offset: Long, len: Int, seed: Long): Long = {
    Murmur3_x86_32.hashUnsafeBytes(base, offset, len, seed.toInt)
  }
}

/**
 * Print the result of an expression to stderr (used for debugging codegen).
 */
case class PrintToStderr(child: Expression) extends UnaryExpression {

  override def dataType: DataType = child.dataType

  protected override def nullSafeEval(input: Any): Any = input

  private val outputPrefix = s"Result of ${child.simpleString} is "

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val outputPrefixField = ctx.addReferenceObj("outputPrefix", outputPrefix)
    nullSafeCodeGen(ctx, ev, c =>
      s"""
         | System.err.println($outputPrefixField + $c);
         | ${ev.value} = $c;
       """.stripMargin)
  }
}

/**
 * A function throws an exception if 'condition' is not true.
 */
@ExpressionDescription(
  usage = "_FUNC_(condition) - Throw an exception if 'condition' is not true.")
case class AssertTrue(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  override def dataType: DataType = NullType

  override def prettyName: String = "assert_true"

  private val errMsg = s"'${child.simpleString}' is not true!"

  override def eval(input: InternalRow) : Any = {
    val v = child.eval(input)
    if (v == null || java.lang.Boolean.FALSE.equals(v)) {
      throw new RuntimeException(errMsg)
    } else {
      null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)

    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the value is null or false.
    val errMsgField = ctx.addReferenceObj(errMsg)
    ExprCode(code = s"""${eval.code}
       |if (${eval.isNull} || !${eval.value}) {
       |  throw new RuntimeException($errMsgField);
       |}""".stripMargin, isNull = "true", value = "null")
  }

  override def sql: String = s"assert_true(${child.sql})"
}

/**
 * A xxHash64 64-bit hash expression.
 */
case class XxHash64(children: Seq[Expression], seed: Long) extends HashExpression[Long] {
  def this(arguments: Seq[Expression]) = this(arguments, 42L)

  override def dataType: DataType = LongType

  override def prettyName: String = "xxHash"

  override protected def hasherClassName: String = classOf[XXH64].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Long): Long = {
    XxHash64Function.hash(value, dataType, seed)
  }
}

object XxHash64Function extends InterpretedHashFunction {
  override protected def hashInt(i: Int, seed: Long): Long = XXH64.hashInt(i, seed)

  override protected def hashLong(l: Long, seed: Long): Long = XXH64.hashLong(l, seed)

  override protected def hashUnsafeBytes(base: AnyRef, offset: Long, len: Int, seed: Long): Long = {
    XXH64.hashUnsafeBytes(base, offset, len, seed)
  }
}

/**
 * Returns the current database of the SessionCatalog.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current database.",
  extended = "> SELECT _FUNC_()")
case class CurrentDatabase() extends LeafExpression with Unevaluable {
  override def dataType: DataType = StringType
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def prettyName: String = "current_database"
}

/**
 * Simulates Hive's hashing function at
 * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils#hashcode() in Hive
 *
 * We should use this hash function for both shuffle and bucket of Hive tables, so that
 * we can guarantee shuffle and bucketing have same data distribution
 *
 * TODO: Support Decimal and date related types
 */
@ExpressionDescription(
  usage = "_FUNC_(a1, a2, ...) - Returns a hash value of the arguments.")
case class HiveHash(children: Seq[Expression]) extends HashExpression[Int] {
  override val seed = 0

  override def dataType: DataType = IntegerType

  override def prettyName: String = "hive-hash"

  override protected def hasherClassName: String = classOf[HiveHasher].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Int): Int = {
    HiveHashFunction.hash(value, dataType, seed).toInt
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.isNull = "false"
    val childHash = ctx.freshName("childHash")
    val childrenHash = children.map { child =>
      val childGen = child.genCode(ctx)
      childGen.code + ctx.nullSafeExec(child.nullable, childGen.isNull) {
        computeHash(childGen.value, child.dataType, childHash, ctx)
      } + s"${ev.value} = (31 * ${ev.value}) + $childHash;"
    }.mkString(s"int $childHash = 0;", s"\n$childHash = 0;\n", "")

    ev.copy(code = s"""
      ${ctx.javaType(dataType)} ${ev.value} = $seed;
      $childrenHash""")
  }

  override def eval(input: InternalRow): Int = {
    var hash = seed
    var i = 0
    val len = children.length
    while (i < len) {
      hash = (31 * hash) + computeHash(children(i).eval(input), children(i).dataType, hash)
      i += 1
    }
    hash
  }

  override protected def genHashInt(i: String, result: String): String =
    s"$result = $hasherClassName.hashInt($i);"

  override protected def genHashLong(l: String, result: String): String =
    s"$result = $hasherClassName.hashLong($l);"

  override protected def genHashBytes(b: String, result: String): String =
    s"$result = $hasherClassName.hashUnsafeBytes($b, Platform.BYTE_ARRAY_OFFSET, $b.length);"

  override protected def genHashCalendarInterval(input: String, result: String): String = {
    s"""
        $result = (31 * $hasherClassName.hashInt($input.months)) +
          $hasherClassName.hashLong($input.microseconds);"
     """
  }

  override protected def genHashString(input: String, result: String): String = {
    val baseObject = s"$input.getBaseObject()"
    val baseOffset = s"$input.getBaseOffset()"
    val numBytes = s"$input.numBytes()"
    s"$result = $hasherClassName.hashUnsafeBytes($baseObject, $baseOffset, $numBytes);"
  }

  override protected def genHashForArray(
      ctx: CodegenContext,
      input: String,
      result: String,
      elementType: DataType,
      containsNull: Boolean): String = {
    val index = ctx.freshName("index")
    val childResult = ctx.freshName("childResult")
    s"""
        int $childResult = 0;
        for (int $index = 0; $index < $input.numElements(); $index++) {
          $childResult = 0;
          ${nullSafeElementHash(input, index, containsNull, elementType, childResult, ctx)};
          $result = (31 * $result) + $childResult;
        }
      """
  }

  override protected def genHashForMap(
      ctx: CodegenContext,
      input: String,
      result: String,
      keyType: DataType,
      valueType: DataType,
      valueContainsNull: Boolean): String = {
    val index = ctx.freshName("index")
    val keys = ctx.freshName("keys")
    val values = ctx.freshName("values")
    val keyResult = ctx.freshName("keyResult")
    val valueResult = ctx.freshName("valueResult")
    s"""
        final ArrayData $keys = $input.keyArray();
        final ArrayData $values = $input.valueArray();
        int $keyResult = 0;
        int $valueResult = 0;
        for (int $index = 0; $index < $input.numElements(); $index++) {
          $keyResult = 0;
          ${nullSafeElementHash(keys, index, false, keyType, keyResult, ctx)}
          $valueResult = 0;
          ${nullSafeElementHash(values, index, valueContainsNull, valueType, valueResult, ctx)}
          $result += $keyResult ^ $valueResult;
        }
      """
  }

  override protected def genHashForStruct(
      ctx: CodegenContext,
      input: String,
      result: String,
      fields: Array[StructField]): String = {
    val localResult = ctx.freshName("localResult")
    val childResult = ctx.freshName("childResult")
    fields.zipWithIndex.map { case (field, index) =>
      s"""
         $childResult = 0;
         ${nullSafeElementHash(input, index.toString, field.nullable, field.dataType,
           childResult, ctx)}
         $localResult = (31 * $localResult) + $childResult;
       """
    }.mkString(
      s"""
         int $localResult = 0;
         int $childResult = 0;
       """,
      "",
      s"$result = (31 * $result) + $localResult;"
    )
  }
}

object HiveHashFunction extends InterpretedHashFunction {
  override protected def hashInt(i: Int, seed: Long): Long = {
    HiveHasher.hashInt(i)
  }

  override protected def hashLong(l: Long, seed: Long): Long = {
    HiveHasher.hashLong(l)
  }

  override protected def hashUnsafeBytes(base: AnyRef, offset: Long, len: Int, seed: Long): Long = {
    HiveHasher.hashUnsafeBytes(base, offset, len)
  }

  override def hash(value: Any, dataType: DataType, seed: Long): Long = {
    value match {
      case null => 0
      case array: ArrayData =>
        val elementType = dataType match {
          case udt: UserDefinedType[_] => udt.sqlType.asInstanceOf[ArrayType].elementType
          case ArrayType(et, _) => et
        }

        var result = 0
        var i = 0
        val length = array.numElements()
        while (i < length) {
          result = (31 * result) + hash(array.get(i, elementType), elementType, 0).toInt
          i += 1
        }
        result

      case map: MapData =>
        val (kt, vt) = dataType match {
          case udt: UserDefinedType[_] =>
            val mapType = udt.sqlType.asInstanceOf[MapType]
            mapType.keyType -> mapType.valueType
          case MapType(_kt, _vt, _) => _kt -> _vt
        }
        val keys = map.keyArray()
        val values = map.valueArray()

        var result = 0
        var i = 0
        val length = map.numElements()
        while (i < length) {
          result += hash(keys.get(i, kt), kt, 0).toInt ^ hash(values.get(i, vt), vt, 0).toInt
          i += 1
        }
        result

      case struct: InternalRow =>
        val types: Array[DataType] = dataType match {
          case udt: UserDefinedType[_] =>
            udt.sqlType.asInstanceOf[StructType].map(_.dataType).toArray
          case StructType(fields) => fields.map(_.dataType)
        }

        var result = 0
        var i = 0
        val length = struct.numFields
        while (i < length) {
          result = (31 * result) + hash(struct.get(i, types(i)), types(i), seed + 1).toInt
          i += 1
        }
        result

      case _ => super.hash(value, dataType, seed)
    }
  }
}

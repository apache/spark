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

import java.math.{BigDecimal, RoundingMode}
import java.util.concurrent.TimeUnit._
import java.util.zip.CRC32

import scala.annotation.tailrec

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.codec.digest.MessageDigestAlgorithms

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationFactory, MapData}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.ArrayImplicits._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines all the expressions for hashing.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * A function that calculates an MD5 128-bit checksum and returns it as a hex string
 * For input of type [[BinaryType]]
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns an MD5 128-bit checksum as a hex string of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark');
       8cde774d6f7333752ed72cacddb05126
  """,
  since = "1.5.0",
  group = "hash_funcs")
case class Md5(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = SQLConf.get.defaultStringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.fromString(DigestUtils.md5Hex(input.asInstanceOf[Array[Byte]]))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c =>
      s"UTF8String.fromString(${classOf[DigestUtils].getName}.md5Hex($c))")
  }

  override protected def withNewChildInternal(newChild: Expression): Md5 = copy(child = newChild)
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
  usage = """
    _FUNC_(expr, bitLength) - Returns a checksum of SHA-2 family as a hex string of `expr`.
      SHA-224, SHA-256, SHA-384, and SHA-512 are supported. Bit length of 0 is equivalent to 256.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark', 256);
       529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b
  """,
  since = "1.5.0",
  group = "hash_funcs")
// scalastyle:on line.size.limit
case class Sha2(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {

  override def dataType: DataType = SQLConf.get.defaultStringType
  override def nullable: Boolean = true

  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val bitLength = input2.asInstanceOf[Int]
    val input = input1.asInstanceOf[Array[Byte]]
    bitLength match {
      case 224 =>
        UTF8String.fromString(
          new DigestUtils(MessageDigestAlgorithms.SHA_224).digestAsHex(input))
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
    val digestUtils = classOf[DigestUtils].getName
    val messageDigestAlgorithms = classOf[MessageDigestAlgorithms].getName
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"""
        if ($eval2 == 224) {
          ${ev.value} = UTF8String.fromString(
                          new $digestUtils($messageDigestAlgorithms.SHA_224).digestAsHex($eval1));
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

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Sha2 =
    copy(left = newLeft, right = newRight)
}

/**
 * A function that calculates a sha1 hash value and returns it as a hex string
 * For input of type [[BinaryType]] or [[StringType]]
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns a sha1 hash value as a hex string of the `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark');
       85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c
  """,
  since = "1.5.0",
  group = "hash_funcs")
case class Sha1(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = SQLConf.get.defaultStringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.fromString(DigestUtils.sha1Hex(input.asInstanceOf[Array[Byte]]))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c =>
      s"UTF8String.fromString(${classOf[DigestUtils].getName}.sha1Hex($c))"
    )
  }

  override protected def withNewChildInternal(newChild: Expression): Sha1 = copy(child = newChild)
}

/**
 * A function that computes a cyclic redundancy check value and returns it as a bigint
 * For input of type [[BinaryType]]
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns a cyclic redundancy check value of the `expr` as a bigint.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark');
       1557323817
  """,
  since = "1.5.0",
  group = "hash_funcs")
case class Crc32(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

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

  override protected def withNewChildInternal(newChild: Expression): Crc32 = copy(child = newChild)
}


/**
 * A function that calculates hash value for a group of expressions.  Note that the `seed` argument
 * is not exposed to users and should only be set inside spark SQL.
 *
 * The hash value for an expression depends on its type and seed:
 *  - null:                    seed
 *  - boolean:                 turn boolean into int, 1 for true, 0 for false,
 *                             and then use murmur3 to hash this int with seed.
 *  - byte, short, int:        use murmur3 to hash the input as int with seed.
 *  - long:                    use murmur3 to hash the long input with seed.
 *  - float:                   turn it into int: java.lang.Float.floatToIntBits(input), and hash it.
 *  - double:                  turn it into long: java.lang.Double.doubleToLongBits(input),
 *                             and hash it.
 *  - decimal:                 if it's a small decimal, i.e. precision <= 18, turn it into long
 *                             and hash it. Else, turn it into bytes and hash it.
 *  - calendar interval:       hash `microseconds` first, and use the result as seed
 *                             to hash `months`.
 *  - interval day to second:  it store long value of `microseconds`, use murmur3 to hash the long
 *                             input with seed.
 *  - interval year to month:  it store int value of `months`, use murmur3 to hash the int
 *                             input with seed.
 *  - binary:                  use murmur3 to hash the bytes with seed.
 *  - string:                  get the bytes of string and hash it.
 *  - array:                   The `result` starts with seed, then use `result` as seed, recursively
 *                             calculate hash value for each element, and assign the element hash
 *                             value to `result`.
 *  - struct:                  The `result` starts with seed, then use `result` as seed, recursively
 *                             calculate hash value for each field, and assign the field hash value
 *                             to `result`.
 *
 * Finally we aggregate the hash values for each expression by the same way of struct.
 */
abstract class HashExpression[E] extends Expression {
  /** Seed of the HashExpression. */
  val seed: E

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = false

  private def hasMapType(dt: DataType): Boolean = {
    dt.existsRecursively(_.isInstanceOf[MapType])
  }

  private def hasVariantType(dt: DataType): Boolean = {
    dt.existsRecursively(_.isInstanceOf[VariantType])
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length < 1) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq("> 0"), children.length
      )
    } else if (children.exists(child => hasMapType(child.dataType)) &&
        !SQLConf.get.getConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE)) {
      DataTypeMismatch(
        errorSubClass = "HASH_MAP_TYPE",
        messageParameters = Map("functionName" -> toSQLId(prettyName)))
    } else if (children.exists(child => hasVariantType(child.dataType))) {
      DataTypeMismatch(
        errorSubClass = "HASH_VARIANT_TYPE",
        messageParameters = Map("functionName" -> toSQLId(prettyName)))
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def eval(input: InternalRow = null): Any = {
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
    ev.isNull = FalseLiteral

    val childrenHash = children.map { child =>
      val childGen = child.genCode(ctx)
      childGen.code.toString + ctx.nullSafeExec(child.nullable, childGen.isNull) {
        computeHash(childGen.value, child.dataType, ev.value, ctx)
      }
    }

    val hashResultType = CodeGenerator.javaType(dataType)
    val typedSeed = if (DataTypeUtils.sameType(dataType, LongType)) s"${seed}L" else s"$seed"
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = childrenHash,
      funcName = "computeHash",
      extraArguments = Seq(hashResultType -> ev.value),
      returnType = hashResultType,
      makeSplitFunction = body =>
        s"""
           |$body
           |return ${ev.value};
         """.stripMargin,
      foldFunctions = _.map(funcCall => s"${ev.value} = $funcCall;").mkString("\n"))

    ev.copy(code =
      code"""
         |$hashResultType ${ev.value} = $typedSeed;
         |$codes
       """.stripMargin)
  }

  protected def nullSafeElementHash(
      input: String,
      index: String,
      nullable: Boolean,
      elementType: DataType,
      result: String,
      ctx: CodegenContext): String = {
    val element = ctx.freshName("element")

    val jt = CodeGenerator.javaType(elementType)
    ctx.nullSafeExec(nullable, s"$input.isNullAt($index)") {
      s"""
        final $jt $element = ${CodeGenerator.getValue(input, elementType, index)};
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

  protected def genHashFloat(input: String, result: String): String = {
    s"""
       |if($input == -0.0f) {
       |  ${genHashInt("0", result)}
       |} else {
       |  ${genHashInt(s"Float.floatToIntBits($input)", result)}
       |}
     """.stripMargin
  }

  protected def genHashDouble(input: String, result: String): String = {
    s"""
      |if($input == -0.0d) {
      |  ${genHashLong("0L", result)}
      |} else {
      |  ${genHashLong(s"Double.doubleToLongBits($input)", result)}
      |}
     """.stripMargin
  }

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
         |final byte[] $bytes = $input.toJavaBigDecimal().unscaledValue().toByteArray();
         |${genHashBytes(bytes, result)}
       """.stripMargin
    }
  }

  protected def genHashTimestamp(t: String, result: String): String = genHashLong(t, result)

  protected def genHashCalendarInterval(input: String, result: String): String = {
    val microsecondsHash = s"$hasherClassName.hashLong($input.microseconds, $result)"
    s"$result = $hasherClassName.hashInt($input.months, $microsecondsHash);"
  }

  protected def genHashString(
      ctx: CodegenContext, stringType: StringType, input: String, result: String): String = {
    if (stringType.supportsBinaryEquality && !stringType.usesTrimCollation) {
      val baseObject = s"$input.getBaseObject()"
      val baseOffset = s"$input.getBaseOffset()"
      val numBytes = s"$input.numBytes()"
      s"$result = $hasherClassName.hashUnsafeBytes($baseObject, $baseOffset, $numBytes, $result);"
    } else {
      val stringHash = ctx.freshName("stringHash")
      s"""
        long $stringHash = CollationFactory.fetchCollation(${stringType.collationId})
          .hashFunction.applyAsLong($input);
        $result = $hasherClassName.hashLong($stringHash, $result);
      """
    }

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
    val tmpInput = ctx.freshName("input")
    val fieldsHash = fields.zipWithIndex.map { case (field, index) =>
      nullSafeElementHash(tmpInput, index.toString, field.nullable, field.dataType, result, ctx)
    }
    val hashResultType = CodeGenerator.javaType(dataType)
    val code = ctx.splitExpressions(
      expressions = fieldsHash.toImmutableArraySeq,
      funcName = "computeHashForStruct",
      arguments = Seq("InternalRow" -> tmpInput, hashResultType -> result),
      returnType = hashResultType,
      makeSplitFunction = body =>
        s"""
           |$body
           |return $result;
         """.stripMargin,
      foldFunctions = _.map(funcCall => s"$result = $funcCall;").mkString("\n"))
    s"""
       |final InternalRow $tmpInput = $input;
       |$code
     """.stripMargin
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
    case LongType => genHashLong(input, result)
    case TimestampType | TimestampNTZType => genHashTimestamp(input, result)
    case FloatType => genHashFloat(input, result)
    case DoubleType => genHashDouble(input, result)
    case d: DecimalType => genHashDecimal(ctx, d, input, result)
    case CalendarIntervalType => genHashCalendarInterval(input, result)
    case _: DayTimeIntervalType => genHashLong(input, result)
    case _: YearMonthIntervalType => genHashInt(input, result)
    case BinaryType => genHashBytes(input, result)
    case st: StringType => genHashString(ctx, st, input, result)
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

  /**
   * Computes hash of a given `value` of type `dataType`. The caller needs to check the validity
   * of input `value`.
   */
  def hash(value: Any, dataType: DataType, seed: Long): Long = {
    value match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float if (f == -0.0f) => hashInt(0, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double if (d == -0.0d) => hashLong(0L, seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case d: Decimal =>
        val precision = dataType.asInstanceOf[DecimalType].precision
        if (precision <= Decimal.MAX_LONG_DIGITS) {
          hashLong(d.toUnscaledLong, seed)
        } else {
          val bytes = d.toJavaBigDecimal.unscaledValue().toByteArray
          hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length, seed)
        }
      case c: CalendarInterval => hashInt(c.months, hashInt(c.days, hashLong(c.microseconds, seed)))
      case a: Array[Byte] =>
        hashUnsafeBytes(a, Platform.BYTE_ARRAY_OFFSET, a.length, seed)
      case s: UTF8String =>
        val st = dataType.asInstanceOf[StringType]
        if (st.supportsBinaryEquality && !st.usesTrimCollation) {
          hashUnsafeBytes(s.getBaseObject, s.getBaseOffset, s.numBytes(), seed)
        } else {
          val stringHash = CollationFactory
            .fetchCollation(st.collationId)
            .hashFunction.applyAsLong(s)
          hashLong(stringHash, seed)
        }

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
  usage = "_FUNC_(expr1, expr2, ...) - Returns a hash value of the arguments.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark', array(123), 2);
       -1321691492
  """,
  since = "2.0.0",
  group = "hash_funcs")
case class Murmur3Hash(children: Seq[Expression], seed: Int) extends HashExpression[Int] {
  def this(arguments: Seq[Expression]) = this(arguments, 42)

  override def dataType: DataType = IntegerType

  override def prettyName: String = "hash"

  override protected def hasherClassName: String = classOf[Murmur3_x86_32].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Int): Int = {
    Murmur3HashFunction.hash(value, dataType, seed).toInt
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Murmur3Hash =
    copy(children = newChildren)
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
 * A xxHash64 64-bit hash expression.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, ...) - Returns a 64-bit hash value of the arguments. " +
    "Hash seed is 42.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark', array(123), 2);
       5602566077635097486
  """,
  since = "3.0.0",
  group = "hash_funcs")
case class XxHash64(children: Seq[Expression], seed: Long) extends HashExpression[Long] {
  def this(arguments: Seq[Expression]) = this(arguments, 42L)

  override def dataType: DataType = LongType

  override def prettyName: String = "xxhash64"

  override protected def hasherClassName: String = classOf[XXH64].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Long): Long = {
    XxHash64Function.hash(value, dataType, seed)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): XxHash64 =
    copy(children = newChildren)
}

object XxHash64Function extends InterpretedHashFunction {
  override protected def hashInt(i: Int, seed: Long): Long = XXH64.hashInt(i, seed)

  override protected def hashLong(l: Long, seed: Long): Long = XXH64.hashLong(l, seed)

  override protected def hashUnsafeBytes(base: AnyRef, offset: Long, len: Int, seed: Long): Long = {
    XXH64.hashUnsafeBytes(base, offset, len, seed)
  }
}

/**
 * Simulates Hive's hashing function from Hive v1.2.1 at
 * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils#hashcode()
 *
 * We should use this hash function for both shuffle and bucket of Hive tables, so that
 * we can guarantee shuffle and bucketing have same data distribution
 */
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, ...) - Returns a hash value of the arguments.",
  since = "2.2.0",
  group = "hash_funcs")
case class HiveHash(children: Seq[Expression]) extends HashExpression[Int] {
  override val seed = 0

  override def dataType: DataType = IntegerType

  override def prettyName: String = "hive-hash"

  override protected def hasherClassName: String = classOf[HiveHasher].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Int): Int = {
    HiveHashFunction.hash(value, dataType, this.seed).toInt
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.isNull = FalseLiteral

    val childHash = ctx.freshName("childHash")
    val childrenHash = children.map { child =>
      val childGen = child.genCode(ctx)
      val codeToComputeHash = ctx.nullSafeExec(child.nullable, childGen.isNull) {
        computeHash(childGen.value, child.dataType, childHash, ctx)
      }
      s"""
         |${childGen.code}
         |$childHash = 0;
         |$codeToComputeHash
         |${ev.value} = (31 * ${ev.value}) + $childHash;
       """.stripMargin
    }

    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = childrenHash,
      funcName = "computeHash",
      extraArguments = Seq(CodeGenerator.JAVA_INT -> ev.value),
      returnType = CodeGenerator.JAVA_INT,
      makeSplitFunction = body =>
        s"""
           |${CodeGenerator.JAVA_INT} $childHash = 0;
           |$body
           |return ${ev.value};
         """.stripMargin,
      foldFunctions = _.map(funcCall => s"${ev.value} = $funcCall;").mkString("\n"))


    ev.copy(code =
      code"""
         |${CodeGenerator.JAVA_INT} ${ev.value} = $seed;
         |${CodeGenerator.JAVA_INT} $childHash = 0;
         |$codes
       """.stripMargin)
  }

  override def eval(input: InternalRow = null): Int = {
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

  override protected def genHashDecimal(
      ctx: CodegenContext,
      d: DecimalType,
      input: String,
      result: String): String = {
    s"""
      $result = ${HiveHashFunction.getClass.getName.stripSuffix("$")}.normalizeDecimal(
        $input.toJavaBigDecimal()).hashCode();"""
  }

  override protected def genHashCalendarInterval(input: String, result: String): String = {
    s"""
      $result = (int)
        ${HiveHashFunction.getClass.getName.stripSuffix("$")}.hashCalendarInterval($input);
     """
  }

  override protected def genHashTimestamp(input: String, result: String): String =
    s"""
      $result = (int) ${HiveHashFunction.getClass.getName.stripSuffix("$")}.hashTimestamp($input);
     """

  override protected def genHashString(
      ctx: CodegenContext, stringType: StringType, input: String, result: String): String = {
    if (stringType.supportsBinaryEquality && !stringType.usesTrimCollation) {
      val baseObject = s"$input.getBaseObject()"
      val baseOffset = s"$input.getBaseOffset()"
      val numBytes = s"$input.numBytes()"
      s"$result = $hasherClassName.hashUnsafeBytes($baseObject, $baseOffset, $numBytes);"
    } else {
      val stringHash = ctx.freshName("stringHash")
      s"""
        long $stringHash = CollationFactory.fetchCollation(${stringType.collationId})
          .hashFunction.applyAsLong($input);
        $result = $hasherClassName.hashLong($stringHash);
      """
    }
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
    val tmpInput = ctx.freshName("input")
    val childResult = ctx.freshName("childResult")
    val fieldsHash = fields.zipWithIndex.map { case (field, index) =>
      val computeFieldHash = nullSafeElementHash(
        tmpInput, index.toString, field.nullable, field.dataType, childResult, ctx)
      s"""
         |$childResult = 0;
         |$computeFieldHash
         |$result = (31 * $result) + $childResult;
       """.stripMargin
    }

    val code = ctx.splitExpressions(
      expressions = fieldsHash.toImmutableArraySeq,
      funcName = "computeHashForStruct",
      arguments = Seq("InternalRow" -> tmpInput, CodeGenerator.JAVA_INT -> result),
      returnType = CodeGenerator.JAVA_INT,
      makeSplitFunction = body =>
        s"""
           |${CodeGenerator.JAVA_INT} $childResult = 0;
           |$body
           |return $result;
           """.stripMargin,
      foldFunctions = _.map(funcCall => s"$result = $funcCall;").mkString("\n"))
    s"""
       |final InternalRow $tmpInput = $input;
       |${CodeGenerator.JAVA_INT} $childResult = 0;
       |$code
     """.stripMargin
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): HiveHash =
    copy(children = newChildren)
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

  private val HIVE_DECIMAL_MAX_PRECISION = 38
  private val HIVE_DECIMAL_MAX_SCALE = 38

  // Mimics normalization done for decimals in Hive at HiveDecimalV1.normalize()
  def normalizeDecimal(input: BigDecimal): BigDecimal = {
    if (input == null) return null

    def trimDecimal(input: BigDecimal) = {
      var result = input
      if (result.compareTo(BigDecimal.ZERO) == 0) {
        // Special case for 0, because java doesn't strip zeros correctly on that number.
        result = BigDecimal.ZERO
      } else {
        result = result.stripTrailingZeros
        if (result.scale < 0) {
          // no negative scale decimals
          result = result.setScale(0)
        }
      }
      result
    }

    var result = trimDecimal(input)
    val intDigits = result.precision - result.scale
    if (intDigits > HIVE_DECIMAL_MAX_PRECISION) {
      return null
    }

    val maxScale = Math.min(HIVE_DECIMAL_MAX_SCALE,
      Math.min(HIVE_DECIMAL_MAX_PRECISION - intDigits, result.scale))
    if (result.scale > maxScale) {
      result = result.setScale(maxScale, RoundingMode.HALF_UP)
      // Trimming is again necessary, because rounding may introduce new trailing 0's.
      result = trimDecimal(result)
    }
    result
  }

  /**
   * Mimics TimestampWritable.hashCode() in Hive
   */
  def hashTimestamp(timestamp: Long): Long = {
    val timestampInSeconds = MICROSECONDS.toSeconds(timestamp)
    val nanoSecondsPortion = (timestamp % MICROS_PER_SECOND) * NANOS_PER_MICROS

    var result = timestampInSeconds
    result <<= 30 // the nanosecond part fits in 30 bits
    result |= nanoSecondsPortion
    ((result >>> 32) ^ result).toInt
  }

  /**
   * Hive allows input intervals to be defined using units below but the intervals
   * have to be from the same category:
   * - year, month (stored as HiveIntervalYearMonth)
   * - day, hour, minute, second, nanosecond (stored as HiveIntervalDayTime)
   *
   * e.g. (INTERVAL '30' YEAR + INTERVAL '-23' DAY) fails in Hive
   *
   * This method mimics HiveIntervalDayTime.hashCode() in Hive.
   *
   * Two differences wrt Hive due to how intervals are stored in Spark vs Hive:
   *
   * - If the `INTERVAL` is backed as HiveIntervalYearMonth in Hive, then this method will not
   *   produce Hive compatible result. The reason being Spark's representation of calendar does not
   *   have such categories based on the interval and is unified.
   *
   * - Spark's [[CalendarInterval]] has precision upto microseconds but Hive's
   *   HiveIntervalDayTime can store data with precision upto nanoseconds. So, any input intervals
   *   with nanosecond values will lead to wrong output hashes (i.e. non adherent with Hive output)
   */
  def hashCalendarInterval(calendarInterval: CalendarInterval): Long = {
    val totalMicroSeconds = calendarInterval.days * MICROS_PER_DAY + calendarInterval.microseconds
    val totalSeconds = totalMicroSeconds / MICROS_PER_SECOND.toInt
    val result: Int = (17 * 37) + (totalSeconds ^ totalSeconds >> 32).toInt

    val nanoSeconds = (totalMicroSeconds - (totalSeconds * MICROS_PER_SECOND.toInt)).toInt * 1000
     (result * 37) + nanoSeconds
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
          result = (31 * result) + hash(struct.get(i, types(i)), types(i), 0).toInt
          i += 1
        }
        result

      case d: Decimal => normalizeDecimal(d.toJavaBigDecimal).hashCode()
      case timestamp: Long if dataType.isInstanceOf[TimestampType] => hashTimestamp(timestamp)
      case calendarInterval: CalendarInterval => hashCalendarInterval(calendarInterval)
      case _ => super.hash(value, dataType, 0)
    }
  }
}

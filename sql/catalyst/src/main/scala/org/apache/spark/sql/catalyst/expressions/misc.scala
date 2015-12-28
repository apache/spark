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

import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{MapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
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
@ExpressionDescription(
  usage = "_FUNC_(input, bitLength) - Returns a checksum of SHA-2 family as a hex string of the " +
    "input. SHA-224, SHA-256, SHA-384, and SHA-512 are supported. Bit length of 0 is equivalent " +
    "to 256",
  extended = "> SELECT _FUNC_('Spark', 0);\n " +
    "'529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b'")
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
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
    UTF8String.fromString(DigestUtils.shaHex(input.asInstanceOf[Array[Byte]]))

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c =>
      s"UTF8String.fromString(org.apache.commons.codec.digest.DigestUtils.shaHex($c))"
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val CRC32 = "java.util.zip.CRC32"
    nullSafeCodeGen(ctx, ev, value => {
      s"""
        $CRC32 checksum = new $CRC32();
        checksum.update($value, 0, $value.length);
        ${ev.value} = checksum.getValue();
      """
    })
  }
}

/**
 * A function that calculates hash value for a group of expressions.
 *
 * The hash value for an expression depends on its type:
 *  - null:               0
 *  - boolean:            1 for true, 0 for false.
 *  - byte, short, int:   the input itself.
 *  - long:               input XOR (input >>> 32)
 *  - float:              java.lang.Float.floatToIntBits(input)
 *  - double:             l = java.lang.Double.doubleToLongBits(input); l XOR (l >>> 32)
 *  - binary:             java.util.Arrays.hashCode(input)
 *  - array:              recursively calculate hash value for each element, and aggregate them by
 *                        `result = result * 31 + elementHash` with an initial value `result = 0`.
 *  - map:                recursively calculate hash value for each key-value pair, and aggregate
 *                        them by `result += keyHash XOR valueHash`.
 *  - struct:             similar to array, calculate hash value for each field and aggregate them.
 *  - other type:         input.hashCode().
 *                        e.g. calculate hash value for string type by `UTF8String.hashCode()`.
 * Finally we aggregate the hash values for each expression by `result = result * 31 + exprHash`.
 *
 * This hash algorithm follows hive's bucketing hash function, so that our bucketing function can
 * be compatible with hive's, e.g. we can benefit from bucketing even the data source is mixed with
 * hive tables.
 */
case class Hash(children: Seq[Expression]) extends Expression {

  override def dataType: DataType = IntegerType

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    var result = 0
    for (e <- children) {
      val hashValue = computeHash(e.eval(input), e.dataType)
      result = result * 31 + hashValue
    }
    result
  }

  private def computeHash(v: Any, dataType: DataType): Int = v match {
    case null => 0
    case b: Boolean => if (b) 1 else 0
    case b: Byte => b.toInt
    case s: Short => s.toInt
    case i: Int => i
    case l: Long => (l ^ (l >>> 32)).toInt
    case f: Float => java.lang.Float.floatToIntBits(f)
    case d: Double =>
      val b = java.lang.Double.doubleToLongBits(d)
      (b ^ (b >>> 32)).toInt
    case a: Array[Byte] => java.util.Arrays.hashCode(a)

    case array: ArrayData =>
      val elementType = dataType.asInstanceOf[ArrayType].elementType
      var result = 0
      var i = 0
      while (i < array.numElements()) {
        val hashValue = computeHash(array.get(i, elementType), elementType)
        result = result * 31 + hashValue
        i += 1
      }
      result

    case map: MapData =>
      val mapType = dataType.asInstanceOf[MapType]
      val keys = map.keyArray()
      val values = map.valueArray()
      var result = 0
      var i = 0
      while (i < map.numElements()) {
        val keyHash = computeHash(keys.get(i, mapType.keyType), mapType.keyType)
        val valueHash = computeHash(values.get(i, mapType.valueType), mapType.valueType)
        result += keyHash ^ valueHash
        i += 1
      }
      result

    case row: InternalRow =>
      val fieldTypes = dataType.asInstanceOf[StructType].map(_.dataType)
      var result = 0
      var i = 0
      while (i < row.numFields) {
        val hashValue = computeHash(row.get(i, fieldTypes(i)), fieldTypes(i))
        result = result * 31 + hashValue
        i += 1
      }
      result

    case other => other.hashCode()
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val expressions = children.map(_.gen(ctx))

    val updateHashResult = expressions.zip(children.map(_.dataType)).map { case (expr, dataType) =>
      val hash = computeHash(expr.value, dataType, ctx)
      s"""
        if (${expr.isNull}) {
          ${ev.value} *= 31;
        } else {
          ${hash.code}
          ${ev.value} = ${ev.value} * 31 + ${hash.value};
        }
      """
    }.mkString("\n")

    s"""
      ${expressions.map(_.code).mkString("\n")}
      final boolean ${ev.isNull} = false;
      int ${ev.value} = 0;
      $updateHashResult
    """
  }

  private def computeHash(
      input: String,
      dataType: DataType,
      ctx: CodeGenContext): GeneratedExpressionCode = {
    def simpleHashValue(v: String) = GeneratedExpressionCode(code = "", isNull = "false", value = v)

    dataType match {
      case NullType => simpleHashValue("0")
      case BooleanType => simpleHashValue(s"($input ? 1 : 0)")
      case ByteType | ShortType | IntegerType | DateType => simpleHashValue(input)
      case LongType | TimestampType => simpleHashValue(s"(int) ($input ^ ($input >>> 32))")
      case FloatType => simpleHashValue(s"Float.floatToIntBits($input)")
      case DoubleType =>
        val longBits = ctx.freshName("longBits")
        GeneratedExpressionCode(
          code = s"final long $longBits = Double.doubleToLongBits($input);",
          isNull = "false",
          value = s"(int) ($longBits ^ ($longBits >>> 32))"
        )
      case BinaryType => simpleHashValue(s"java.util.Arrays.hashCode($input)")

      case ArrayType(et, _) =>
        val arrayHash = ctx.freshName("arrayHash")
        val index = ctx.freshName("index")
        val element = ctx.freshName("element")
        val hash = computeHash(element, et, ctx)
        val code = s"""
          int $arrayHash = 0;
          for (int $index = 0; $index < $input.numElements(); $index++) {
            if ($input.isNullAt($index)) {
              $arrayHash *= 31;
            } else {
              final ${ctx.javaType(et)} $element = ${ctx.getValue(input, et, index)};
              ${hash.code}
              $arrayHash = $arrayHash * 31 + ${hash.value};
            }
          }
        """
        GeneratedExpressionCode(code = code, isNull = "false", value = arrayHash)

      case MapType(kt, vt, _) =>
        val mapHash = ctx.freshName("mapHash")

        val keys = ctx.freshName("keys")
        val key = ctx.freshName("key")
        val keyHash = computeHash(key, kt, ctx)

        val values = ctx.freshName("values")
        val value = ctx.freshName("value")
        val valueHash = computeHash(value, vt, ctx)

        val index = ctx.freshName("index")

        val code = s"""
          final ArrayData $keys = $input.keyArray();
          final ArrayData $values = $input.valueArray();
          int $mapHash = 0;
          for (int $index = 0; $index < $input.numElements(); $index++) {
            final ${ctx.javaType(kt)} $key = ${ctx.getValue(keys, kt, index)};
            ${keyHash.code}
            if ($values.isNullAt($index)) {
              $mapHash += ${keyHash.value};
            } else {
              final ${ctx.javaType(vt)} $value = ${ctx.getValue(values, vt, index)};
              ${valueHash.code}
              $mapHash += ${keyHash.value} ^ ${valueHash.value};
            }
          }
        """
        GeneratedExpressionCode(code = code, isNull = "false", value = mapHash)

      case StructType(fields) =>
        val structHash = ctx.freshName("structHash")

        val updateHashResult = fields.zipWithIndex.map { case (f, i) =>
          val jt = ctx.javaType(f.dataType)
          val fieldValue = ctx.freshName(f.name)
          val fieldHash = computeHash(fieldValue, f.dataType, ctx)
          s"""
            if ($input.isNullAt($i)) {
              $structHash *= 31;
            } else {
              final $jt $fieldValue = ${ctx.getValue(input, f.dataType, i.toString)};
              ${fieldHash.code}
              $structHash = $structHash * 31 + ${fieldHash.value};
            }
          """
        }.mkString("\n")

        val code = s"""
          int $structHash = 0;
          $updateHashResult
        """
        GeneratedExpressionCode(code = code, isNull = "false", value = structHash)

      case other => simpleHashValue(s"$input.hashCode()")
    }
  }
}

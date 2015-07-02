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

import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.zip.CRC32

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A function that calculates an MD5 128-bit checksum and returns it as a hex string
 * For input of type [[BinaryType]]
 */
case class Md5(child: Expression)
  extends UnaryExpression with AutoCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      UTF8String.fromString(DigestUtils.md5Hex(value.asInstanceOf[Array[Byte]]))
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c =>
      s"${ctx.stringType}.fromString(org.apache.commons.codec.digest.DigestUtils.md5Hex($c))")
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
case class Sha2(left: Expression, right: Expression)
  extends BinaryExpression with Serializable with AutoCastInputTypes {

  override def dataType: DataType = StringType

  override def toString: String = s"SHA2($left, $right)"

  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType)

  override def eval(input: InternalRow): Any = {
    val evalE1 = left.eval(input)
    if (evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        val bitLength = evalE2.asInstanceOf[Int]
        val input = evalE1.asInstanceOf[Array[Byte]]
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
    }
  }
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val digestUtils = "org.apache.commons.codec.digest.DigestUtils"

    s"""
      ${eval1.code}
      boolean ${ev.isNull} = ${eval1.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${eval2.code}
        if (!${eval2.isNull}) {
          if (${eval2.primitive} == 224) {
            try {
              java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-224");
              md.update(${eval1.primitive});
              ${ev.primitive} = ${ctx.stringType}.fromBytes(md.digest());
            } catch (java.security.NoSuchAlgorithmException e) {
              ${ev.isNull} = true;
            }
          } else if (${eval2.primitive} == 256 || ${eval2.primitive} == 0) {
            ${ev.primitive} =
              ${ctx.stringType}.fromString(${digestUtils}.sha256Hex(${eval1.primitive}));
          } else if (${eval2.primitive} == 384) {
            ${ev.primitive} =
              ${ctx.stringType}.fromString(${digestUtils}.sha384Hex(${eval1.primitive}));
          } else if (${eval2.primitive} == 512) {
            ${ev.primitive} =
              ${ctx.stringType}.fromString(${digestUtils}.sha512Hex(${eval1.primitive}));
          } else {
            ${ev.isNull} = true;
          }
        } else {
          ${ev.isNull} = true;
        }
      }
    """
  }
}

/**
 * A function that calculates a sha1 hash value and returns it as a hex string
 * For input of type [[BinaryType]] or [[StringType]]
 */
case class Sha1(child: Expression) extends UnaryExpression with AutoCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      UTF8String.fromString(DigestUtils.shaHex(value.asInstanceOf[Array[Byte]]))
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c =>
      "org.apache.spark.unsafe.types.UTF8String.fromString" +
        s"(org.apache.commons.codec.digest.DigestUtils.shaHex($c))"
    )
  }
}

/**
 * A function that computes a cyclic redundancy check value and returns it as a bigint
 * For input of type [[BinaryType]]
 */
case class Crc32(child: Expression)
  extends UnaryExpression with AutoCastInputTypes {

  override def dataType: DataType = LongType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      val checksum = new CRC32
      checksum.update(value.asInstanceOf[Array[Byte]], 0, value.asInstanceOf[Array[Byte]].length)
      checksum.getValue
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val value = child.gen(ctx)
    val CRC32 = "java.util.zip.CRC32"
    s"""
      ${value.code}
      boolean ${ev.isNull} = ${value.isNull};
      long ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${CRC32} checksum = new ${CRC32}();
        checksum.update(${value.primitive}, 0, ${value.primitive}.length);
        ${ev.primitive} = checksum.getValue();
      }
    """
  }

}

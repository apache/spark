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
  extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def expectedChildTypes: Seq[DataType] = Seq(BinaryType)

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
      "org.apache.spark.unsafe.types.UTF8String.fromString" +
        s"(org.apache.commons.codec.digest.DigestUtils.md5Hex($c))")
  }
}

/**
 * A function that calculates a sha1 hash value and returns it as a hex string
 * For input of type [[BinaryType]] or [[StringType]]
 */
case class Sha1(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def expectedChildTypes: Seq[DataType] = Seq(BinaryType, StringType)

  override def checkInputDataTypes(): TypeCheckResult =
    if (child.dataType.isInstanceOf[BinaryType] || child.dataType == NullType) {
      TypeCheckResult.TypeCheckSuccess
    } else if (child.dataType.isInstanceOf[StringType]) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"Sha1 accepts string or binary, not ${child.dataType}")
    }

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      value match {
        case b: Array[Byte] =>
          UTF8String.fromString(DigestUtils.sha1Hex(value.asInstanceOf[Array[Byte]]))
        case s: UTF8String =>
          UTF8String.fromString(DigestUtils.sha1Hex(s.getBytes))
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c =>
      child.dataType match {
        case BinaryType =>
          "org.apache.spark.unsafe.types.UTF8String.fromString" +
            s"(org.apache.commons.codec.digest.DigestUtils.sha1Hex($c))"
        case StringType =>
          "org.apache.spark.unsafe.types.UTF8String.fromString" +
            s"(org.apache.commons.codec.digest.DigestUtils.sha1Hex($c.getBytes()))"
      })
  }
}

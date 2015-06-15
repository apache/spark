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

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.types.{BinaryType, StringType, DataType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A function that calculates an MD5 128-bit checksum and returns it as a hex string
 * For input of type [[StringType]] or [[BinaryType]]
 */
case class Md5(child: Expression) extends UnaryExpression {

  override def dataType: DataType = StringType

  override def checkInputDataTypes(): TypeCheckResult =
    if (child.dataType == StringType || child.dataType == BinaryType) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"types error in ${this.getClass.getSimpleName} " +
          s"get (${child.dataType}, expect StringType or BinaryType).")
    }

  override def children: Seq[Expression] = child :: Nil

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else if (child.dataType == BinaryType) {
      UTF8String.fromString(DigestUtils.md5Hex(value.asInstanceOf[Array[Byte]]))
    } else {
      UTF8String.fromString(DigestUtils.md5Hex(value.asInstanceOf[UTF8String].getBytes))
    }
  }

  override def toString: String = s"MD5($child)"
}

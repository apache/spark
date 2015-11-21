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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class MiscFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("md5") {
    checkEvaluation(Md5(Literal("ABC".getBytes)), "902fbdd2b1df0c4f70b4a5d23525e932")
    checkEvaluation(Md5(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      "6ac1e56bc78f031059be7be854522c4c")
    checkEvaluation(Md5(Literal.create(null, BinaryType)), null)
    checkConsistencyBetweenInterpretedAndCodegen(Md5, BinaryType)
  }

  test("hash") {
    def projection(exprs: Expression*): UnsafeProjection =
      GenerateUnsafeProjection.generate(exprs)
    def getHashCode(inputs: Expression*): Int = projection(inputs: _*)(null).hashCode

    checkEvaluation(Hash(Literal(3)), getHashCode(Literal(3)))
    checkEvaluation(Hash(Literal(3L)), getHashCode(Literal(3L)))
    checkEvaluation(Hash(Literal(3.7d)), getHashCode(Literal(3.7d)))
    checkEvaluation(Hash(Literal(3.7f)), getHashCode(Literal(3.7f)))
    val v1: Byte = 3
    val v2: Short = 3
    checkEvaluation(Hash(Literal(v1)), getHashCode(Literal(v1)))
    checkEvaluation(Hash(Literal(v2)), getHashCode(Literal(v2)))
    checkEvaluation(Hash(Literal(v1), Literal(v2)), getHashCode(Literal(v1), Literal(v2)))
    checkEvaluation(Hash(Literal("ABC")), getHashCode(Literal("ABC")))
    checkEvaluation(Hash(Literal(true)), getHashCode(Literal(true)))
    checkEvaluation(Hash(Literal.create(Decimal(3.7), DecimalType.Unlimited)),
      getHashCode(Literal.create(Decimal(3.7), DecimalType.Unlimited)))
    checkEvaluation(Hash(Literal.create(java.sql.Date.valueOf("1991-12-07"), DateType)),
      getHashCode(Literal.create(java.sql.Date.valueOf("1991-12-07"), DateType)))
    checkEvaluation(
      Hash(Literal.create(java.sql.Timestamp.valueOf("1991-12-07 12:00:00"), TimestampType)),
      getHashCode(Literal.create(java.sql.Timestamp.valueOf("1991-12-07 12:00:00"), TimestampType)))
    checkEvaluation(Hash(Literal.create(Map[Int, Int](1 -> 2), MapType(IntegerType, IntegerType))),
      getHashCode(Literal.create(Map[Int, Int](1 -> 2), MapType(IntegerType, IntegerType))))
    checkEvaluation(Hash(Literal.create(Seq[Byte](1, 2, 3, 4, 5, 6), ArrayType(ByteType))),
      getHashCode(Literal.create(Seq[Byte](1, 2, 3, 4, 5, 6), ArrayType(ByteType))))
    checkEvaluation(Hash(Literal.create(Seq[Double](1.1, 2.2, 3.3, 4.4, 5.5, 6.6),
      ArrayType(DoubleType))), getHashCode(Literal.create(Seq[Double](1.1, 2.2, 3.3, 4.4, 5.5, 6.6),
        ArrayType(DoubleType))))
  }

  test("sha1") {
    checkEvaluation(Sha1(Literal("ABC".getBytes)), "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8")
    checkEvaluation(Sha1(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      "5d211bad8f4ee70e16c7d343a838fc344a1ed961")
    checkEvaluation(Sha1(Literal.create(null, BinaryType)), null)
    checkEvaluation(Sha1(Literal("".getBytes)), "da39a3ee5e6b4b0d3255bfef95601890afd80709")
    checkConsistencyBetweenInterpretedAndCodegen(Sha1, BinaryType)
  }

  test("sha2") {
    checkEvaluation(Sha2(Literal("ABC".getBytes), Literal(256)), DigestUtils.sha256Hex("ABC"))
    checkEvaluation(Sha2(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType), Literal(384)),
      DigestUtils.sha384Hex(Array[Byte](1, 2, 3, 4, 5, 6)))
    // unsupported bit length
    checkEvaluation(Sha2(Literal.create(null, BinaryType), Literal(1024)), null)
    checkEvaluation(Sha2(Literal.create(null, BinaryType), Literal(512)), null)
    checkEvaluation(Sha2(Literal("ABC".getBytes), Literal.create(null, IntegerType)), null)
    checkEvaluation(Sha2(Literal.create(null, BinaryType), Literal.create(null, IntegerType)), null)
  }

  test("crc32") {
    checkEvaluation(Crc32(Literal("ABC".getBytes)), 2743272264L)
    checkEvaluation(Crc32(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      2180413220L)
    checkEvaluation(Crc32(Literal.create(null, BinaryType)), null)
    checkConsistencyBetweenInterpretedAndCodegen(Crc32, BinaryType)
  }
}

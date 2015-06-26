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
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.dsl.expressions._

class MiscFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("md5") {
    checkEvaluation(Md5(Literal("ABC".getBytes)), "902fbdd2b1df0c4f70b4a5d23525e932")
    checkEvaluation(Md5(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      "6ac1e56bc78f031059be7be854522c4c")
    checkEvaluation(Md5(Literal.create(null, BinaryType)), null)
  }

  test("hash") {
    val f = 'd.struct(StructField("name", StringType, true)).at(0)
    checkEvaluation(Hash(Literal.create(null, NullType)), 0)
    checkEvaluation(Hash(Literal(3)), 3)
    checkEvaluation(Hash(Literal(3l)), 3)
    checkEvaluation(Hash(Literal(3.7d)), -644612093)
    checkEvaluation(Hash(Literal(3.7f)), 1080872141)
    val v1: Byte = 3
    val v2: Short = 3
    checkEvaluation(Hash(Literal(v1)), 3)
    checkEvaluation(Hash(Literal(v2)), 3)
    checkEvaluation(Hash(Literal(v1), Literal(v2)), 6)
    checkEvaluation(Hash(Literal("ABC")), 94369)
    checkEvaluation(Hash(Literal(true)), 1231)
    checkEvaluation(Hash(Literal.create(3.7, DecimalType.Unlimited)), -644612093)
    checkEvaluation(Hash(Literal.create(java.sql.Date.valueOf("1991-12-07"), DateType)), 8010)
    checkEvaluation(
      Hash(Literal.create(java.sql.Timestamp.valueOf("1991-12-07 12:00:00"), TimestampType)),
      -1745111446)
    checkEvaluation(Hash(Literal.create(Map[Int, Int](1 -> 2), IntegerType)), 3)
    checkEvaluation(Hash(Literal.create(Seq[Byte](1, 2, 3, 4, 5, 6), BinaryType)), 21)
    checkEvaluation(Hash(Literal.create(Array[Double](1.1, 2.2, 3.3, 4.4, 5.5, 6.6), DoubleType)),
      -427425783)
    checkEvaluation(Hash(f), 94369, InternalRow(create_row("ABC")))
  }

  test("sha1") {
    checkEvaluation(Sha1(Literal("ABC".getBytes)), "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8")
    checkEvaluation(Sha1(Literal.create(Array[Byte](1, 2, 3, 4, 5, 6), BinaryType)),
      "5d211bad8f4ee70e16c7d343a838fc344a1ed961")
    checkEvaluation(Sha1(Literal.create(null, BinaryType)), null)
    checkEvaluation(Sha1(Literal("".getBytes)), "da39a3ee5e6b4b0d3255bfef95601890afd80709")
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
  }

}

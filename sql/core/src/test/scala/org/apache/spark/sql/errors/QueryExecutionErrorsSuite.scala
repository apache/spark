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

package org.apache.spark.sql.errors

import org.apache.spark.{SparkException, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions.{lit, lower, struct, sum}
import org.apache.spark.sql.test.SharedSparkSession

class QueryExecutionErrorsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private def getAesInputs(): (DataFrame, DataFrame) = {
    val encryptedText16 = "4Hv0UKCx6nfUeAoPZo1z+w=="
    val encryptedText24 = "NeTYNgA+PCQBN50DA//O2w=="
    val encryptedText32 = "9J3iZbIxnmaG+OIA9Amd+A=="
    val encryptedEmptyText16 = "jmTOhz8XTbskI/zYFFgOFQ=="
    val encryptedEmptyText24 = "9RDK70sHNzqAFRcpfGM5gQ=="
    val encryptedEmptyText32 = "j9IDsCvlYXtcVJUf4FAjQQ=="

    val df1 = Seq("Spark", "").toDF
    val df2 = Seq(
      (encryptedText16, encryptedText24, encryptedText32),
      (encryptedEmptyText16, encryptedEmptyText24, encryptedEmptyText32)
    ).toDF("value16", "value24", "value32")

    (df1, df2)
  }

  test("INVALID_PARAMETER_VALUE: invalid key lengths in AES functions") {
    val (df1, df2) = getAesInputs()
    def checkInvalidKeyLength(df: => DataFrame): Unit = {
      val e = intercept[SparkException] {
        df.collect
      }.getCause.asInstanceOf[SparkRuntimeException]
      assert(e.getErrorClass === "INVALID_PARAMETER_VALUE")
      assert(e.getSqlState === "22023")
      assert(e.getMessage.matches(
        "The value of parameter\\(s\\) 'key' in the aes_encrypt/aes_decrypt function is invalid: " +
        "expects a binary value with 16, 24 or 32 bytes, but got \\d+ bytes."))
    }

    // Encryption failure - invalid key length
    checkInvalidKeyLength(df1.selectExpr("aes_encrypt(value, '12345678901234567')"))
    checkInvalidKeyLength(df1.selectExpr("aes_encrypt(value, binary('123456789012345'))"))
    checkInvalidKeyLength(df1.selectExpr("aes_encrypt(value, binary(''))"))

    // Decryption failure - invalid key length
    Seq("value16", "value24", "value32").foreach { colName =>
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), '12345678901234567')"))
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), binary('123456789012345'))"))
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), '')"))
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), binary(''))"))
    }
  }

  test("INVALID_PARAMETER_VALUE: AES decrypt failure - key mismatch") {
    val (_, df2) = getAesInputs()
    Seq(
      ("value16", "1234567812345678"),
      ("value24", "123456781234567812345678"),
      ("value32", "12345678123456781234567812345678")).foreach { case (colName, key) =>
      val e = intercept[SparkException] {
        df2.selectExpr(s"aes_decrypt(unbase64($colName), binary('$key'), 'ECB')").collect
      }.getCause.asInstanceOf[SparkRuntimeException]
      assert(e.getErrorClass === "INVALID_PARAMETER_VALUE")
      assert(e.getSqlState === "22023")
      assert(e.getMessage ===
        "The value of parameter(s) 'expr, key' in the aes_encrypt/aes_decrypt function " +
        "is invalid: Detail message: " +
        "Given final block not properly padded. " +
        "Such issues can arise if a bad key is used during decryption.")
    }
  }

  test("UNSUPPORTED_FEATURE: unsupported combinations of AES modes and padding") {
    val key16 = "abcdefghijklmnop"
    val key32 = "abcdefghijklmnop12345678ABCDEFGH"
    val (df1, df2) = getAesInputs()
    def checkUnsupportedMode(df: => DataFrame): Unit = {
      val e = intercept[SparkException] {
        df.collect
      }.getCause.asInstanceOf[SparkRuntimeException]
      assert(e.getErrorClass === "UNSUPPORTED_FEATURE")
      assert(e.getSqlState === "0A000")
      assert(e.getMessage.matches("""The feature is not supported: AES-\w+ with the padding \w+""" +
        " by the aes_encrypt/aes_decrypt function."))
    }

    // Unsupported AES mode and padding in encrypt
    checkUnsupportedMode(df1.selectExpr(s"aes_encrypt(value, '$key16', 'CBC')"))
    checkUnsupportedMode(df1.selectExpr(s"aes_encrypt(value, '$key16', 'ECB', 'NoPadding')"))

    // Unsupported AES mode and padding in decrypt
    checkUnsupportedMode(df2.selectExpr(s"aes_decrypt(value16, '$key16', 'GSM')"))
    checkUnsupportedMode(df2.selectExpr(s"aes_decrypt(value16, '$key16', 'GCM', 'PKCS')"))
    checkUnsupportedMode(df2.selectExpr(s"aes_decrypt(value32, '$key32', 'ECB', 'None')"))
  }

  test("UNSUPPORTED_FEATURE: unsupported types (map and struct) in lit()") {
    def checkUnsupportedTypeInLiteral(v: Any): Unit = {
      val e1 = intercept[SparkRuntimeException] { lit(v) }
      assert(e1.getErrorClass === "UNSUPPORTED_FEATURE")
      assert(e1.getSqlState === "0A000")
      assert(e1.getMessage.matches("""The feature is not supported: literal for '.+' of .+\."""))
    }
    checkUnsupportedTypeInLiteral(Map("key1" -> 1, "key2" -> 2))
    checkUnsupportedTypeInLiteral(("mike", 29, 1.0))

    val e2 = intercept[SparkRuntimeException] {
      trainingSales
        .groupBy($"sales.year")
        .pivot(struct(lower($"sales.course"), $"training"))
        .agg(sum($"sales.earnings"))
        .collect()
    }
    assert(e2.getMessage === "The feature is not supported: " +
      "literal for '[dotnet,Dummies]' of class " +
      "org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.")
  }

  test("UNSUPPORTED_FEATURE: unsupported pivot operations") {
    val e1 = intercept[SparkUnsupportedOperationException] {
      trainingSales
        .groupBy($"sales.year")
        .pivot($"sales.course")
        .pivot($"training")
        .agg(sum($"sales.earnings"))
        .collect()
    }
    assert(e1.getErrorClass === "UNSUPPORTED_FEATURE")
    assert(e1.getSqlState === "0A000")
    assert(e1.getMessage === "The feature is not supported: Repeated pivots.")

    val e2 = intercept[SparkUnsupportedOperationException] {
      trainingSales
        .rollup($"sales.year")
        .pivot($"training")
        .agg(sum($"sales.earnings"))
        .collect()
    }
    assert(e2.getErrorClass === "UNSUPPORTED_FEATURE")
    assert(e2.getSqlState === "0A000")
    assert(e2.getMessage === "The feature is not supported: Pivot not after a groupBy.")
  }
}

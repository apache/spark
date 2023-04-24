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

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.unsafe.types.UTF8String

class ExpressionImplUtilsSuite extends SparkFunSuite {
  private val b64decoder = java.util.Base64.getDecoder
  private val b64encoder = java.util.Base64.getEncoder

  case class TestCase(
    plaintext: String,
    key: String,
    base64CiphertextExpected: String,
    mode: String,
    padding: String = "Default",
    ivHex: String = null,
    aad: String = null,
    expectedErrorClass: String = null) {
    val plaintextBytes = plaintext.getBytes("UTF-8")
    val keyBytes = key.getBytes("UTF-8")
    val utf8mode = UTF8String.fromString(mode)
    val utf8Padding = UTF8String.fromString(padding)
    val deterministic = mode.equalsIgnoreCase("ECB") || (ivHex != null)
    val ivBytes = if (ivHex == null) null else Hex.unhex(ivHex.getBytes("UTF-8"))
    val aadBytes = if (aad == null) null else aad.getBytes("UTF-8")
  }

  val testCases = Seq(
    TestCase(
      "Spark",
      "abcdefghijklmnop",
      "4Hv0UKCx6nfUeAoPZo1z+w==",
      "ECB"),
    TestCase("Spark",
      "abcdefghijklmnop12345678",
      "NeTYNgA+PCQBN50DA//O2w==",
      "ECB"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "9J3iZbIxnmaG+OIA9Amd+A==",
      "ECB"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "+MgyzJxhusYVGWCljk7fhhl6C6oUqWmtdqoaG93KvhY=",
      "CBC"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "y5la3muiuxN2suj6VsYXB+0XUFjtrUD0/zv5eDafsA3U",
      "GCM"),
    TestCase(
      "This message is longer than a single AES block and should work fine.",
      "abcdefghijklmnop12345678ABCDEFGH",
      "agUfTbLT8KPsqbAmQn/YdpohvxqX5bBsfFjtxE5UwqvO6EWSUVy" +
        "jeDA6r30XyS0ARebsBgXKSExaAVZ40NMgDLQa6/o9pieYwLT5YXI7flU=",
      "ECB"),
    TestCase(
      "This message is longer than a single AES block and should work fine.",
      "abcdefghijklmnop12345678ABCDEFGH",
      "cxUKNdlZa/6hT6gdhp46OThPcdNONdBwJj/Ctl6z4gWVKfcA6DE" +
        "lJg84LbkueIifjNOTloduKgidk9G9a4BDsn0NjlGLUeG8GH1moPWb/+knBC7oT/OOA06W6rJXudDo",
      "CBC"),
    TestCase(
      "This message is longer than a single AES block and should work fine.",
      "abcdefghijklmnop12345678ABCDEFGH",
      "73B0tHM3F7bvmG7yIZB9vMKnzHyuCYjD9PzAI7NJ+kDBWtaFO22" +
        "n2cKlkNcCzr45a4Uol+sNtQwQAV7iRhBdt6YmXoviemyXJWOZ89G279SgxabaomEIyN/HZwenxeN4",
      "GCM")
  )

  test("AesDecrypt Only") {
    testCases.map(decOnlyCase)
  }

  test("AesEncrypt and AesDecrypt") {
    testCases.map(encDecCase)
  }

  val ivAadTestCases = Seq(
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg=",
      "CBC",
      ivHex = "00000000000000000000000000000000"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "AAAAAAAAAAAAAAAAQiYi+sRNYDAOTjdSEcYBFsAWPL1f",
      "GCM",
      ivHex = "000000000000000000000000"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
      "GCM",
      ivHex = "000000000000000000000000",
      aad = "This is an AAD mixed into the input"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
      "GCM",
      aad = "This is an AAD mixed into the input")
  )

  test("AesDecrypt only with IVs or AADs") {
    ivAadTestCases.map(decOnlyCase)
  }

  test("AesEncrypt and AesDecrypt with IVs or AADs") {
    ivAadTestCases.map(encDecCase)
  }

  def decOnlyCase(t: TestCase): Unit = {
    val expectedBytes = b64decoder.decode(t.base64CiphertextExpected)
    val decryptedBytes = ExpressionImplUtils.aesDecrypt(
      expectedBytes,
      t.keyBytes,
      t.utf8mode,
      t.utf8Padding,
      t.aadBytes
    )
    val decryptedString = new String(decryptedBytes)
    assert(decryptedString == t.plaintext)
  }

  def encDecCase(t: TestCase): Unit = {
    val ciphertextBytes = ExpressionImplUtils.aesEncrypt(
      t.plaintextBytes,
      t.keyBytes,
      t.utf8mode,
      t.utf8Padding,
      t.ivBytes,
      t.aadBytes
    )
    val ciphertextBase64 = b64encoder.encodeToString(ciphertextBytes)
    val decryptedBytes = ExpressionImplUtils.aesDecrypt(
      ciphertextBytes,
      t.keyBytes,
      t.utf8mode,
      t.utf8Padding,
      t.aadBytes
    )
    val decryptedString = new String(decryptedBytes)
    assert(decryptedString == t.plaintext)
    if (t.deterministic) {
      assert(t.base64CiphertextExpected == ciphertextBase64)
    }
  }

  val unsupportedErrorCases = Seq(
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "ECB",
      ivHex = "0000000000000000",
      expectedErrorClass = "UNSUPPORTED_FEATURE.AES_MODE_IV"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "ECB",
      aad = "ECB does not support AAD mode",
      expectedErrorClass = "UNSUPPORTED_FEATURE.AES_MODE_AAD"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "CBC",
      ivHex = "0000000000",
      expectedErrorClass = "INVALID_PARAMETER_VALUE.AES_IV_LENGTH"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "GCM",
      ivHex = "0000000000",
      expectedErrorClass = "INVALID_PARAMETER_VALUE.AES_IV_LENGTH"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "GCM",
      padding = "PKCS",
      expectedErrorClass = "UNSUPPORTED_FEATURE.AES_MODE"),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "CBC",
      aad = "CBC doesn't support AADs",
      expectedErrorClass = "UNSUPPORTED_FEATURE.AES_MODE_AAD")
  )

  test("AesEncrypt unsupported errors") {
    unsupportedErrorCases.foreach { t =>
      val e1 = intercept[SparkRuntimeException] {
        encDecCase(t)
      }
      assert(e1.isInstanceOf[SparkRuntimeException])
      assert(e1.getErrorClass == t.expectedErrorClass)
    }
  }

  val corruptedCiphertexts = Seq(
    // This is truncated
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "+MgyzJxhusYVGWCljk7fhhl6C6oUqWmtdqoaG93=",
      "CBC",
      expectedErrorClass = "INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR"),
    // The ciphertext is corrupted
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "y5la3muiuxN2suj6VsYXB+1XUFjtrUD0/zv5eDafsA3U",
      "GCM",
      expectedErrorClass = "INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR"),
    // Valid ciphertext, wrong AAD
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
      "GCM",
      aad = "The ciphertext is valid, but the AAD is wrong",
      expectedErrorClass = "INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR")
  )

  test("AesEncrypt Expected Errors") {
    corruptedCiphertexts.foreach { t =>
      val e1 = intercept[SparkRuntimeException] {
        decOnlyCase(t)
      }
      assert(e1.isInstanceOf[SparkRuntimeException])
      assert(e1.getErrorClass == t.expectedErrorClass)
    }
  }
}

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

import org.apache.commons.lang3.{JavaVersion, SystemUtils}

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException, SparkRuntimeException}
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
    ivHexOpt: Option[String] = None,
    aadOpt: Option[String] = None,
    expectedErrorClassOpt: Option[String] = None,
    errorParamsMap: Map[String, String] = Map()) {

    def isIvDefined: Boolean = {
      ivHexOpt.isDefined && ivHexOpt.get != null && ivHexOpt.get.length > 0
    }

    val plaintextBytes: Array[Byte] = plaintext.getBytes("UTF-8")
    val keyBytes: Array[Byte] = key.getBytes("UTF-8")
    val utf8mode: UTF8String = UTF8String.fromString(mode)
    val utf8Padding: UTF8String = UTF8String.fromString(padding)
    val deterministic: Boolean = mode.equalsIgnoreCase("ECB") || isIvDefined
    val ivBytes: Array[Byte] =
      ivHexOpt.map({ivHex => Hex.unhex(ivHex.getBytes("UTF-8"))}).getOrElse(null)
    val aadBytes: Array[Byte] = aadOpt.map({aad => aad.getBytes("UTF-8")}).getOrElse(null)
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
    // Test passing non-null, but empty arrays for IV and AAD
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "9J3iZbIxnmaG+OIA9Amd+A==",
      "ECB",
      ivHexOpt = Some(""),
      aadOpt = Some("")),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "+MgyzJxhusYVGWCljk7fhhl6C6oUqWmtdqoaG93KvhY=",
      "CBC"),
    // Test passing non-null, but empty arrays for IV and AAD
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "+MgyzJxhusYVGWCljk7fhhl6C6oUqWmtdqoaG93KvhY=",
      "CBC",
      ivHexOpt = Some(""),
      aadOpt = Some("")),
    TestCase(
      "Apache Spark",
      "1234567890abcdef",
      "2NYmDCjgXTbbxGA3/SnJEfFC/JQ7olk2VQWReIAAFKo=",
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
      ivHexOpt = Some("00000000000000000000000000000000")),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "AAAAAAAAAAAAAAAAQiYi+sRNYDAOTjdSEcYBFsAWPL1f",
      "GCM",
      ivHexOpt = Some("000000000000000000000000")
    ),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
      "GCM",
      ivHexOpt = Some("000000000000000000000000"),
      aadOpt = Some("This is an AAD mixed into the input")
    ),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
      "GCM",
      aadOpt = Some("This is an AAD mixed into the input")
    )
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
      ivHexOpt = Some("0000000000000000"),
      expectedErrorClassOpt = Some("UNSUPPORTED_FEATURE.AES_MODE_IV"),
      errorParamsMap = Map(
        "mode" -> "ECB",
        "functionName" -> "`aes_encrypt`"
      )
    ),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "ECB",
      aadOpt = Some("ECB does not support AAD mode"),
      expectedErrorClassOpt = Some("UNSUPPORTED_FEATURE.AES_MODE_AAD"),
      errorParamsMap = Map(
        "mode" -> "ECB",
        "functionName" -> "`aes_encrypt`"
      )
    ),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "CBC",
      ivHexOpt = Some("0000000000"),
      expectedErrorClassOpt = Some("INVALID_PARAMETER_VALUE.AES_IV_LENGTH"),
      errorParamsMap = Map(
        "mode" -> "CBC",
        "parameter" -> "`iv`",
        "functionName" -> "`aes_encrypt`/`aes_decrypt`",
        "actualLength" -> "5"
      )
    ),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "GCM",
      ivHexOpt = Some("0000000000"),
      expectedErrorClassOpt = Some("INVALID_PARAMETER_VALUE.AES_IV_LENGTH"),
      errorParamsMap = Map(
        "mode" -> "GCM",
        "parameter" -> "`iv`",
        "functionName" -> "`aes_encrypt`/`aes_decrypt`",
        "actualLength" -> "5"
      )
    ),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "GCM",
      padding = "PKCS",
      expectedErrorClassOpt = Some("UNSUPPORTED_FEATURE.AES_MODE"),
      errorParamsMap = Map(
        "mode" -> "GCM",
        "padding" -> "PKCS",
        "functionName" -> "`aes_encrypt`/`aes_decrypt`"
      )
    ),
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "unused",
      "CBC",
      aadOpt = Some("CBC doesn't support AADs"),
      expectedErrorClassOpt = Some("UNSUPPORTED_FEATURE.AES_MODE_AAD"),
      errorParamsMap = Map(
        "mode" -> "CBC",
        "functionName" -> "`aes_encrypt`"
      )
    )
  )

  test("AesEncrypt unsupported errors") {
    unsupportedErrorCases.foreach { t =>
      checkExpectedError(t, encDecCase)
    }
  }

  // JDK-8267125 changes tag error message at Java 18
  val msgTagMismatch = if (SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_17)) {
    "Tag mismatch!"
  } else {
    "Tag mismatch"
  }
  val corruptedCiphertexts = Seq(
    // This is truncated
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "+MgyzJxhusYVGWCljk7fhhl6C6oUqWmtdqoaG93=",
      "CBC",
      expectedErrorClassOpt = Some("INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR"),
      errorParamsMap = Map(
        "parameter" -> "`expr`, `key`",
        "functionName" -> "`aes_encrypt`/`aes_decrypt`",
        "detailMessage" ->
          "Input length must be multiple of 16 when decrypting with padded cipher"
      )
    ),
    // The ciphertext is corrupted
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "y5la3muiuxN2suj6VsYXB+1XUFjtrUD0/zv5eDafsA3U",
      "GCM",
      expectedErrorClassOpt = Some("INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR"),
      errorParamsMap = Map(
        "parameter" -> "`expr`, `key`",
        "functionName" -> "`aes_encrypt`/`aes_decrypt`",
        "detailMessage" -> msgTagMismatch
      )
    ),
    // Valid ciphertext, wrong AAD
    TestCase(
      "Spark",
      "abcdefghijklmnop12345678ABCDEFGH",
      "AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4",
      "GCM",
      aadOpt = Some("The ciphertext is valid, but the AAD is wrong"),
      expectedErrorClassOpt = Some("INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR"),
      errorParamsMap = Map(
        "parameter" -> "`expr`, `key`",
        "functionName" -> "`aes_encrypt`/`aes_decrypt`",
        "detailMessage" -> msgTagMismatch
      )
    )
  )

  test("AesEncrypt Expected Errors") {
    corruptedCiphertexts.foreach { t =>
      checkExpectedError(t, decOnlyCase)
    }
  }


  private def checkExpectedError(t: TestCase, f: TestCase => Unit) = {
    checkError(
      exception = intercept[SparkRuntimeException] {
        f(t)
      },
      errorClass = t.expectedErrorClassOpt.get,
      parameters = t.errorParamsMap
    )
  }

  test("Validate UTF8 string") {
    def validateUTF8(str: UTF8String, expected: UTF8String, except: Boolean): Unit = {
      if (except) {
        checkError(
          exception = intercept[SparkIllegalArgumentException] {
            ExpressionImplUtils.validateUTF8String(str)
          },
          errorClass = "INVALID_UTF8_STRING",
          parameters = Map(
            "str" -> str.getBytes.map(byte => f"\\x$byte%02X").mkString
          )
        )
      } else {
        assert(ExpressionImplUtils.validateUTF8String(str)== expected)
      }
    }
    validateUTF8(UTF8String.EMPTY_UTF8,
      UTF8String.fromString(""), except = false)
    validateUTF8(UTF8String.fromString(""),
      UTF8String.fromString(""), except = false)
    validateUTF8(UTF8String.fromString("aa"),
      UTF8String.fromString("aa"), except = false)
    validateUTF8(UTF8String.fromString("\u0061"),
      UTF8String.fromString("\u0061"), except = false)
    validateUTF8(UTF8String.fromString(""),
      UTF8String.fromString(""), except = false)
    validateUTF8(UTF8String.fromString("abc"),
      UTF8String.fromString("abc"), except = false)
    validateUTF8(UTF8String.fromString("hello"),
      UTF8String.fromString("hello"), except = false)
    validateUTF8(UTF8String.fromBytes(Array.empty[Byte]),
      UTF8String.fromString(""), except = false)
    validateUTF8(UTF8String.fromBytes(Array[Byte](0x41)),
      UTF8String.fromString("A"), except = false)
    validateUTF8(UTF8String.fromBytes(Array[Byte](0x61)),
      UTF8String.fromString("a"), except = false)
    // scalastyle:off nonascii
    validateUTF8(UTF8String.fromBytes(Array[Byte](0x80.toByte)),
      UTF8String.fromString("\uFFFD"), except = true)
    validateUTF8(UTF8String.fromBytes(Array[Byte](0xFF.toByte)),
      UTF8String.fromString("\uFFFD"), except = true)
    // scalastyle:on nonascii
  }

  test("TryValidate UTF8 string") {
    def tryValidateUTF8(str: UTF8String, expected: UTF8String): Unit = {
      assert(ExpressionImplUtils.tryValidateUTF8String(str) == expected)
    }
    tryValidateUTF8(UTF8String.fromString(""), UTF8String.fromString(""))
    tryValidateUTF8(UTF8String.fromString("aa"), UTF8String.fromString("aa"))
    tryValidateUTF8(UTF8String.fromString("\u0061"), UTF8String.fromString("\u0061"))
    tryValidateUTF8(UTF8String.EMPTY_UTF8, UTF8String.fromString(""))
    tryValidateUTF8(UTF8String.fromString(""), UTF8String.fromString(""))
    tryValidateUTF8(UTF8String.fromString("abc"), UTF8String.fromString("abc"))
    tryValidateUTF8(UTF8String.fromString("hello"), UTF8String.fromString("hello"))
    tryValidateUTF8(UTF8String.fromBytes(Array.empty[Byte]), UTF8String.fromString(""))
    tryValidateUTF8(UTF8String.fromBytes(Array[Byte](0x41)), UTF8String.fromString("A"))
    tryValidateUTF8(UTF8String.fromBytes(Array[Byte](0x61)), UTF8String.fromString("a"))
    tryValidateUTF8(UTF8String.fromBytes(Array[Byte](0x80.toByte)), null)
    tryValidateUTF8(UTF8String.fromBytes(Array[Byte](0xFF.toByte)), null)
  }

}

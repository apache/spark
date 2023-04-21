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

import org.apache.spark.SparkFunSuite
import org.apache.spark.unsafe.types.UTF8String


class ExpressionImplUtilsSuite extends SparkFunSuite {
  case class TestCase(
    plaintext: String,
    key: String,
    base64CiphertextExpected: String,
    mode: String,
    padding: String = "Default") {
    val plaintextBytes = plaintext.getBytes("UTF-8")
    val keyBytes = key.getBytes("UTF-8")
    val utf8mode = UTF8String.fromString(mode)
    val utf8Padding = UTF8String.fromString(padding)
    val deterministic = mode.equalsIgnoreCase("ECB")
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
    val decoder = java.util.Base64.getDecoder
    testCases.foreach { t =>
      val expectedBytes = decoder.decode(t.base64CiphertextExpected)
      val decryptedBytes =
        ExpressionImplUtils.aesDecrypt(expectedBytes, t.keyBytes, t.utf8mode, t.utf8Padding)
      val decryptedString = new String(decryptedBytes)
      assert(decryptedString == t.plaintext)
    }
  }

  test("AesEncrypt and AesDecrypt") {
    val encoder = java.util.Base64.getEncoder
    testCases.foreach { t =>
      val ciphertextBytes =
        ExpressionImplUtils.aesEncrypt(t.plaintextBytes, t.keyBytes, t.utf8mode, t.utf8Padding)
      val ciphertextBase64 = encoder.encodeToString(ciphertextBytes)
      val decryptedBytes =
        ExpressionImplUtils.aesDecrypt(ciphertextBytes, t.keyBytes, t.utf8mode, t.utf8Padding)
      val decryptedString = new String(decryptedBytes)
      assert(decryptedString == t.plaintext)
      if (t.deterministic) {
        assert(t.base64CiphertextExpected == ciphertextBase64)
      }
    }
  }
}

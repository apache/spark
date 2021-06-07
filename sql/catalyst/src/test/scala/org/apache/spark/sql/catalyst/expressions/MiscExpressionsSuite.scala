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

import java.io.PrintStream
import java.nio.charset.StandardCharsets

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class MiscExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("RaiseError") {
    checkExceptionInExpression[RuntimeException](
      RaiseError(Literal("error message")),
      EmptyRow,
      "error message"
    )

    checkExceptionInExpression[RuntimeException](
      RaiseError(Literal.create(null, StringType)),
      EmptyRow,
      null
    )

    // Expects a string
    assert(RaiseError(Literal(5)).checkInputDataTypes().isFailure)
  }

  test("uuid") {
    checkEvaluation(Length(Uuid(Some(0))), 36)
    val r = new Random()
    val seed1 = Some(r.nextLong())
    assert(evaluateWithoutCodegen(Uuid(seed1)) === evaluateWithoutCodegen(Uuid(seed1)))
    assert(evaluateWithMutableProjection(Uuid(seed1)) ===
      evaluateWithMutableProjection(Uuid(seed1)))
    assert(evaluateWithUnsafeProjection(Uuid(seed1)) ===
      evaluateWithUnsafeProjection(Uuid(seed1)))

    val seed2 = Some(r.nextLong())
    assert(evaluateWithoutCodegen(Uuid(seed1)) !== evaluateWithoutCodegen(Uuid(seed2)))
    assert(evaluateWithMutableProjection(Uuid(seed1)) !==
      evaluateWithMutableProjection(Uuid(seed2)))
    assert(evaluateWithUnsafeProjection(Uuid(seed1)) !==
      evaluateWithUnsafeProjection(Uuid(seed2)))

    val uuid = Uuid(seed1)
    assert(uuid.fastEquals(uuid))
    assert(!uuid.fastEquals(Uuid(seed1)))
    assert(!uuid.fastEquals(uuid.freshCopy()))
    assert(!uuid.fastEquals(Uuid(seed2)))
  }

  test("PrintToStderr") {
    val inputExpr = Literal(1)
    val systemErr = System.err

    val (outputEval, outputCodegen) = try {
      val errorStream = new java.io.ByteArrayOutputStream()
      System.setErr(new PrintStream(errorStream))
      // check without codegen
      checkEvaluationWithoutCodegen(PrintToStderr(inputExpr), 1)
      val outputEval = errorStream.toString
      errorStream.reset()
      // check with codegen
      checkEvaluationWithMutableProjection(PrintToStderr(inputExpr), 1)
      val outputCodegen = errorStream.toString
      (outputEval, outputCodegen)
    } finally {
      System.setErr(systemErr)
    }

    assert(outputCodegen.contains(s"Result of $inputExpr is 1"))
    assert(outputEval.contains(s"Result of $inputExpr is 1"))
  }

  test("aes") {
    val text = "Spark"
    val textBytes = Literal(text.getBytes(StandardCharsets.UTF_8))
    val nullBytes = Literal(null, BinaryType)
    val key16 = Literal("abcdefghijklmnop".getBytes(StandardCharsets.UTF_8))
    val key24 = Literal("abcdefghijklmnop12345678".getBytes(StandardCharsets.UTF_8))
    val key32 = Literal("abcdefghijklmnop12345678ABCDEFGH".getBytes(StandardCharsets.UTF_8))
    val dummyKey16 = Literal("1234567812345678".getBytes(StandardCharsets.UTF_8))
    val dummyKey24 = Literal("123456781234567812345678".getBytes(StandardCharsets.UTF_8))
    val dummyKey32 = Literal("12345678123456781234567812345678".getBytes(StandardCharsets.UTF_8))
    val invalidLengthKey = Literal("abc".getBytes(StandardCharsets.UTF_8))
    val encryptedText16 = "4Hv0UKCx6nfUeAoPZo1z+w=="
    val encryptedText24 = "NeTYNgA+PCQBN50DA//O2w=="
    val encryptedText32 = "9J3iZbIxnmaG+OIA9Amd+A=="

    // Successful encryption
    checkEvaluation(Base64(AesEncrypt(textBytes, key16)), encryptedText16)
    checkEvaluation(Base64(AesEncrypt(textBytes, key24)), encryptedText24)
    checkEvaluation(Base64(AesEncrypt(textBytes, key32)), encryptedText32)

    // Encryption failure - input or key is null
    checkEvaluation(Base64(AesEncrypt(nullBytes, key16)), null)
    checkEvaluation(Base64(AesEncrypt(nullBytes, key24)), null)
    checkEvaluation(Base64(AesEncrypt(nullBytes, key32)), null)
    checkEvaluation(Base64(AesEncrypt(textBytes, nullBytes)), null)

    // Encryption failure - invalid key length
    checkEvaluation(Base64(AesEncrypt(textBytes, Literal(invalidLengthKey))), null)

    // Successful decryption
    checkEvaluation(Cast(AesDecrypt(UnBase64(Literal(encryptedText16)), key16), StringType), text)
    checkEvaluation(Cast(AesDecrypt(UnBase64(Literal(encryptedText24)), key24), StringType), text)
    checkEvaluation(Cast(AesDecrypt(UnBase64(Literal(encryptedText32)), key32), StringType), text)

    // Decryption failure - input or key is null
    checkEvaluation(Cast(AesDecrypt(nullBytes, key16), StringType), null)
    checkEvaluation(Cast(AesDecrypt(nullBytes, key24), StringType), null)
    checkEvaluation(Cast(AesDecrypt(nullBytes, key32), StringType), null)
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText16)), nullBytes), StringType), null)
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText24)), nullBytes), StringType), null)
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText32)), nullBytes), StringType), null)

    // Decryption failure - invalid key length
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText16)), invalidLengthKey), StringType), null)
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText24)), invalidLengthKey), StringType), null)
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText32)), invalidLengthKey), StringType), null)

    // Decryption failure - key mismatch
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText16)), dummyKey16), StringType), null)
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText16)), dummyKey24), StringType), null)
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText16)), dummyKey32), StringType), null)
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText16)), key24), StringType), null)
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText24)), key32), StringType), null)
    checkEvaluation(
      Cast(AesDecrypt(UnBase64(Literal(encryptedText32)), key16), StringType), null)
  }
}

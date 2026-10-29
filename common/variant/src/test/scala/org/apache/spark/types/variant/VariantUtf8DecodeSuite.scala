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

package org.apache.spark.types.variant

import java.nio.charset.StandardCharsets
import java.nio.file.Paths

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class VariantUtf8DecodeSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  test("SPARK-57599: keys and string values decode as UTF-8 under a non-UTF-8 default charset") {
    // The reader must decode object keys and string values as UTF-8 regardless of the JVM default
    // charset. That can only be exercised by changing the default charset, which is fixed at JVM
    // startup and pinned to UTF-8 in the test JVM, so fork a child JVM with a non-UTF-8
    // -Dfile.encoding and round-trip a variant with non-ASCII content there (see
    // VariantUtf8DecodeChecker). With the pre-fix default-charset decode the characters are
    // corrupted and the child exits non-zero.
    val javaExe = Paths.get(sys.props("java.home"), "bin", "java").toString
    val command = Seq(
      javaExe,
      "-Dfile.encoding=ISO-8859-1",
      "-cp", sys.props("java.class.path"),
      VariantUtf8DecodeChecker.getClass.getName.stripSuffix("$"))
    val process = new ProcessBuilder(command: _*).redirectErrorStream(true).start()
    val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8).trim
    val exitCode = process.waitFor()
    if (output.contains("RESULT=INCONCLUSIVE")) {
      // The child JVM ended up with a UTF-8 default charset, so it cannot exercise the fix.
      assume(false, s"Inconclusive; child output: $output")
    }
    assert(exitCode === 0, s"child JVM reported a decode failure; output: $output")
  }
}

/**
 * Entry point run in a child JVM by `VariantUtf8DecodeSuite` to verify that the Variant reader
 * decodes object keys and string values as UTF-8 independently of the JVM default charset.
 *
 * This has to run as a separate `main`: the only way to actually exercise the bug is to start a JVM
 * with a non-UTF-8 `-Dfile.encoding`, because the default charset is fixed at JVM startup and is
 * pinned to UTF-8 in the test JVM. With the pre-fix default-charset decode, the non-ASCII content
 * round-tripped here is corrupted and the process exits non-zero.
 *
 * All output is ASCII (characters are reported as code points) so it is readable regardless of the
 * child's default charset. The process prints `RESULT=OK` and exits 0 on success, `RESULT=FAIL` and
 * exits 1 if any value was corrupted, or `RESULT=INCONCLUSIVE` and exits 0 if the child's default
 * charset turned out to be UTF-8 (in which case the fixed and buggy code cannot be told apart).
 */
private[variant] object VariantUtf8DecodeChecker {

  // This object is a standalone process whose stdout is its result protocol, so println is used
  // deliberately as the communication channel with the parent test.
  // scalastyle:off println

  // scalastyle:off nonascii
  // (key, value) pairs covering 2-byte, 3-byte, and LONG_STR (> 63 UTF-8 bytes) UTF-8 content.
  private val cases: Seq[(String, String)] = Seq(
    "café" -> "résumé",
    "你好" -> "世界",
    "é" -> ("你" * 22)) // value is 66 UTF-8 bytes, over MAX_SHORT_STR_SIZE -> LONG_STR encoding
  // scalastyle:on nonascii

  def main(args: Array[String]): Unit = {
    // Read the startup `file.encoding`, which determines the JVM default charset that the buggy
    // `new String(bytes)` decode used. (Querying the default charset directly is banned by
    // scalastyle.)
    val fileEncoding = sys.props.getOrElse("file.encoding", "")
    if (fileEncoding.equalsIgnoreCase("UTF-8")) {
      // The fixed and buggy code are indistinguishable when the default charset is already UTF-8.
      println(s"RESULT=INCONCLUSIVE fileEncoding=$fileEncoding")
      System.exit(0)
    }
    var failures = 0
    cases.foreach { case (key, value) =>
      val json = "{" + jsonString(key) + ":" + jsonString(value) + "}"
      val field = VariantBuilder.parseJson(json, false).getFieldAtIndex(0)
      if (field.key != key) {
        println(s"KEY_MISMATCH expected=${codePoints(key)} actual=${codePoints(field.key)}")
        failures += 1
      }
      val decoded = field.value.getString
      if (decoded != value) {
        println(s"VALUE_MISMATCH expected=${codePoints(value)} actual=${codePoints(decoded)}")
        failures += 1
      }
    }
    if (failures == 0) {
      println(s"RESULT=OK fileEncoding=$fileEncoding")
      System.exit(0)
    } else {
      println(s"RESULT=FAIL failures=$failures fileEncoding=$fileEncoding")
      System.exit(1)
    }
  }

  private def jsonString(s: String): String = "\"" + s + "\""

  private def codePoints(s: String): String =
    s.codePoints().toArray.map(c => "U+%04X".format(c)).mkString("[", ",", "]")

  // scalastyle:on println
}

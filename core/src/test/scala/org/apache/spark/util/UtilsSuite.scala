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

package org.apache.spark.util

import com.google.common.base.Charsets
import com.google.common.io.Files
import java.io.{ByteArrayOutputStream, ByteArrayInputStream, FileOutputStream, File}
import java.nio.{ByteBuffer, ByteOrder}
import org.scalatest.FunSuite
import org.apache.commons.io.FileUtils
import scala.util.Random

class UtilsSuite extends FunSuite {

  test("bytesToString") {
    assert(Utils.bytesToString(10) === "10.0 B")
    assert(Utils.bytesToString(1500) === "1500.0 B")
    assert(Utils.bytesToString(2000000) === "1953.1 KB")
    assert(Utils.bytesToString(2097152) === "2.0 MB")
    assert(Utils.bytesToString(2306867) === "2.2 MB")
    assert(Utils.bytesToString(5368709120L) === "5.0 GB")
    assert(Utils.bytesToString(5L * 1024L * 1024L * 1024L * 1024L) === "5.0 TB")
  }

  test("copyStream") {
    //input array initialization
    val bytes = Array.ofDim[Byte](9000)
    Random.nextBytes(bytes)

    val os = new ByteArrayOutputStream()
    Utils.copyStream(new ByteArrayInputStream(bytes), os)

    assert(os.toByteArray.toList.equals(bytes.toList))
  }

  test("memoryStringToMb") {
    assert(Utils.memoryStringToMb("1") === 0)
    assert(Utils.memoryStringToMb("1048575") === 0)
    assert(Utils.memoryStringToMb("3145728") === 3)

    assert(Utils.memoryStringToMb("1024k") === 1)
    assert(Utils.memoryStringToMb("5000k") === 4)
    assert(Utils.memoryStringToMb("4024k") === Utils.memoryStringToMb("4024K"))

    assert(Utils.memoryStringToMb("1024m") === 1024)
    assert(Utils.memoryStringToMb("5000m") === 5000)
    assert(Utils.memoryStringToMb("4024m") === Utils.memoryStringToMb("4024M"))

    assert(Utils.memoryStringToMb("2g") === 2048)
    assert(Utils.memoryStringToMb("3g") === Utils.memoryStringToMb("3G"))

    assert(Utils.memoryStringToMb("2t") === 2097152)
    assert(Utils.memoryStringToMb("3t") === Utils.memoryStringToMb("3T"))
  }

  test("splitCommandString") {
    assert(Utils.splitCommandString("") === Seq())
    assert(Utils.splitCommandString("a") === Seq("a"))
    assert(Utils.splitCommandString("aaa") === Seq("aaa"))
    assert(Utils.splitCommandString("a b c") === Seq("a", "b", "c"))
    assert(Utils.splitCommandString("  a   b\t c ") === Seq("a", "b", "c"))
    assert(Utils.splitCommandString("a 'b c'") === Seq("a", "b c"))
    assert(Utils.splitCommandString("a 'b c' d") === Seq("a", "b c", "d"))
    assert(Utils.splitCommandString("'b c'") === Seq("b c"))
    assert(Utils.splitCommandString("a \"b c\"") === Seq("a", "b c"))
    assert(Utils.splitCommandString("a \"b c\" d") === Seq("a", "b c", "d"))
    assert(Utils.splitCommandString("\"b c\"") === Seq("b c"))
    assert(Utils.splitCommandString("a 'b\" c' \"d' e\"") === Seq("a", "b\" c", "d' e"))
    assert(Utils.splitCommandString("a\t'b\nc'\nd") === Seq("a", "b\nc", "d"))
    assert(Utils.splitCommandString("a \"b\\\\c\"") === Seq("a", "b\\c"))
    assert(Utils.splitCommandString("a \"b\\\"c\"") === Seq("a", "b\"c"))
    assert(Utils.splitCommandString("a 'b\\\"c'") === Seq("a", "b\\\"c"))
    assert(Utils.splitCommandString("'a'b") === Seq("ab"))
    assert(Utils.splitCommandString("'a''b'") === Seq("ab"))
    assert(Utils.splitCommandString("\"a\"b") === Seq("ab"))
    assert(Utils.splitCommandString("\"a\"\"b\"") === Seq("ab"))
    assert(Utils.splitCommandString("''") === Seq(""))
    assert(Utils.splitCommandString("\"\"") === Seq(""))
  }

  test("string formatting of time durations") {
    val second = 1000
    val minute = second * 60
    val hour = minute * 60
    def str = Utils.msDurationToString(_)

    assert(str(123) === "123 ms")
    assert(str(second) === "1.0 s")
    assert(str(second + 462) === "1.5 s")
    assert(str(hour) === "1.00 h")
    assert(str(minute) === "1.0 m")
    assert(str(minute + 4 * second + 34) === "1.1 m")
    assert(str(10 * hour + minute + 4 * second) === "10.02 h")
    assert(str(10 * hour + 59 * minute + 59 * second + 999) === "11.00 h")
  }

  test("reading offset bytes of a file") {
    val tmpDir2 = Files.createTempDir()
    val f1Path = tmpDir2 + "/f1"
    val f1 = new FileOutputStream(f1Path)
    f1.write("1\n2\n3\n4\n5\n6\n7\n8\n9\n".getBytes(Charsets.UTF_8))
    f1.close()

    // Read first few bytes
    assert(Utils.offsetBytes(f1Path, 0, 5) === "1\n2\n3")

    // Read some middle bytes
    assert(Utils.offsetBytes(f1Path, 4, 11) === "3\n4\n5\n6")

    // Read last few bytes
    assert(Utils.offsetBytes(f1Path, 12, 18) === "7\n8\n9\n")

    // Read some nonexistent bytes in the beginning
    assert(Utils.offsetBytes(f1Path, -5, 5) === "1\n2\n3")

    // Read some nonexistent bytes at the end
    assert(Utils.offsetBytes(f1Path, 12, 22) === "7\n8\n9\n")

    // Read some nonexistent bytes on both ends
    assert(Utils.offsetBytes(f1Path, -3, 25) === "1\n2\n3\n4\n5\n6\n7\n8\n9\n")

    FileUtils.deleteDirectory(tmpDir2)
  }

  test("deserialize long value") {
    val testval : Long = 9730889947L
    val bbuf = ByteBuffer.allocate(8)
    assert(bbuf.hasArray)
    bbuf.order(ByteOrder.BIG_ENDIAN)
    bbuf.putLong(testval)
    assert(bbuf.array.length === 8)
    assert(Utils.deserializeLongValue(bbuf.array) === testval)
  }
}


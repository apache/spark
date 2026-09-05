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

package org.apache.spark.scheduler

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.zip.CRC32

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.BlockManagerId

class MapStatusChecksumSuite extends SparkFunSuite {

  private val loc = BlockManagerId("exec-1", "host-1", 7337)
  private val CRC32Alg = MapStatusChecksum.CRC32_ALG
  private val ADLER32Alg = MapStatusChecksum.ADLER32

  test("compute returns None when all partitions are empty") {
    val sizes = Array.fill[Long](128)(0L)
    assert(MapStatusChecksum.compute(sizes, CRC32Alg).isEmpty)
  }

  test("compute is invariant to size values, only depends on non-empty partition indices") {
    val sizesA = Array[Long](0, 100, 0, 200, 0, 300)
    val sizesB = Array[Long](0, 1, 0, 2, 0, 3)
    assert(MapStatusChecksum.compute(sizesA, CRC32Alg) ===
      MapStatusChecksum.compute(sizesB, CRC32Alg))
  }

  test("compute is sensitive to a single-bit change in the non-empty set") {
    val base = Array[Long](0, 100, 0, 200, 0, 300)
    val flipped = base.clone(); flipped(2) = 42
    assert(MapStatusChecksum.compute(base, CRC32Alg) !==
      MapStatusChecksum.compute(flipped, CRC32Alg))
  }

  test("compute matches an independent CRC32 reference implementation") {
    val sizes = Array[Long](0, 1, 0, 2, 3, 0, 0, 4)
    val expected = {
      val crc = new CRC32()
      val buf = new Array[Byte](4)
      def writeIntBE(v: Int): Unit = {
        buf(0) = (v >>> 24).toByte
        buf(1) = (v >>> 16).toByte
        buf(2) = (v >>>  8).toByte
        buf(3) = v.toByte
        crc.update(buf, 0, 4)
      }
      writeIntBE(1); writeIntBE(3); writeIntBE(4); writeIntBE(7)
      crc.getValue.toInt
    }
    assert(MapStatusChecksum.compute(sizes, CRC32Alg) === Some(expected))
  }

  test("ADLER32 produces a different value than CRC32 for the same input") {
    val sizes = Array[Long](0, 1, 0, 2, 3, 0, 0, 4)
    val crc = MapStatusChecksum.compute(sizes, CRC32Alg)
    val adler = MapStatusChecksum.compute(sizes, ADLER32Alg)
    assert(crc.isDefined && adler.isDefined)
    assert(crc !== adler)
  }

  test("newChecksum rejects unsupported algorithm names") {
    intercept[IllegalArgumentException] {
      MapStatusChecksum.newChecksum("SHA256")
    }
  }

  test("recompute agrees with compute for the same non-empty partition set") {
    val sizes = Array[Long](0, 100, 0, 0, 200, 0, 300)
    val expected = MapStatusChecksum.compute(sizes, CRC32Alg)
    val status = new CompressedMapStatus(loc, sizes, mapTaskId = 7L, nonEmptyChecksum = expected)
    assert(MapStatusChecksum.recompute(status, CRC32Alg) === expected)
  }

  test("recompute matches the stored checksum when partitions are unchanged") {
    val sizes = Array[Long](0, 100, 0, 200)
    val cs = MapStatusChecksum.compute(sizes, CRC32Alg)
    val status = new CompressedMapStatus(loc, sizes, mapTaskId = 1L, nonEmptyChecksum = cs)
    assert(MapStatusChecksum.recompute(status, CRC32Alg) === cs)
  }

  test("recompute differs from stored checksum when the non-empty set changes") {
    val sizes = Array[Long](0, 100, 0, 200)
    val wrongChecksum: Int = 0x12345678
    val status = new CompressedMapStatus(
      loc, sizes, mapTaskId = 1L, nonEmptyChecksum = Some(wrongChecksum))
    val actual = MapStatusChecksum.recompute(status, CRC32Alg)
    assert(actual !== Some(wrongChecksum))
    assert(actual === MapStatusChecksum.compute(sizes, CRC32Alg))
  }

  test("CompressedMapStatus round-trips checksum via Java serialization") {
    val sizes = Array[Long](0, 100, 0, 200, 0, 300)
    val cs = MapStatusChecksum.compute(sizes, CRC32Alg)
    val original = new CompressedMapStatus(loc, sizes, mapTaskId = 42L, nonEmptyChecksum = cs)
    val roundTripped = javaRoundTrip(original)
    assert(roundTripped.nonEmptyChecksum === cs)
    assert(roundTripped.mapId === 42L)
    assert(roundTripped.location === loc)
  }

  test("HighlyCompressedMapStatus round-trips checksum via Java serialization") {
    val sizes = Array.tabulate[Long](3000)(i => if (i % 100 == 0) i.toLong else 0L)
    val cs = MapStatusChecksum.compute(sizes, CRC32Alg)
    val original = HighlyCompressedMapStatus(loc, sizes, mapTaskId = 99L, nonEmptyChecksum = cs)
    val roundTripped = javaRoundTrip(original)
    assert(roundTripped.nonEmptyChecksum === cs)
    assert(roundTripped.mapId === 99L)
  }

  private def javaRoundTrip[T <: MapStatus](status: T): T = {
    val bytes = javaBytes(status)
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val out = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }

  private def javaBytes(o: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(o)
    oos.close()
    baos.toByteArray
  }
}

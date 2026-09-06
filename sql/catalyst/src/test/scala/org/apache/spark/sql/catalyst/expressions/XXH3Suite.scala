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

/**
 * Validates the [[XXH3]] port against the reference implementation's known-answer vectors
 * (github.com/Cyan4973/xxHash). Inputs are prefixes of the same pseudo-random buffer the reference
 * test harness uses (tests/sanity_test.c); expected values were produced with the reference
 * `xxhash` library and cover every length branch (0, 1-3, 4-8, 9-16, 17-128, 129-240, and the
 * long path) with both a zero and a non-zero seed.
 */
class XXH3Suite extends SparkFunSuite {

  private val buffer: Array[Byte] = {
    val buf = new Array[Byte](4200)
    var byteGen = 0x9E3779B1L
    for (i <- buf.indices) {
      buf(i) = (byteGen >>> 56).toByte
      byteGen *= 0x9E3779B185EBCA8DL
    }
    buf
  }

  private def input(len: Int): Array[Byte] = java.util.Arrays.copyOf(buffer, len)

  // 64-bit: (length, seed, expected)
  private val vectors64: Seq[(Int, Long, Long)] = Seq(
    (0, 0L, 0x2d06800538d394c2L),
    (1, 0L, 0xc44bdff4074eecdbL),
    (2, 0L, 0x7a9978044cb8a8bbL),
    (3, 0L, 0x54247382a8d6b94dL),
    (4, 0L, 0xe5dc74bc51848a51L),
    (5, 0L, 0xe4243f00720306bbL),
    (7, 0L, 0x9941e0007f555e50L),
    (8, 0L, 0x24ccc9acaa9f65e4L),
    (9, 0L, 0x14d5001c15dd3f2bL),
    (12, 0L, 0xa713daf0dfbb77e7L),
    (16, 0L, 0x981b17d36c7498c9L),
    (17, 0L, 0x796f5acd3a60f862L),
    (32, 0L, 0x9feaddbdbf57eed3L),
    (64, 0L, 0x9cb48487720ec49dL),
    (100, 0L, 0x93cd95432b7d483fL),
    (128, 0L, 0xfcff24126754d861L),
    (129, 0L, 0x98f1b0a679a2ca29L),
    (160, 0L, 0x9d03a319ed4cbd2bL),
    (200, 0L, 0xbddca58935d7c038L),
    (240, 0L, 0x81c3c2b67f568ccfL),
    (241, 0L, 0xc5a639ecd2030e5eL),
    (256, 0L, 0x55de574ad89d0ac5L),
    (512, 0L, 0x617e49599013cb6bL),
    (1024, 0L, 0xdd85c9b5c1109c5cL),
    (2048, 0L, 0xdd59e2c3a5f038e0L),
    (4096, 0L, 0xe91206429d1f48f9L),
    (0, 0x9e3779b185ebca8dL, 0xa8a6b918b2f0364aL),
    (2, 0x9e3779b185ebca8dL, 0x764b35c90519ad88L),
    (8, 0x9e3779b185ebca8dL, 0x8f973410999b8f6bL),
    (16, 0x9e3779b185ebca8dL, 0x663f29333b4db6b1L),
    (64, 0x9e3779b185ebca8dL, 0x4fe8895db9b8c077L),
    (240, 0x9e3779b185ebca8dL, 0xcc0f58c27ef3d8eeL),
    (256, 0x9e3779b185ebca8dL, 0x4d30234b7a3aa61cL),
    (1024, 0x9e3779b185ebca8dL, 0xef368a8a2ebabaefL)
  )

  // 128-bit: (length, seed, expected canonical hex)
  private val vectors128: Seq[(Int, Long, String)] = Seq(
    (0, 0L, "99aa06d3014798d86001c324468d497f"),
    (1, 0L, "a6cd5e9392000f6ac44bdff4074eecdb"),
    (2, 0L, "76750c3c7bf956687a9978044cb8a8bb"),
    (3, 0L, "20efc49ff02422ea54247382a8d6b94d"),
    (4, 0L, "970d585ac632bf8e2e7d8d6876a39fe9"),
    (5, 0L, "62ed587687606b4e057c7ed2c01fa1d1"),
    (7, 0L, "dd9b6039f79ec416081c22dd284a2f0a"),
    (8, 0L, "47a7f080d82bb45664c69cab4bb21dc5"),
    (9, 0L, "564ef6078950d457ed7ccbc501eb7501"),
    (12, 0L, "6e3efd8fc7802b18061a192713f69ad9"),
    (16, 0L, "c68c368ecf8a9c05562980258a998629"),
    (17, 0L, "955fa78643ed3669abbc12d11973d7db"),
    (32, 0L, "98fc6458710dc2e8278410a17595e3f9"),
    (64, 0L, "6d90e81a9b0fd622efdb6a44690721a9"),
    (100, 0L, "9b50b05817ab158e5fcbc2e3295f2476"),
    (128, 0L, "39992220e045260aebb15e34a7fb5ab1"),
    (129, 0L, "03815fc91f1b30b686c9e3bc8f0a3b5c"),
    (160, 0L, "ba5d218964b622ad737126c8d7c09cee"),
    (200, 0L, "e76ff4780fe18439eb060f1bb3126f5a"),
    (240, 0L, "aa4202daa2769dc85c9aae94c8ebe5a0"),
    (241, 0L, "99a80ecf0ecfc647c5a639ecd2030e5e"),
    (256, 0L, "8b1c66091423d28855de574ad89d0ac5"),
    (512, 0L, "18d2d110dcc9bca1617e49599013cb6b"),
    (1024, 0L, "0d30d24071c64c57dd85c9b5c1109c5c"),
    (2048, 0L, "f736557fd47073a5dd59e2c3a5f038e0"),
    (4096, 0L, "b9cfaea2ca5626a4e91206429d1f48f9"),
    (0, 0x9e3779b185ebca8dL, "00feaa732a3ce25ea986dfc5d7605bfe"),
    (2, 0x9e3779b185ebca8dL, "7b96e6a600dae67d764b35c90519ad88"),
    (8, 0x9e3779b185ebca8dL, "f50cec145bcd5c5a7b29471dc729b5ff"),
    (16, 0x9e3779b185ebca8dL, "6ffcb80cd33085c80346d13a7a5498c7"),
    (64, 0x9e3779b185ebca8dL, "37b738968d40bda59405ba2affa95ceb"),
    (240, 0x9e3779b185ebca8dL, "29d2133d6ea58c5b604e98db085c1864"),
    (256, 0x9e3779b185ebca8dL, "aaa57235b92d5e7c4d30234b7a3aa61c"),
    (1024, 0x9e3779b185ebca8dL, "17600efe2b493a18ef368a8a2ebabaef")
  )

  test("reference test buffer generation") {
    val expectedFirst24 = Seq(0x00, 0x52, 0x92, 0x9B, 0xB7, 0x32, 0xA3, 0x24, 0x2D, 0x00, 0xAF,
      0x95, 0x0E, 0xEC, 0xB8, 0x93, 0xE3, 0xDF, 0xEF, 0x93, 0xAA, 0xD6, 0xCD, 0x2A)
    expectedFirst24.zipWithIndex.foreach { case (b, i) =>
      assert((buffer(i) & 0xFF) == b,
        s"buffer($i): expected 0x${b.toHexString} got 0x${(buffer(i) & 0xFF).toHexString}")
    }
  }

  test("XXH3 64-bit against reference vectors") {
    vectors64.foreach { case (len, seed, expected) =>
      val actual = XXH3.hash64(input(len), seed)
      assert(actual == expected, s"len=$len seed=0x${seed.toHexString}: " +
        s"expected 0x${expected.toHexString} but got 0x${actual.toHexString}")
    }
  }

  test("XXH3 128-bit against reference vectors") {
    vectors128.foreach { case (len, seed, expected) =>
      val actual = XXH3.hash128Hex(input(len), seed).toString
      assert(actual == expected, s"len=$len seed=0x${seed.toHexString}: " +
        s"expected $expected but got $actual")
    }
  }
}

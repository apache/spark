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

package org.apache.spark.shuffle.checksum

import org.apache.spark.SparkFunSuite

class RowBasedChecksumSuite extends SparkFunSuite {

//  test("Invalid checksum algorithm should fail") {
//    val rowBasedChecksum = new OutputStreamRowBasedChecksum("invalid")
//    rowBasedChecksum.update(Long.box(10), Long.box(20))
//    // We fail to compute the checksum, and getValue returns 0.
//    assert(rowBasedChecksum.getValue == 0)
//  }

  test("Two identical rows should have a checksum of zero with XOR - ADLER32") {
    val rowBasedChecksum = new OutputStreamRowBasedChecksum("ADLER32")
    assert(rowBasedChecksum.getValue == 0)

    // Updates the checksum with one row.
    rowBasedChecksum.update(Long.box(10), Long.box(20))
    assert(rowBasedChecksum.getValue != 0)
    // specific value:
    // - Scala 2.12: 3569823494L
    // - Scala 2.13: 3329306339L

    // Updates the checksum with the same row again, and the row-based checksum should become 0.
    rowBasedChecksum.update(Long.box(10), Long.box(20))
    assert(rowBasedChecksum.getValue == 0)
  }

  test("Two identical rows should have a checksum of zero with XOR - CRC32") {
    val rowBasedChecksum = new OutputStreamRowBasedChecksum("CRC32")
    assert(rowBasedChecksum.getValue == 0)

    // Updates the checksum with one row.
    rowBasedChecksum.update(Long.box(10), Long.box(20))
    assert(rowBasedChecksum.getValue != 0)
    // specific value:
    // - Scala 2.12: 846153942L
    // - Scala 2.13: 2004131951L

    // Updates the checksum with the same row again, and the row-based checksum should become 0.
    rowBasedChecksum.update(Long.box(10), Long.box(20))
    assert(rowBasedChecksum.getValue == 0)
  }

  test("The checksum is independent of row order - two rows") {
    val algorithms = Array("ADLER32", "CRC32")
    for(algorithm <- algorithms) {
      val rowBasedChecksum1 = new OutputStreamRowBasedChecksum(algorithm)
      val rowBasedChecksum2 = new OutputStreamRowBasedChecksum(algorithm)
      assert(rowBasedChecksum1.getValue == 0)
      assert(rowBasedChecksum2.getValue == 0)

      rowBasedChecksum1.update(Long.box(10), Long.box(20))
      rowBasedChecksum2.update(Long.box(30), Long.box(40))
      assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

      rowBasedChecksum1.update(Long.box(30), Long.box(40))
      rowBasedChecksum2.update(Long.box(10), Long.box(20))
      assert(rowBasedChecksum1.getValue == rowBasedChecksum2.getValue)

      assert(rowBasedChecksum1.getValue != 0)
      assert(rowBasedChecksum2.getValue != 0)
    }
  }

  test("The checksum is independent of row order - multiple rows") {
    val algorithms = Array("ADLER32", "CRC32")
    for(algorithm <- algorithms) {
      val rowBasedChecksum1 = new OutputStreamRowBasedChecksum(algorithm)
      val rowBasedChecksum2 = new OutputStreamRowBasedChecksum(algorithm)
      assert(rowBasedChecksum1.getValue == 0)
      assert(rowBasedChecksum2.getValue == 0)

      rowBasedChecksum1.update(Long.box(10), Long.box(20))
      rowBasedChecksum1.update(Long.box(90), Long.box(100))
      assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

      rowBasedChecksum1.update(Long.box(30), Long.box(40))
      rowBasedChecksum1.update(Long.box(70), Long.box(80))
      assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

      rowBasedChecksum1.update(Long.box(50), Long.box(60))
      rowBasedChecksum1.update(Long.box(50), Long.box(60))
      assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

      rowBasedChecksum1.update(Long.box(70), Long.box(80))
      rowBasedChecksum2.update(Long.box(30), Long.box(40))
      assert(rowBasedChecksum1.getValue != rowBasedChecksum2.getValue)

      rowBasedChecksum1.update(Long.box(90), Long.box(100))
      rowBasedChecksum2.update(Long.box(10), Long.box(20))
      assert(rowBasedChecksum1.getValue == rowBasedChecksum2.getValue)

      assert(rowBasedChecksum1.getValue != 0)
      assert(rowBasedChecksum2.getValue != 0)
    }
  }
}

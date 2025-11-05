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

package org.apache.spark.sql.util

import org.apache.spark.SparkFunSuite

class GeoWkbReaderSuite extends SparkFunSuite {
  // scalastyle:off line.size.limit
  val testData: Seq[(String, String, String)] = Seq(
    (
      // POINT EMPTY
      "0101000000000000000000f87f000000000000f87f",
      "00000000017ff80000000000007ff8000000000000",
      "0101000000000000000000f87f000000000000f87f"
    ),
    (
      // POINT(1 2)
      "0101000000000000000000f03f0000000000000040",
      "00000000013ff00000000000004000000000000000",
      "0101000000000000000000f03f0000000000000040"
    ),
    (
      // POINTZ(1 2 3)
      "01e9030000000000000000f03f00000000000000400000000000000840",
      "00000003e93ff000000000000040000000000000004008000000000000",
      "0101000080000000000000f03f00000000000000400000000000000840"
    ),
    (
      // POINTM(1 2 3)
      "01d1070000000000000000f03f00000000000000400000000000000840",
      "00000007d13ff000000000000040000000000000004008000000000000",
      "0101000080000000000000f03f00000000000000400000000000000840"
    ),
    (
      // POINTZM(1 2 3 4)
      "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040",
      "0000000bb93ff0000000000000400000000000000040080000000000004010000000000000",
      "01010000c0000000000000f03f000000000000004000000000000008400000000000001040"
    ),
    (
      // LINESTRING EMPTY
      "010200000000000000",
      "000000000200000000",
      "010200000000000000"
    ),
    (
      // LINESTRING(0 0, 1 0, 1 1, 0 1, 0 0)
      "01020000000500000000000000000000000000000000000000000000000000f03f0000000000000000000000000000f03f000000000000f03f0000000000000000000000000000f03f00000000000000000000000000000000",
      "000000000200000005000000000000000000000000000000003ff000000000000000000000000000003ff00000000000003ff000000000000000000000000000003ff000000000000000000000000000000000000000000000",
      "01020000000500000000000000000000000000000000000000000000000000f03f0000000000000000000000000000f03f000000000000f03f0000000000000000000000000000f03f00000000000000000000000000000000"
    ),
  )
  // scalastyle:on line.size.limit

  // WKB reader.
  test("Read WKB - NDR") {
    // NDR encoded geometry inputs.
    testData.foreach { case (wkbNDR, _, resultWkb) =>
      val wkbBytes = wkbNDR.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
      val geometry = GeoWkbReader.readWkb(wkbBytes)
      assert(geometry != null)
      val resultBytes = resultWkb.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
      assert(resultBytes.sameElements(geometry))
    }
  }

  test("Read WKB - XDR") {
    // NDR encoded geometry inputs.
    testData.foreach { case (_, wkbXDR, resultWkb) =>
      val wkbBytes = wkbXDR.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
      val geometry = GeoWkbReader.readWkb(wkbBytes)
      assert(geometry != null)
      val resultBytes = resultWkb.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
      assert(resultBytes.sameElements(geometry))
    }
  }

}

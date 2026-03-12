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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{st_asbinary, st_geogfromwkb, st_geomfromwkb}
import org.apache.spark.sql.test.SharedSparkSession

class ParquetGeoSuite
    extends ParquetCompatibilityTest
    with SharedSparkSession {

  import testImplicits._

  /**
   * Writes the given WKB byte arrays to Parquet files in geometry and geography columns.
   */
  private def writeParquetFiles(dir: File, wkbValues: Seq[Array[Byte]]): Unit = {
    val df = wkbValues
      .toDF("wkb")
      .select(st_geomfromwkb($"wkb").as("geom"), st_geogfromwkb($"wkb").as("geog"))
    df.write.mode("overwrite").parquet(dir.getAbsolutePath)
  }

  /**
   * Reads all parquet files from the given directory and returns a DataFrame with WKB
   * representation of `geom` and `geog` columns.
   */
  private def readParquetFiles(dir: File): DataFrame = {
    spark.read
      .parquet(dir.getAbsolutePath)
      .select(st_asbinary($"geom"), st_asbinary($"geog"))
  }

  /**
   * Writes the given WKB byte arrays to Parquet files in both geometry and geography columns.
   * Reads them back checking that the read values match the original WKB values.
   */
  private def testReadWrite(wkbValues: Seq[Array[Byte]]): Unit = {
    withTempDir { dir =>
      withAllParquetWriters {
        writeParquetFiles(dir, wkbValues)

        withAllParquetReaders {
          checkAnswer(readParquetFiles(dir), wkbValues.map(wkb => Row(wkb, wkb)))
        }
      }
    }
  }

  /** Common WKB test values used across multiple tests. */
  private val point0Wkb = makePointWkb(0, 0) // POINT(0 0)
  private val point1Wkb = makePointWkb(1, 1) // POINT(1 1)
  private val line0Wkb = makeLineStringWkb((0, 0), (1, 1)) // LINESTRING(0 0,1 1)
  private val line1Wkb = makeLineStringWkb((1, 1), (2, 2)) // LINESTRING(1 1,2 2)
  private val line2Wkb = makeLineStringWkb((2, 2), (3, 3)) // LINESTRING(2 2,3 3)
  // POLYGON((0 0,1 1,2 2,0 0))
  private val polygon0Wkb = makePolygonWkb((0, 0), (1, 1), (2, 2), (0, 0))
  // POLYGON((3 3,4 4,5 5,3 3))
  private val polygon3Wkb = makePolygonWkb((3, 3), (4, 4), (5, 5), (3, 3))
  // MULTIPOINT((0 0),(1 1))
  private val multiPointWkb = makeMultiWkb(4, point0Wkb, point1Wkb)
  // MULTILINESTRING((0 0,1 1),(2 2,3 3))
  private val multiLineStringWkb = makeMultiWkb(5, line0Wkb, line2Wkb)
  // MULTIPOLYGON(((0 0,1 1,2 2,0 0)),((3 3,4 4,5 5,3 3)))
  private val multiPolygonWkb = makeMultiWkb(6, polygon0Wkb, polygon3Wkb)
  // GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 1,2 2))
  private val geometryCollectionWkb = makeMultiWkb(7, point0Wkb, line1Wkb)

  test("basic read and write") {
    testReadWrite(Seq(point0Wkb))
    testReadWrite(Seq(line0Wkb))
    testReadWrite(Seq(polygon0Wkb))
    testReadWrite(Seq(multiPointWkb))
    testReadWrite(Seq(multiLineStringWkb))
    testReadWrite(Seq(multiPolygonWkb))
    testReadWrite(Seq(geometryCollectionWkb))
    // Test writing multiple values at once.
    testReadWrite(Seq(point1Wkb, line1Wkb, makePolygonWkb()))
  }

  test("dictionary encoding") {
    val wkbValues = Seq(
      point0Wkb,
      line0Wkb,
      polygon0Wkb,
      multiPointWkb,
      multiLineStringWkb,
      multiPolygonWkb,
      geometryCollectionWkb
    )

    // Repeat the values to ensure that we have a large enough dataset to test
    // the dictionary encoding.
    val repeatedValues = List.fill(10000)(wkbValues).flatten

    Seq(true, false).foreach { useDictionary =>
      withSQLConf(ParquetOutputFormat.ENABLE_DICTIONARY -> useDictionary.toString) {
        testReadWrite(repeatedValues)
      }
    }
  }
}

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

package org.apache.spark.sql

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class STFunctionsSuite extends SharedSparkSession {

  import testImplicits._

  /** ST reader/writer expressions. */

  test("st_asbinary") {
    // Test data: Well-Known Binary (WKB) representations.
    val wkbNdr = "0101000000000000000000f03f0000000000000040"
    val wkbXdr = "00000000013ff00000000000004000000000000000"
    val df = Seq[(String, String, String, String)](
        (wkbNdr, wkbXdr, "NDR", "XDR")
      ).toDF("wkbNDR", "wkbXDR", "endNDR", "endXDR")
    // ST_GeogFromWKB/ST_GeomFromWKB and ST_AsBinary.
    checkAnswer(
      df.select(
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkbNDR"))))),
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkbNDR")), "NDR"))),
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkbNDR")), $"endNDR"))),
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkbNDR")), "XDR"))),
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkbNDR")), $"endXDR"))),
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkbXDR"))))),
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkbXDR")), "NDR"))),
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkbXDR")), $"endNDR"))),
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkbXDR")), "XDR"))),
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkbXDR")), $"endXDR"))),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkbNDR"))))),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkbNDR")), "NDR"))),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkbNDR")), $"endNDR"))),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkbNDR")), "XDR"))),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkbNDR")), $"endXDR"))),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkbXDR"))))),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkbXDR")), "NDR"))),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkbXDR")), $"endNDR"))),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkbXDR")), "XDR"))),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkbXDR")), $"endXDR")))),
      Row(
        wkbNdr, wkbNdr, wkbNdr, wkbXdr, wkbXdr, wkbNdr, wkbNdr, wkbNdr, wkbXdr, wkbXdr,
        wkbNdr, wkbNdr, wkbNdr, wkbXdr, wkbXdr, wkbNdr, wkbNdr, wkbNdr, wkbXdr, wkbXdr))
  }

  test("st_geogfromwkb") {
    // Test data: Well-Known Binary (WKB) representations.
    val df = Seq[(String)](
      (
        "0101000000000000000000f03f0000000000000040"
      )).toDF("wkb")
    // ST_GeogFromWKB.
    checkAnswer(
      df.select(lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkb"))))).as("col0")),
      Row("0101000000000000000000f03f0000000000000040"))
    // ST_GeogFromWKB with invalid WKB.
    val df_invalid = Seq(Array[Byte](111)).toDF("wkb")
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        df_invalid.select(st_geogfromwkb($"wkb")).collect()
      },
      condition = "WKB_PARSE_ERROR",
      parameters = Map("parseError" -> "Unexpected end of WKB buffer", "pos" -> "0")
    )
  }

  test("st_geomfromwkb") {
    // Test data: Well-Known Binary (WKB) representations.
    val df = Seq[(String, Int)](
      (
        "0101000000000000000000f03f0000000000000040", 4326
      )).toDF("wkb", "srid")
    // ST_GeomFromWKB.
    checkAnswer(
      df.select(
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkb"))))).as("col0"),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkb"), $"srid")))).as("col1"),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkb"), 4326)))).as("col1")),
      Row(
        "0101000000000000000000f03f0000000000000040",
        "0101000000000000000000f03f0000000000000040",
        "0101000000000000000000f03f0000000000000040"))
    // ST_GeomFromWKB with invalid SRID.
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        df.select(st_geomfromwkb(unhex($"wkb"), lit(-1))).collect()
      },
      condition = "ST_INVALID_SRID_VALUE",
      parameters = Map("srid" -> "-1")
    )
    // ST_GeomFromWKB with invalid WKB.
    val df_invalid = Seq(Array[Byte](111)).toDF("wkb")
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        df_invalid.select(st_geomfromwkb($"wkb")).collect()
      },
      condition = "WKB_PARSE_ERROR",
      parameters = Map("parseError" -> "Unexpected end of WKB buffer", "pos" -> "0")
    )
  }

  /** ST accessor expressions. */

  test("st_srid") {
    // Test data: Well-Known Binary (WKB) representations.
    val df = Seq[(String)](
      (
        "0101000000000000000000f03f0000000000000040"
      )).toDF("wkb")
    // ST_GeogFromWKB/ST_GeomFromWKB and ST_Srid.
    checkAnswer(
      df.select(
        st_srid(st_geogfromwkb(unhex($"wkb"))).as("col0"),
        st_srid(st_geomfromwkb(unhex($"wkb"))).as("col1"),
        st_srid(st_geomfromwkb(unhex($"wkb"), 4326)).as("col1")),
      Row(4326, 0, 4326))
  }

  /** ST modifier expressions. */

  test("st_setsrid") {
    // Test data: Well-Known Binary (WKB) representations.
    val df = Seq[(String, Int)](
      (
        "0101000000000000000000f03f0000000000000040", 4326
      )).toDF("wkb", "srid")
    // ST_GeogFromWKB/ST_GeomFromWKB and ST_Srid.
    checkAnswer(
      df.select(
        st_srid(st_setsrid(st_geogfromwkb(unhex($"wkb")), $"srid")).as("col0"),
        st_srid(st_setsrid(st_geomfromwkb(unhex($"wkb")), $"srid")).as("col1"),
        st_srid(st_setsrid(st_geomfromwkb(unhex($"wkb")), 4326)).as("col1"),
        st_srid(st_setsrid(st_geomfromwkb(unhex($"wkb")), 4326)).as("col1")),
      Row(4326, 4326, 4326, 4326))
  }

  /** Geospatial feature is disabled. */

  test("verify that geospatial functions are disabled when the config is off") {
    withSQLConf(SQLConf.GEOSPATIAL_ENABLED.key -> "false") {
      val df = Seq[String](null).toDF("col")
      Seq(
        st_asbinary(lit(null)).as("res"),
        st_geogfromwkb(lit(null)).as("res"),
        st_geomfromwkb(lit(null)).as("res"),
        st_srid(lit(null)).as("res"),
        st_setsrid(lit(null), lit(null)).as("res")
      ).foreach { func =>
        checkError(
          exception = intercept[AnalysisException] {
            df.select(func).collect()
          },
          condition = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED"
        )
      }
    }
  }

}

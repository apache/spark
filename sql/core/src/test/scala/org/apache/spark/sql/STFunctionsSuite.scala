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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class STFunctionsSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  /** ST reader/writer expressions. */

  test("st_asbinary") {
    // Test data: Well-Known Binary (WKB) representations.
    val df = Seq[(String)](
      (
        "0101000000000000000000f03f0000000000000040"
      )).toDF("wkb")
    // ST_GeogFromWKB/ST_GeomFromWKB and ST_AsBinary.
    checkAnswer(
      df.select(
        lower(hex(st_asbinary(st_geogfromwkb(unhex($"wkb"))))).as("col0"),
        lower(hex(st_asbinary(st_geomfromwkb(unhex($"wkb"))))).as("col1")),
      Row(
        "0101000000000000000000f03f0000000000000040",
        "0101000000000000000000f03f0000000000000040"))
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
        st_srid(st_geomfromwkb(unhex($"wkb"))).as("col1")),
      Row(4326, 0))
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

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

}

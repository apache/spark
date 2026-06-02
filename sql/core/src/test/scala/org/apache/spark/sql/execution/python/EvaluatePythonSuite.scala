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

package org.apache.spark.sql.execution.python

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.util.STUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.BinaryView

class EvaluatePythonSuite extends SparkFunSuite {

  // POINT(1 2) in WKB, little-endian.
  private val pointWkb: Array[Byte] = "010100000000000000000031400000000000001C40"
    .grouped(2).map(Integer.parseInt(_, 16).toByte).toArray

  private def pyGeo(srid: Int, wkb: Array[Byte]): java.util.HashMap[String, Any] = {
    val m = new java.util.HashMap[String, Any]()
    m.put("srid", srid)
    m.put("wkb", wkb)
    m
  }

  // ----- GeographyType -----

  test("makeFromJava(GeographyType): preserves per-row SRID for fixed-SRID columns") {
    // Geography supports a variety of geographic SRIDs beyond the default 4326. Ensure that the
    // SRID is preserved on the Python -> Catalyst conversion path.
    Seq(4267, 4269, 4326, 4612, 37001, 104030).foreach { srid =>
      val convert = EvaluatePython.makeFromJava(GeographyType(srid))
      val result = convert(pyGeo(srid, pointWkb))
      assert(result.isInstanceOf[BinaryView])
      assert(STUtils.stGeogSrid(result.asInstanceOf[BinaryView]) === srid)
    }
  }

  test("makeFromJava(GeographyType ANY): preserves per-row SRID for mixed-SRID columns") {
    val convert = EvaluatePython.makeFromJava(GeographyType("ANY"))
    Seq(4267, 4269, 4326).foreach { srid =>
      val result = convert(pyGeo(srid, pointWkb))
      assert(result.isInstanceOf[BinaryView])
      assert(STUtils.stGeogSrid(result.asInstanceOf[BinaryView]) === srid)
    }
  }

  test("makeFromJava(GeographyType): rejects SRID mismatch on a fixed-SRID column") {
    val convert = EvaluatePython.makeFromJava(GeographyType(4326))
    checkError(
      exception = intercept[SparkRuntimeException] {
        convert(pyGeo(4267, pointWkb))
      },
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOGRAPHY", "valueSrid" -> "4267", "typeSrid" -> "4326"))
  }

  test("makeFromJava(GeographyType ANY): rejects non-geographic SRID") {
    val convert = EvaluatePython.makeFromJava(GeographyType("ANY"))
    // SRID 0 is not a geographic SRID; even mixed-SRID columns must reject it.
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        convert(pyGeo(0, pointWkb))
      },
      condition = "ST_INVALID_SRID_VALUE",
      parameters = Map("srid" -> "0"))
    // SRID 3857 is a valid Cartesian SRID but not geographic.
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        convert(pyGeo(3857, pointWkb))
      },
      condition = "ST_INVALID_SRID_VALUE",
      parameters = Map("srid" -> "3857"))
  }

  test("makeFromJava(GeographyType): null is preserved") {
    val convert = EvaluatePython.makeFromJava(GeographyType(4326))
    assert(convert(null) === null)
  }

  // ----- GeometryType -----

  test("makeFromJava(GeometryType): preserves per-row SRID for fixed-SRID columns") {
    // Geometry supports both the default SRID 0 and a variety of Cartesian/geographic SRIDs.
    Seq(0, 3857, 4267, 4269, 4326, 32601, 102964).foreach { srid =>
      val convert = EvaluatePython.makeFromJava(GeometryType(srid))
      val result = convert(pyGeo(srid, pointWkb))
      assert(result.isInstanceOf[BinaryView])
      assert(STUtils.stGeomSrid(result.asInstanceOf[BinaryView]) === srid)
    }
  }

  test("makeFromJava(GeometryType ANY): preserves per-row SRID for mixed-SRID columns") {
    val convert = EvaluatePython.makeFromJava(GeometryType("ANY"))
    Seq(0, 3857, 4267, 4269, 4326).foreach { srid =>
      val result = convert(pyGeo(srid, pointWkb))
      assert(result.isInstanceOf[BinaryView])
      assert(STUtils.stGeomSrid(result.asInstanceOf[BinaryView]) === srid)
    }
  }

  test("makeFromJava(GeometryType): rejects SRID mismatch on a fixed-SRID column") {
    val convert = EvaluatePython.makeFromJava(GeometryType(0))
    checkError(
      exception = intercept[SparkRuntimeException] {
        convert(pyGeo(4326, pointWkb))
      },
      condition = "GEO_ENCODER_SRID_MISMATCH_ERROR",
      parameters = Map("type" -> "GEOMETRY", "valueSrid" -> "4326", "typeSrid" -> "0"))
  }

  test("makeFromJava(GeometryType ANY): rejects unsupported SRID") {
    val convert = EvaluatePython.makeFromJava(GeometryType("ANY"))
    // SRID 1 is not a registered SRID, so even mixed-SRID columns must reject it.
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        convert(pyGeo(1, pointWkb))
      },
      condition = "ST_INVALID_SRID_VALUE",
      parameters = Map("srid" -> "1"))
  }

  test("makeFromJava(GeometryType): null is preserved") {
    val convert = EvaluatePython.makeFromJava(GeometryType(0))
    assert(convert(null) === null)
  }
}

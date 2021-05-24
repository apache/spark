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


package org.apache.spark.resource

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkException, SparkFunSuite}

class ResourceInformationSuite extends SparkFunSuite {
  test("ResourceInformation.parseJson for valid JSON") {
    val json1 = compact(render(("name" -> "p100") ~ ("addresses" -> Seq("0", "1"))))
    val info1 = ResourceInformation.parseJson(json1)
    assert(info1.name === "p100")
    assert(info1.addresses === Array("0", "1"))

    // Currently we allow empty addresses.
    val json2 = compact(render("name" -> "fpga"))
    val info2 = ResourceInformation.parseJson(json2)
    assert(info2.name === "fpga")
    assert(info2.addresses.isEmpty)

    val json3 = compact(render("addresses" -> Seq("0")))
    val json4 = "invalid_json"
    for (invalidJson <- Seq(json3, json4)) {
      val ex = intercept[SparkException] {
        print(ResourceInformation.parseJson(invalidJson))
      }
      assert(ex.getMessage.contains("Error parsing JSON into ResourceInformation"),
        "Error message should provide context.")
      assert(ex.getMessage.contains(invalidJson), "Error message should include input json.")
    }
  }

  test("ResourceInformation.equals/hashCode") {
    val a1 = new ResourceInformation("a", addresses = Array("0"))
    val a21 = new ResourceInformation("a", addresses = Array("0", "1"))
    val a22 = new ResourceInformation("a", addresses = Array("0", "1"))
    val b2 = new ResourceInformation("b", addresses = Array("0", "1"))
    object A2 extends ResourceInformation("a", Array("0", "1"))
    assert(a1.equals(null) === false)
    assert(a1.equals(a1))
    assert(a1.equals(a21) === false)
    assert(a21.equals(a22) && a21.hashCode() === a22.hashCode())
    assert(a21.equals(b2) === false)
    assert(a21.equals(A2) === false)
  }
}

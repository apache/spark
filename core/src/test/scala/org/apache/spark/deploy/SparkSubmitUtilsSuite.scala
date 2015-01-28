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

package org.apache.spark.deploy

import org.apache.spark.util.ResetSystemProperties
import org.scalatest.{Matchers, FunSuite}

class SparkSubmitUtilsSuite extends FunSuite with Matchers with ResetSystemProperties {

  def beforeAll() {
    System.setProperty("spark.testing", "true")
  }

  test("incorrect maven coordinate throws error") {
    val coordinates = Seq("a:b: ", " :a:b", "a: :b", "a:b:", ":a:b", "a::b", "::", "a:b", "a")
    for (coordinate <- coordinates) {
      intercept[IllegalArgumentException] {
        SparkSubmitUtils.resolveMavenCoordinates(coordinate, null, null, true)
      }
    }
  }

  test("dependency not found throws RuntimeException") {
    intercept[RuntimeException] {
      SparkSubmitUtils.resolveMavenCoordinates("a:b:c", null, null, true)
    }
  }

  test("neglects Spark and Spark's dependencies") {
    val path = SparkSubmitUtils.resolveMavenCoordinates(
      "org.apache.spark:spark-core_2.10:1.2.0", null, null, true)
    assert(path == "", "should return empty path")
  }

  test("search for artifact at other repositories") {
    val path = SparkSubmitUtils.resolveMavenCoordinates("com.agimatec:agimatec-validation:0.9.3",
      "https://oss.sonatype.org/content/repositories/agimatec/", null, true)
    assert(path.indexOf("agimatec-validation") >= 0, "should find package. If it doesn't, check" +
      "if package still exists. If it has been removed, replace the example in this test.")
  }

  test("ivy path works correctly") {
    val ivyPath = "dummy/ivy"
    val jarPath = SparkSubmitUtils.resolveMavenCoordinates(
      "com.databricks:spark-csv_2.10:0.1", null, ivyPath, true)
    assert(jarPath.indexOf(ivyPath) >= 0, "should use non-default ivy path")
  }
}

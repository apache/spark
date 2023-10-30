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

package org.apache.spark.util

import java.net.URI

import org.apache.spark.SparkFunSuite

class DependencyUtilsSuite extends SparkFunSuite {

  test("SPARK-33084: Add jar support Ivy URI -- test invalid ivy uri") {
    val e1 = intercept[IllegalArgumentException] {
      DependencyUtils.resolveMavenDependencies(URI.create("ivy://"))
    }.getMessage
    assert(e1.contains("Expected authority at index 6: ivy://"))

    val e2 = intercept[IllegalArgumentException] {
      DependencyUtils.resolveMavenDependencies(URI.create("ivy://org.apache.test:test-test"))
    }.getMessage
    assert(e2.contains("Invalid Ivy URI authority in uri ivy://org.apache.test:test-test: " +
      "Expected 'org:module:version', found org.apache.test:test-test."))

    val e3 = intercept[IllegalArgumentException] {
      DependencyUtils.resolveMavenDependencies(
        URI.create("ivy://org.apache.test:test-test:1.0.0?foo="))
    }.getMessage
    assert(e3.contains("Invalid query string in Ivy URI " +
      "ivy://org.apache.test:test-test:1.0.0?foo=:"))

    val e4 = intercept[IllegalArgumentException] {
      DependencyUtils.resolveMavenDependencies(
        URI.create("ivy://org.apache.test:test-test:1.0.0?bar=&baz=foo"))
    }.getMessage
    assert(e4.contains("Invalid query string in Ivy URI " +
      "ivy://org.apache.test:test-test:1.0.0?bar=&baz=foo: bar=&baz=foo"))

    val e5 = intercept[IllegalArgumentException] {
      DependencyUtils.resolveMavenDependencies(
        URI.create("ivy://org.apache.test:test-test:1.0.0?exclude=org.apache"))
    }.getMessage
    assert(e5.contains("Invalid exclude string in Ivy URI " +
      "ivy://org.apache.test:test-test:1.0.0?exclude=org.apache: " +
      "expected 'org:module,org:module,..', found org.apache"))
  }

  test("SPARK-39501: Resolve maven dependenicy in IPv6") {
    assume(Utils.preferIPv6)
    DependencyUtils.resolveMavenDependencies(
      URI.create("ivy://org.apache.logging.log4j:log4j-api:2.17.2"))
  }
}

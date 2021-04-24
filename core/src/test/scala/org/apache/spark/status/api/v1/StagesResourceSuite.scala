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
package org.apache.spark.status.api.v1

import java.util.Arrays
import javax.ws.rs.core.MultivaluedHashMap

import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkFunSuite

class StagesResourceSuite extends SparkFunSuite with Matchers {

  test("SPARK-33195: Avoid stages/stage encoding URL twice") {
    val stageResource = new StagesResource()

    // parameters not encoded
    val queryParameters1 = new MultivaluedHashMap[String, String]()
    queryParameters1.put("search[value]", Arrays.asList("boo"))
    queryParameters1.put("order[0][column]", Arrays.asList("1"))
    queryParameters1.put("order[0][dir]", Arrays.asList("desc"))
    assert(stageResource.encodeKeyAndGetValue(
      queryParameters1, "search[value]", "foo") == "boo")
    assert(stageResource.encodeKeyAndGetValue(
      queryParameters1, "order[0][column]", "0") == "1")
    assert(stageResource.encodeKeyAndGetValue(
      queryParameters1, "order[0][dir]", "asc") == "desc")

    // parameters encoded once
    val queryParameters2 = new MultivaluedHashMap[String, String]()
    queryParameters2.put("search%5Bvalue%5D", Arrays.asList("boo"))
    queryParameters2.put("order%5B0%5D%5Bcolumn%5D", Arrays.asList("1"))
    queryParameters2.put("order%5B0%5D%5Bdir%5D", Arrays.asList("desc"))
    assert(stageResource.encodeKeyAndGetValue(
      queryParameters2, "search[value]", "foo") == "boo")
    assert(stageResource.encodeKeyAndGetValue(
      queryParameters2, "order[0][column]", "0") == "1")
    assert(stageResource.encodeKeyAndGetValue(
      queryParameters2, "order[0][dir]", "asc") == "desc")

    // parameters encoded twice
    val queryParameters3 = new MultivaluedHashMap[String, String]()
    queryParameters3.put("search%255Bvalue%255D", Arrays.asList("boo"))
    queryParameters3.put("order%255B0%255D%255Bcolumn%255D", Arrays.asList("1"))
    queryParameters3.put("order%255B0%255D%255Bdir%255D", Arrays.asList("desc"))
    assert(stageResource.encodeKeyAndGetValue(
      queryParameters3, "search[value]", "foo") == "boo")
    assert(stageResource.encodeKeyAndGetValue(
      queryParameters3, "order[0][column]", "0") == "1")
    assert(stageResource.encodeKeyAndGetValue(
      queryParameters3, "order[0][dir]", "asc") == "desc")
  }
}

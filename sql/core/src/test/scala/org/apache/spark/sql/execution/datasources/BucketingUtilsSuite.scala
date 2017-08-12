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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.SparkFunSuite

class BucketingUtilsSuite extends SparkFunSuite {

  test("generate bucket id") {
    assert(BucketingUtils.bucketIdToString(0) == "_00000")
    assert(BucketingUtils.bucketIdToString(10) == "_00010")
    assert(BucketingUtils.bucketIdToString(999999) == "_999999")
  }

  test("match bucket ids") {
    def testCase(filename: String, expected: Option[Int]): Unit = withClue(s"name: $filename") {
      assert(BucketingUtils.getBucketId(filename) == expected)
    }

    testCase("a_1", Some(1))
    testCase("a_1.txt", Some(1))
    testCase("a_9999999", Some(9999999))
    testCase("a_9999999.txt", Some(9999999))
    testCase("a_1.c2.txt", Some(1))
    testCase("a_1.", Some(1))

    testCase("a_1:txt", None)
    testCase("a_1-c2.txt", None)
  }

}

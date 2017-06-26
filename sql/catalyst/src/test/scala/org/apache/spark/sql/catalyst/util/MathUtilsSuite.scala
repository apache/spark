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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.MathUtils._

class MathUtilsSuite extends SparkFunSuite {

  test("widthBucket") {
    assert(widthBucket(5.35, 0.024, 10.06, 5) === 3)
    assert(widthBucket(0, 1, 1, 1) === 0)
    assert(widthBucket(20, 1, 1, 1) === 2)

    // Test https://docs.oracle.com/cd/B28359_01/olap.111/b28126/dml_functions_2137.htm#OLADM717
    // WIDTH_BUCKET(credit_limit, 100, 5000, 10)
    assert(widthBucket(500, 100, 5000, 10) === 1)
    assert(widthBucket(2300, 100, 5000, 10) === 5)
    assert(widthBucket(3500, 100, 5000, 10) === 7)
    assert(widthBucket(1200, 100, 5000, 10) === 3)
    assert(widthBucket(1400, 100, 5000, 10) === 3)
    assert(widthBucket(700, 100, 5000, 10) === 2)
    assert(widthBucket(5000, 100, 5000, 10) === 11)
    assert(widthBucket(1800, 100, 5000, 10) === 4)
    assert(widthBucket(400, 100, 5000, 10) === 1)

    // minValue == maxValue
    assert(widthBucket(10, 4, 4, 15) === 16)

    // numBucket <= 0
    intercept[AnalysisException]{
      assert(widthBucket(100, 100, 5000, -1) === 1)
    }
  }
}

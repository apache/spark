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
    assert(widthBucket(99, 100, 5000, 10) === 0)
    assert(widthBucket(100, 100, 5000, 10) === 1)
    assert(widthBucket(590, 100, 5000, 10) === 2)
    assert(widthBucket(5000, 100, 5000, 10) === 11)
    assert(widthBucket(6000, 100, 5000, 10) === 11)
    intercept[AnalysisException]{
      assert(widthBucket(100, 100, 5000, -1) === 1)
    }
  }
}

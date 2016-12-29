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

import org.apache.spark.SparkFunSuite

class VersionUtilsSuite extends SparkFunSuite {

  import org.apache.spark.util.VersionUtils._

  test("Parse Spark major version") {
    assert(majorVersion("2.0") === 2)
    assert(majorVersion("12.10.11") === 12)
    assert(majorVersion("2.0.1-SNAPSHOT") === 2)
    assert(majorVersion("2.0.x") === 2)
    withClue("majorVersion parsing should fail for invalid major version number") {
      intercept[IllegalArgumentException] {
        majorVersion("2z.0")
      }
    }
    withClue("majorVersion parsing should fail for invalid minor version number") {
      intercept[IllegalArgumentException] {
        majorVersion("2.0z")
      }
    }
  }

  test("Parse Spark minor version") {
    assert(minorVersion("2.0") === 0)
    assert(minorVersion("12.10.11") === 10)
    assert(minorVersion("2.0.1-SNAPSHOT") === 0)
    assert(minorVersion("2.0.x") === 0)
    withClue("minorVersion parsing should fail for invalid major version number") {
      intercept[IllegalArgumentException] {
        minorVersion("2z.0")
      }
    }
    withClue("minorVersion parsing should fail for invalid minor version number") {
      intercept[IllegalArgumentException] {
        minorVersion("2.0z")
      }
    }
  }

  test("Parse Spark major and minor versions") {
    assert(majorMinorVersion("2.0") === (2, 0))
    assert(majorMinorVersion("12.10.11") === (12, 10))
    assert(majorMinorVersion("2.0.1-SNAPSHOT") === (2, 0))
    assert(majorMinorVersion("2.0.x") === (2, 0))
    withClue("majorMinorVersion parsing should fail for invalid major version number") {
      intercept[IllegalArgumentException] {
        majorMinorVersion("2z.0")
      }
    }
    withClue("majorMinorVersion parsing should fail for invalid minor version number") {
      intercept[IllegalArgumentException] {
        majorMinorVersion("2.0z")
      }
    }
  }
}

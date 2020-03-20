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
import org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper._

class DateTimeFormatterHelperSuite extends SparkFunSuite {

  test("check incompatible pattern") {
    assert(convertIncompatiblePattern("MM-DD-u") === "MM-DD-e")
    assert(convertIncompatiblePattern("yyyy-MM-dd'T'HH:mm:ss.SSSz")
      === "uuuu-MM-dd'T'HH:mm:ss.SSSz")
    assert(convertIncompatiblePattern("yyyy-MM'y contains in quoted text'HH:mm:ss")
      === "uuuu-MM'y contains in quoted text'HH:mm:ss")
    assert(convertIncompatiblePattern("yyyy-MM-dd-u'T'HH:mm:ss.SSSz")
      === "uuuu-MM-dd-e'T'HH:mm:ss.SSSz")
    assert(convertIncompatiblePattern("yyyy-MM'u contains in quoted text'HH:mm:ss")
      === "uuuu-MM'u contains in quoted text'HH:mm:ss")
    assert(convertIncompatiblePattern("yyyy-MM'u contains in quoted text'''''HH:mm:ss")
      === "uuuu-MM'u contains in quoted text'''''HH:mm:ss")
    assert(convertIncompatiblePattern("yyyy-MM-dd'T'HH:mm:ss.SSSz G")
      === "yyyy-MM-dd'T'HH:mm:ss.SSSz G")
    unsupportedLetters.foreach { l =>
      val e = intercept[IllegalArgumentException](convertIncompatiblePattern(s"yyyy-MM-dd $l G"))
      assert(e.getMessage === s"Illegal pattern character: $l")
    }
    assert(convertIncompatiblePattern("yyyy-MM-dd uuuu") === "uuuu-MM-dd eeee")
    assert(convertIncompatiblePattern("yyyy-MM-dd EEEE") === "uuuu-MM-dd EEEE")
    assert(convertIncompatiblePattern("yyyy-MM-dd'e'HH:mm:ss") === "uuuu-MM-dd'e'HH:mm:ss")
    assert(convertIncompatiblePattern("yyyy-MM-dd'T'") === "uuuu-MM-dd'T'")
  }
}

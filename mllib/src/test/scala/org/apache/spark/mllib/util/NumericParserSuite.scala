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

package org.apache.spark.mllib.util

import org.apache.spark.{SparkException, SparkFunSuite}

class NumericParserSuite extends SparkFunSuite {

  test("parser") {
    val s = "((1.0,2e3),-4,[5e-6,7.0E8],+9)"
    val parsed = NumericParser.parse(s).asInstanceOf[Seq[_]]
    assert(parsed(0).asInstanceOf[Seq[_]] === Seq(1.0, 2.0e3))
    assert(parsed(1).asInstanceOf[Double] === -4.0)
    assert(parsed(2).asInstanceOf[Array[Double]] === Array(5.0e-6, 7.0e8))
    assert(parsed(3).asInstanceOf[Double] === 9.0)

    val malformatted = Seq("a", "[1,,]", "0.123.4", "1 2", "3+4")
    malformatted.foreach { s =>
      intercept[SparkException] {
        NumericParser.parse(s)
        throw new RuntimeException(s"Didn't detect malformatted string $s.")
      }
    }
  }

  test("parser with whitespaces") {
    val s = "(0.0, [1.0, 2.0])"
    val parsed = NumericParser.parse(s).asInstanceOf[Seq[_]]
    assert(parsed(0).asInstanceOf[Double] === 0.0)
    assert(parsed(1).asInstanceOf[Array[Double]] === Array(1.0, 2.0))
  }
}

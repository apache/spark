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

package org.apache.spark.sql.internal

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException

class VariableSubstitutionSuite extends SparkFunSuite {

  private lazy val conf = new SQLConf
  private lazy val sub = new VariableSubstitution(conf)

  test("system property") {
    System.setProperty("varSubSuite.var", "abcd")
    assert(sub.substitute("${system:varSubSuite.var}") == "abcd")
  }

  test("environmental variables") {
    assert(sub.substitute("${env:SPARK_TESTING}") == "1")
  }

  test("Spark configuration variable") {
    conf.setConfString("some-random-string-abcd", "1234abcd")
    assert(sub.substitute("${hiveconf:some-random-string-abcd}") == "1234abcd")
    assert(sub.substitute("${sparkconf:some-random-string-abcd}") == "1234abcd")
    assert(sub.substitute("${spark:some-random-string-abcd}") == "1234abcd")
    assert(sub.substitute("${some-random-string-abcd}") == "1234abcd")
  }

  test("multiple substitutes") {
    val q = "select ${bar} ${foo} ${doo} this is great"
    conf.setConfString("bar", "1")
    conf.setConfString("foo", "2")
    conf.setConfString("doo", "3")
    assert(sub.substitute(q) == "select 1 2 3 this is great")
  }

  test("test nested substitutes") {
    val q = "select ${bar} ${foo} this is great"
    conf.setConfString("bar", "1")
    conf.setConfString("foo", "${bar}")
    assert(sub.substitute(q) == "select 1 1 this is great")
  }

}

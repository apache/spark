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

package org.apache.spark

import org.scalatest.Matchers

class NamespaceSuite extends SparkFunSuite with Matchers {
  test("applying a namespace to env variable") {
    val ns = Namespace.Submission.envNamespace
    Namespace.Submission.envName("A_B_C") shouldBe "A_B_C"
    Namespace.Submission.envName("SPARK_A_B_C") shouldBe s"SPARK_${ns}_A_B_C"
  }

  test("applying a namespace to configuration property") {
    val ns = Namespace.Submission.confNamespace
    Namespace.Submission.confName("a.b.c") shouldBe "a.b.c"
    Namespace.Submission.confName("spark.a.b.c") shouldBe s"spark.$ns.a.b.c"
  }

  test("removing a namespace from SparkConf") {
    val ns = Namespace.Submission.confNamespace
    val conf = new SparkConf(loadDefaults = false)

    conf.set(s"spark.$ns.b.c", "1")
    conf.set(s"spark.$ns.b.d", "2")
    conf.set(s"spark.$ns.e", "3")
    conf.set(s"spark.$ns", "4")
    conf.set("spark.b.c", "5")
    conf.set(s"nonspark.$ns.b.c", "7")

    val conf2 = conf.fromNamespace(Namespace.Submission)
    conf2.getOption("spark.b.c") shouldBe Some("1")
    conf2.getOption("spark.b.d") shouldBe Some("2")
    conf2.getOption(s"spark.$ns.e") shouldBe Some("3")
    conf2.getOption(s"spark.$ns")shouldBe Some("4")
  }

  test("ID transformation with Blank namespace") {
    Namespace.Blank.envName("A_B_C") shouldBe "A_B_C"
    Namespace.Blank.envName("SPARK_A_B_C") shouldBe s"SPARK_A_B_C"
    Namespace.Blank.confName("a.b.c") shouldBe "a.b.c"
    Namespace.Blank.confName("spark.a.b.c") shouldBe s"spark.a.b.c"

    val conf = new SparkConf(loadDefaults = false)
    conf.set("a.b.c", "1")
    conf.set("spark.a.b.c", "2")

    val conf2 = Namespace.Blank.removeFrom(conf)
    conf2.getOption("a.b.c") shouldBe Some("1")
    conf2.getOption("spark.a.b.c") shouldBe Some("2")
    conf2.getAll should have size 2
  }
}

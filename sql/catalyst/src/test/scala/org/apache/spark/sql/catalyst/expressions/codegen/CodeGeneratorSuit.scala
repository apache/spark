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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.{SparkContext, SparkFunSuite, TestUtils}
import org.apache.spark.deploy.SparkSubmitSuite
import org.apache.spark.internal.Logging

class CodeGeneratorSuit extends SparkFunSuite {

  test("SPARK-23563: config codegen compile cache size in local-cluster") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)

    val argsForSparkSubmit = Seq(
      "--class", CodeGeneratorSuit.getClass.getName.stripSuffix("$"),
      "--name", "SPARK-23563",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.sql.codegen.compile.maxCacheSize=2",
      unusedJar.toString)
    SparkSubmitSuite.runSparkSubmit(argsForSparkSubmit, "../..")
  }

  test("SPARK-23563: config codegen compile cache size in local") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)

    val argsForSparkSubmit = Seq(
      "--class", CodeGeneratorSuit.getClass.getName.stripSuffix("$"),
      "--name", "SPARK-23563",
      "--master", "local[*]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.sql.codegen.compile.maxCacheSize=2",
      unusedJar.toString)
    SparkSubmitSuite.runSparkSubmit(argsForSparkSubmit, "../..")
  }
}

object CodeGeneratorSuit extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext()
    val s = CodeGenerator.maxCacheSize
    assert(s == 2)
    sparkContext.stop()
  }
}

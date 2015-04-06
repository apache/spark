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

package org.apache.spark.deploy.worker

import org.apache.spark.SparkConf
import org.apache.spark.deploy.Command

import org.scalatest.{Matchers, FunSuite}

class WorkerSuite extends FunSuite with Matchers {

  def cmd(javaOpts: String*) = Command("", Seq.empty, Map.empty, Seq.empty, Seq.empty, Seq(javaOpts:_*))
  def conf(opts: (String, String)*) = new SparkConf(loadDefaults = false).setAll(opts)

  test("test isUseLocalNodeSSLConfig") {
    Worker.isUseLocalNodeSSLConfig(cmd("-Dasdf=dfgh")) shouldBe false
    Worker.isUseLocalNodeSSLConfig(cmd("-Dspark.ssl.useNodeLocalConf=true")) shouldBe true
    Worker.isUseLocalNodeSSLConfig(cmd("-Dspark.ssl.useNodeLocalConf=false")) shouldBe false
    Worker.isUseLocalNodeSSLConfig(cmd("-Dspark.ssl.useNodeLocalConf=")) shouldBe false
  }

  test("test maybeUpdateSSLSettings") {
    Worker.maybeUpdateSSLSettings(
      cmd("-Dasdf=dfgh", "-Dspark.ssl.opt1=x"),
      conf("spark.ssl.opt1" -> "y", "spark.ssl.opt2" -> "z"))
        .javaOpts should contain theSameElementsInOrderAs Seq(
          "-Dasdf=dfgh", "-Dspark.ssl.opt1=x")

    Worker.maybeUpdateSSLSettings(
      cmd("-Dspark.ssl.useNodeLocalConf=false", "-Dspark.ssl.opt1=x"),
      conf("spark.ssl.opt1" -> "y", "spark.ssl.opt2" -> "z"))
        .javaOpts should contain theSameElementsInOrderAs Seq(
          "-Dspark.ssl.useNodeLocalConf=false", "-Dspark.ssl.opt1=x")

    Worker.maybeUpdateSSLSettings(
      cmd("-Dspark.ssl.useNodeLocalConf=true", "-Dspark.ssl.opt1=x"),
      conf("spark.ssl.opt1" -> "y", "spark.ssl.opt2" -> "z"))
        .javaOpts should contain theSameElementsAs Seq(
          "-Dspark.ssl.useNodeLocalConf=true", "-Dspark.ssl.opt1=y", "-Dspark.ssl.opt2=z")

  }
}

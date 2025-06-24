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

package org.apache.spark.deploy

import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers

import org.apache.spark.SparkUserAppException

class SparkPipelinesSuite extends SparkSubmitTestUtils with Matchers with BeforeAndAfterEach {
  test("only spark submit args") {
    val args = Array(
      "--remote",
      "local[2]",
      "--deploy-mode",
      "client",
      "--supervise",
      "--conf",
      "spark.conf1=2",
      "--conf",
      "spark.conf2=3"
    )
    assert(
      SparkPipelines.constructSparkSubmitArgs(args, sparkHome = "abc") ==
      Seq(
        "--deploy-mode",
        "client",
        "--supervise",
        "--conf",
        "spark.conf1=2",
        "--conf",
        "spark.conf2=3",
        "--conf",
        "spark.api.mode=connect",
        "--remote",
        "local[2]",
        "abc/python/pyspark/pipelines/cli.py"
      )
    )
  }

  test("only pipelines args") {
    val args = Array(
      "run",
      "--spec",
      "pipeline.yml"
    )
    assert(
      SparkPipelines.constructSparkSubmitArgs(args, sparkHome = "abc") ==
      Seq(
        "--conf",
        "spark.api.mode=connect",
        "--remote",
        "local",
        "abc/python/pyspark/pipelines/cli.py",
        "run",
        "--spec",
        "pipeline.yml"
      )
    )
  }

  test("spark-submit and pipelines args") {
    val args = Array(
      "--remote",
      "local[2]",
      "run",
      "--supervise",
      "--spec",
      "pipeline.yml",
      "--conf",
      "spark.conf2=3"
    )
    assert(
      SparkPipelines.constructSparkSubmitArgs(args, sparkHome = "abc") ==
      Seq(
        "--supervise",
        "--conf",
        "spark.conf2=3",
        "--conf",
        "spark.api.mode=connect",
        "--remote",
        "local[2]",
        "abc/python/pyspark/pipelines/cli.py",
        "run",
        "--spec",
        "pipeline.yml"
      )
    )
  }

  test("class arg prohibited") {
    val args = Array(
      "--class",
      "org.apache.spark.deploy.SparkPipelines"
    )
    intercept[SparkUserAppException] {
      SparkPipelines.constructSparkSubmitArgs(args, sparkHome = "abc")
    }
  }

  test("name arg") {
    val args = Array(
      "init",
      "--name",
      "myproject"
    )
    assert(
      SparkPipelines.constructSparkSubmitArgs(args, sparkHome = "abc") ==
        Seq(
          "--conf",
          "spark.api.mode=connect",
          "--remote",
          "local",
          "abc/python/pyspark/pipelines/cli.py",
          "init",
          "--name",
          "myproject"
        )
    )
  }
}

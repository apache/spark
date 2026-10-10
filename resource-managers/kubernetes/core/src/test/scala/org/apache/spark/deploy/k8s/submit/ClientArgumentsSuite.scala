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
package org.apache.spark.deploy.k8s.submit

import org.apache.spark.SparkFunSuite

class ClientArgumentsSuite extends SparkFunSuite {

  test("fromCommandLineArgs parses primary Java resource") {
    val args = ClientArguments.fromCommandLineArgs(
      Array("--primary-java-resource", "local:///path/to/app.jar", "--main-class", "my.Main"))
    assert(args.mainAppResource === JavaMainAppResource(Some("local:///path/to/app.jar")))
    assert(args.mainClass === "my.Main")
    assert(args.driverArgs === Array.empty[String])
    assert(args.proxyUser === None)
  }

  test("fromCommandLineArgs parses primary Python file") {
    val args = ClientArguments.fromCommandLineArgs(
      Array("--primary-py-file", "local:///path/to/app.py", "--main-class", "org.Main"))
    assert(args.mainAppResource === PythonMainAppResource("local:///path/to/app.py"))
  }

  test("fromCommandLineArgs parses primary R file") {
    val args = ClientArguments.fromCommandLineArgs(
      Array("--primary-r-file", "local:///path/to/app.R", "--main-class", "org.Main"))
    assert(args.mainAppResource === RMainAppResource("local:///path/to/app.R"))
  }

  test("fromCommandLineArgs defaults main app resource when none is specified") {
    val args = ClientArguments.fromCommandLineArgs(Array("--main-class", "my.Main"))
    assert(args.mainAppResource === JavaMainAppResource(None))
  }

  test("fromCommandLineArgs collects repeated --arg values in order") {
    val args = ClientArguments.fromCommandLineArgs(
      Array("--main-class", "my.Main", "--arg", "a", "--arg", "b", "--arg", "c"))
    assert(args.driverArgs === Array("a", "b", "c"))
  }

  test("fromCommandLineArgs parses proxy user") {
    val args = ClientArguments.fromCommandLineArgs(
      Array("--main-class", "my.Main", "--proxy-user", "alice"))
    assert(args.proxyUser === Some("alice"))
  }

  test("fromCommandLineArgs uses the last --main-class when specified more than once") {
    val args = ClientArguments.fromCommandLineArgs(
      Array("--main-class", "first.Main", "--main-class", "second.Main"))
    assert(args.mainClass === "second.Main")
  }

  test("fromCommandLineArgs throws when --main-class is missing") {
    val thrown = intercept[IllegalArgumentException] {
      ClientArguments.fromCommandLineArgs(Array("--arg", "a"))
    }
    assert(thrown.getMessage.contains("Main class must be specified via --main-class"))
  }

  test("fromCommandLineArgs throws when given no arguments at all") {
    val thrown = intercept[IllegalArgumentException] {
      ClientArguments.fromCommandLineArgs(Array.empty)
    }
    assert(thrown.getMessage.contains("Main class must be specified via --main-class"))
  }

  test("fromCommandLineArgs throws on an unrecognized flag") {
    val thrown = intercept[RuntimeException] {
      ClientArguments.fromCommandLineArgs(
        Array("--unknown-flag", "value", "--main-class", "my.Main"))
    }
    assert(thrown.getMessage.contains("Unknown arguments: --unknown-flag value"))
  }

  test("fromCommandLineArgs throws on a trailing flag with no value") {
    val thrown = intercept[RuntimeException] {
      ClientArguments.fromCommandLineArgs(Array("--main-class", "my.Main", "--arg"))
    }
    assert(thrown.getMessage.contains("Unknown arguments: --arg"))
  }
}

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

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class ClientSuite extends SparkFunSuite with Matchers {
  test("correctly validates driver jar URL's") {
    ClientArguments.isValidJarUrl("http://someHost:8080/foo.jar") should be (true)
    ClientArguments.isValidJarUrl("https://someHost:8080/foo.jar") should be (true)

    // file scheme with authority and path is valid.
    ClientArguments.isValidJarUrl("file://somehost/path/to/a/jarFile.jar") should be (true)

    // file scheme without path is not valid.
    // In this case, jarFile.jar is recognized as authority.
    ClientArguments.isValidJarUrl("file://jarFile.jar") should be (false)

    // file scheme without authority but with triple slash is valid.
    ClientArguments.isValidJarUrl("file:///some/path/to/a/jarFile.jar") should be (true)
    ClientArguments.isValidJarUrl("hdfs://someHost:1234/foo.jar") should be (true)

    ClientArguments.isValidJarUrl("hdfs://someHost:1234/foo") should be (false)
    ClientArguments.isValidJarUrl("/missing/a/protocol/jarfile.jar") should be (false)
    ClientArguments.isValidJarUrl("not-even-a-path.jar") should be (false)

    // This URI doesn't have authority and path.
    ClientArguments.isValidJarUrl("hdfs:someHost:1234/jarfile.jar") should be (false)

    // Invalid syntax.
    ClientArguments.isValidJarUrl("hdfs:") should be (false)
  }
}

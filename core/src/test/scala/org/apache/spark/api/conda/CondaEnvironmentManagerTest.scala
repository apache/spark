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
package org.apache.spark.api.conda

import java.nio.file.Files

import org.apache.spark.util.TempDirectory

class CondaEnvironmentManagerTest extends org.apache.spark.SparkFunSuite with TempDirectory {
  test("CondaEnvironmentManager.ensureExecutable") {
    val path = tempDir.toPath.resolve("myfile")
    Files.createFile(path)
    assert(!Files.isExecutable(path), "File shouldn't be executable initially")
    CondaEnvironmentManager.ensureExecutable(path.toString)
    assert(Files.isExecutable(path), "File should now be executable")
  }

  test("CondaEnvironmentManager.redactCredentials") {
    val original = "24u0f8 adfghjfouh https://:f35g35b_t5gbn.asfad3@my-host-name-5.foo.bar" +
      ".baz:12345/whatever/else"
    val redacted = "24u0f8 adfghjfouh https://:<password>@my-host-name-5.foo.bar" +
      ".baz:12345/whatever/else"
    assert(CondaEnvironmentManager.redactCredentials(original) == redacted)
  }

  test("CondaEnvironmentManager.redactTwoCredentials") {
    val original = "random:https://:creds1@x-5.bar/whatever/else][http://us_r:creds2@yy.bar:222"
    val redacted = "random:https://:<password>@x-5.bar/whatever/else]" +
      "[http://us_r:<password>@yy.bar:222"
    assert(CondaEnvironmentManager.redactCredentials(original) == redacted)
  }
}

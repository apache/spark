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

import java.io.File
import java.nio.file.Files

import org.apache.spark.{SparkFunSuite, SparkUserAppException}

class RRunnerSuite extends SparkFunSuite {

  test("RRunner main") {
    val tempRFile = File.createTempFile("test", ".R")
    Files.write(tempRFile.toPath,
      s"""
        quit()
      """.stripMargin.getBytes("utf8"))
    RRunner.main(Array(tempRFile.toURI.toString))

    val exception = intercept[SparkUserAppException] { RRunner.main(Array("nonexistent.R")) }
    assert(exception.exitCode === 2)
    assert(exception.message.nonEmpty)
    assert(exception.message.get.contains(
      "Fatal error: cannot open file 'nonexistent.R': No such file or directory"))
  }
}

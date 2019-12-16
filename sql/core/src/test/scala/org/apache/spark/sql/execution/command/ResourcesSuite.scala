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

package org.apache.spark.sql.execution.command

import java.io.File

import org.apache.spark.{SparkException, SparkFiles, SparkFunSuite}
import org.apache.spark.sql.SparkSession

class ResourcesSuite extends SparkFunSuite{

  val sparkSession = SparkSession.builder().master("local").appName("test_session").getOrCreate()

  test("Add Directory when ADD_DIRECTORY_USING_RECURSIVE not set to true") {
    withTempDir { dir =>
      val dirPath = dir.getAbsolutePath
        intercept[SparkException] {
          sparkSession.sql(s"ADD FILE $dir")
        }.getMessage.contains(s" Added file $dirPath is a directory and recursive is not turned on")
    }
  }

  test("Add Directory when ADD_DIRECTORY_USING_RECURSIVE set to true") {
    withTempDir { testDir =>
      val testFile1 = File.createTempFile("testFile", "1", testDir)
      sparkSession.sql("set spark.sql.addDirectory.recursive=true")
      sparkSession.sql(s"ADD FILE $testDir")
      val sep = File.separator
      if(!new File(SparkFiles.get(testDir.getName + sep + testFile1.getName)).exists()) {
        throw new SparkException("TestFile1 Not found.")
      }
    }
  }
}

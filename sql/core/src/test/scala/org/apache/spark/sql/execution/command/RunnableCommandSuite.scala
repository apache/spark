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

import org.apache.spark.{SparkException, SparkFiles}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class RunnableCommandSuite extends SQLTestUtils with SharedSparkSession {

  test("Add Directory when LEGACY_ADD_DIRECTORY_USING_RECURSIVE not set to true") {
    withTempDir { dir =>
      val msg = intercept[SparkException] {
          spark.sql(s"ADD FILE $dir")
        }.getMessage
      assert(msg.contains("is a directory and recursive is not turned on"))
    }
  }

  test("Add Directory when LEGACY_ADD_DIRECTORY_USING_RECURSIVE set to true") {
    withTempDir { testDir =>
      val testFile = File.createTempFile("testFile", "1", testDir)
      withSQLConf(SQLConf.LEGACY_ADD_DIRECTORY_USING_RECURSIVE.key -> "true") {
        spark.sql(s"ADD FILE $testDir")
        assert(new File(SparkFiles.get(s"${testDir.getName}/${testFile.getName}")).exists())
      }
    }
  }
}

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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SQLTestUtils

abstract class DataSourceCodecTest extends QueryTest with SQLTestUtils {

  protected def dataSourceName: String
  protected val codecConfigName: String
  protected def availableCodecs: Seq[String]

  def testWithAllCodecs(name: String)(f: => Unit): Unit = {
    for (codec <- availableCodecs) {
      test(s"$name - data source $dataSourceName - codec: $codec") {
        withSQLConf(codecConfigName -> codec) {
          f
        }
      }
    }
  }

  testWithAllCodecs("write and read - single partition") {
    withTempPath { dir =>
      testData
        .repartition(1)
        .write
        .format(dataSourceName)
        .save(dir.getCanonicalPath)

      val df = spark.read.format(dataSourceName).load(dir.getCanonicalPath)
      checkAnswer(df, testData)
    }
  }

  testWithAllCodecs("write and read") {
    withTempPath { dir =>
      testData
        .repartition(5)
        .write
        .format(dataSourceName)
        .save(dir.getCanonicalPath)

      val df = spark.read.format(dataSourceName).load(dir.getCanonicalPath)
      checkAnswer(df, testData)
    }
  }
}



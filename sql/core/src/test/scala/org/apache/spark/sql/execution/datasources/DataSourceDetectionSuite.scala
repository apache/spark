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

import java.io.File

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

class DataSourceDetectionSuite extends QueryTest with SharedSQLContext  {

  test("detect datasource - parquet (by magic number)") {
    val data = (1 to 10).map(i => (i, i.toString))
    withTempPath { file =>
      val path = file.getCanonicalPath
      sqlContext.createDataFrame(data).write.parquet(path)
      file.listFiles().filterNot { file =>
        file.toString.startsWith(".") || file.toString.startsWith("_")
      }.foreach { file =>
        // Remove extensions
        file.renameTo(new File(file.toString.replace(".parquet", "")))
      }

      val source = "parquet"
      assert(DataSourceDetection.detect(sqlContext, path) == source)
    }
  }

  test("detect datasource - parquet (by part file extension)") {
    val data = (1 to 10).map(i => (i, i.toString))
    withTempPath { file =>
      val path = file.getCanonicalPath
      sqlContext.createDataFrame(data).write.parquet(path)

      val source = "parquet"
      assert(DataSourceDetection.detect(sqlContext, path) == source)
    }
  }

  test("detect datasource - json (by directory extension)") {
    val data = (1 to 10).map(i => (i, i.toString))
    withTempPath { file =>
      val path = s"${file.getCanonicalPath}.json"
      sqlContext.createDataFrame(data).write.json(path)

      val source = "json"
      assert(DataSourceDetection.detect(sqlContext, path) == source)
    }
  }

  test("detect datasource - fail to load json without extensions") {
    val data = (1 to 10).map(i => (i, i.toString))
    withTempPath { file =>
      val path = file.getCanonicalPath
      sqlContext.createDataFrame(data).write.json(path)

      val message = intercept[SparkException] {
        DataSourceDetection.detect(sqlContext, path)
      }.getMessage
      assert(message.contains("Detected data source was"))
    }
  }
}

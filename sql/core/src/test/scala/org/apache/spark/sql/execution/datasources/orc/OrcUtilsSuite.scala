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
package org.apache.spark.sql.execution.datasources.orc

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class OrcUtilsSuite extends OrcTest with SharedSQLContext {

  test("read and merge orc schemas in parallel") {
    def testMergeSchemasInParallel(ignoreCorruptFiles: Boolean): Unit = {
      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> ignoreCorruptFiles.toString) {
        withTempDir { dir =>
          val fs = FileSystem.get(spark.sessionState.newHadoopConf())
          val basePath = dir.getCanonicalPath

          val path1 = new Path(basePath, "first")
          val path2 = new Path(basePath, "second")
          val path3 = new Path(basePath, "third")

          spark.range(1).toDF("a").coalesce(1).write.orc(path1.toString)
          spark.range(1, 2).toDF("b").coalesce(1).write.orc(path2.toString)
          spark.range(2, 3).toDF("a").coalesce(1).write.json(path3.toString)

          val fileStatuses =
            Seq(fs.listStatus(path1), fs.listStatus(path2), fs.listStatus(path3)).flatten

          val schema = OrcUtils.mergeSchemasInParallel(
            spark,
            fileStatuses,
            OrcUtils.singleFileSchemaReader)

          assert(schema.isDefined == true)
          assert(schema.get == StructType(Seq(
            StructField("a", LongType, true),
            StructField("b", LongType, true))))
        }
      }
    }

    testMergeSchemasInParallel(true)
    val exception = intercept[SparkException] {
      testMergeSchemasInParallel(false)
    }.getCause
    assert(exception.getCause.getMessage.contains("Could not read footer for file"))
  }
}

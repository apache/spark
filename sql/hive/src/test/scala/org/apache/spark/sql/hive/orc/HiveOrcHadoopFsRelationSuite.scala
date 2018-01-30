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

package org.apache.spark.sql.hive.orc

import java.io.File

import org.apache.spark.sql.execution.datasources.orc.OrcHadoopFsRelationBase
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveOrcHadoopFsRelationSuite extends OrcHadoopFsRelationBase with TestHiveSingleton {
  import testImplicits._

  override val dataSourceName: String =
    classOf[org.apache.spark.sql.hive.orc.OrcFileFormat].getCanonicalName

  test("SPARK-13543: Support for specifying compression codec for ORC via option()") {
    withTempPath { dir =>
      val path = s"${dir.getCanonicalPath}/table1"
      val df = (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b")
      df.write
        .option("compression", "ZlIb")
        .orc(path)

      // Check if this is compressed as ZLIB.
      val maybeOrcFile = new File(path).listFiles().find { f =>
        !f.getName.startsWith("_") && f.getName.endsWith(".zlib.orc")
      }
      assert(maybeOrcFile.isDefined)
      val orcFilePath = maybeOrcFile.get.toPath.toString
      val expectedCompressionKind =
        OrcFileOperator.getFileReader(orcFilePath).get.getCompression
      assert("ZLIB" === expectedCompressionKind.name())

      val copyDf = spark
        .read
        .orc(path)
      checkAnswer(df, copyDf)
    }
  }

  test("Default compression codec is snappy for ORC compression") {
    withTempPath { file =>
      spark.range(0, 10).write
        .orc(file.getCanonicalPath)
      val expectedCompressionKind =
        OrcFileOperator.getFileReader(file.getCanonicalPath).get.getCompression
      assert("SNAPPY" === expectedCompressionKind.name())
    }
  }
}

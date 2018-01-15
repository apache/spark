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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

class FileBasedDataSourceSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  Seq("orc", "parquet", "csv", "json", "text").foreach { format =>
    test(s"Writing empty datasets should not fail - $format") {
      withTempDir { dir =>
        Seq("str").toDS.limit(0).write.format(format).save(dir.getCanonicalPath + "/tmp")
      }
    }
  }

  Seq("orc", "parquet", "csv", "json").foreach { format =>
    test(s"Write and read back unicode schema - $format") {
      withTempPath { path =>
        val dir = path.getCanonicalPath

        // scalastyle:off nonascii
        val df = Seq("a").toDF("한글")
        // scalastyle:on nonascii

        df.write.format(format).option("header", "true").save(dir)
        val answerDf = spark.read.format(format).option("header", "true").load(dir)

        assert(df.schema === answerDf.schema)
        checkAnswer(df, answerDf)
      }
    }
  }

  // Only New OrcFileFormat supports this
  Seq(classOf[org.apache.spark.sql.execution.datasources.orc.OrcFileFormat].getCanonicalName,
      "parquet").foreach { format =>
    test(s"SPARK-15474 Write and read back non-emtpy schema with empty dataframe - $format") {
      withTempPath { file =>
        val path = file.getCanonicalPath
        val emptyDf = Seq((true, 1, "str")).toDF.limit(0)
        emptyDf.write.format(format).save(path)

        val df = spark.read.format(format).load(path)
        assert(df.schema.sameType(emptyDf.schema))
        checkAnswer(df, emptyDf)
      }
    }
  }
}

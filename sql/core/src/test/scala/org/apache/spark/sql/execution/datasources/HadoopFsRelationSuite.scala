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

import java.io.{File, FilenameFilter}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.test.SharedSQLContext

class HadoopFsRelationSuite extends QueryTest with SharedSQLContext {

  test("sizeInBytes should be the total size of all files") {
    withTempDir{ dir =>
      dir.delete()
      spark.range(1000).write.parquet(dir.toString)
      // ignore hidden files
      val allFiles = dir.listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          !name.startsWith(".") && !name.startsWith("_")
        }
      })
      val totalSize = allFiles.map(_.length()).sum
      val df = spark.read.parquet(dir.toString)
      assert(df.queryExecution.logical.stats.sizeInBytes === BigInt(totalSize))
    }
  }

  test("SPARK-22790: spark.sql.sources.compressionFactor takes effect") {
    import testImplicits._
    Seq(1.0, 0.5).foreach { compressionFactor =>
      withSQLConf("spark.sql.sources.fileCompressionFactor" -> compressionFactor.toString,
        "spark.sql.autoBroadcastJoinThreshold" -> "434") {
        withTempPath { workDir =>
          // the file size is 740 bytes
          val workDirPath = workDir.getAbsolutePath
          val data1 = Seq(100, 200, 300, 400).toDF("count")
          data1.write.parquet(workDirPath + "/data1")
          val df1FromFile = spark.read.parquet(workDirPath + "/data1")
          val data2 = Seq(100, 200, 300, 400).toDF("count")
          data2.write.parquet(workDirPath + "/data2")
          val df2FromFile = spark.read.parquet(workDirPath + "/data2")
          val joinedDF = df1FromFile.join(df2FromFile, Seq("count"))
          if (compressionFactor == 0.5) {
            val bJoinExec = joinedDF.queryExecution.executedPlan.collect {
              case bJoin: BroadcastHashJoinExec => bJoin
            }
            assert(bJoinExec.nonEmpty)
            val smJoinExec = joinedDF.queryExecution.executedPlan.collect {
              case smJoin: SortMergeJoinExec => smJoin
            }
            assert(smJoinExec.isEmpty)
          } else {
            // compressionFactor is 1.0
            val bJoinExec = joinedDF.queryExecution.executedPlan.collect {
              case bJoin: BroadcastHashJoinExec => bJoin
            }
            assert(bJoinExec.isEmpty)
            val smJoinExec = joinedDF.queryExecution.executedPlan.collect {
              case smJoin: SortMergeJoinExec => smJoin
            }
            assert(smJoinExec.nonEmpty)
          }
        }
      }
    }
  }
}

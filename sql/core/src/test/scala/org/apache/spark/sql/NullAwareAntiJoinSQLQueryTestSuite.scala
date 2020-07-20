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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

/**
 * End-to-end test cases for subquery SQL queries coverage with
 * NULL_AWARE_ANTI_JOIN_OPTIMIZE_ENABLED = true.
 *
 * Each case is loaded from a file in
 * "spark/sql/core/src/test/resources/sql-tests/inputs/subquery".
 * Each case has a golden result file in
 * "spark/sql/core/src/test/resources/sql-tests/results/subquery".
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt "sql/test-only *NullAwareAntiJoinSQLQueryTestSuite"
 * }}}
 *
 */
class NullAwareAntiJoinSQLQueryTestSuite extends SQLQueryTestSuite {

  protected override def sparkConf: SparkConf = super.sparkConf
    // Fewer shuffle partitions to speed up testing.
    // enable NULL_AWARE_ANTI_JOIN_OPTIMIZE_ENABLED for subquery case coverage.
    .set(SQLConf.SHUFFLE_PARTITIONS, 4)
    .set(SQLConf.NULL_AWARE_ANTI_JOIN_OPTIMIZE_ENABLED, true)

  override lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName =
        s"NAAJ@" +
          s"${absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)}"

      if (file.getAbsolutePath.startsWith(
        s"$inputFilePath${File.separator}subquery")) {
        RegularTestCase(testCaseName, absPath, resultFile) :: Nil
      } else {
        Seq.empty
      }
    }
  }
}

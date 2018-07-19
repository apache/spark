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

package org.apache.spark.sql.execution

import org.apache.spark.sql.test.SharedSQLContext

class FileSourceScanExecSuite extends SharedSQLContext {
  test("FileSourceScanExec should be canonicalizable in executor side") {
    withTempPath { path =>
      spark.range(1).toDF().write.parquet(path.getAbsolutePath)
      val df = spark.read.parquet(path.getAbsolutePath)
      val fileSourceScanExec =
        df.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
      try {
        spark.range(1).foreach(_ => fileSourceScanExec.canonicalized)
      } catch {
        case e: Throwable => fail("FileSourceScanExec was not canonicalizable", e)
      }
    }
  }
}

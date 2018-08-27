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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.test.SharedSQLContext


class FileSourceSuite extends QueryTest with SharedSQLContext with PredicateHelper {

  test("[SPARK-25237] remove updateBytesReadWithFileSize in FileScanRdd") {
    withTempPath { p =>
      val path = p.getAbsolutePath
      spark.range(1000).selectExpr("id AS c0", "rand() AS c1").repartition(10).write.csv(path)
      val df = spark.read.csv(path).limit(1)

      val bytesReads = new ArrayBuffer[Long]()
      val bytesReadListener = new SparkListener() {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
          bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
        }
      }
      // Avoid receiving earlier taskEnd events
      spark.sparkContext.listenerBus.waitUntilEmpty(500)

      spark.sparkContext.addSparkListener(bytesReadListener)

      df.collect()

      spark.sparkContext.listenerBus.waitUntilEmpty(500)
      spark.sparkContext.removeSparkListener(bytesReadListener)

      assert(bytesReads.sum < 3000)
    }
  }
}

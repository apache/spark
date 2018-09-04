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
import org.apache.spark.sql.test.SharedSQLContext


class FileSourceSuite extends SharedSQLContext {

  test("SPARK-25237 compute correct input metrics in FileScanRDD") {
    withTempPath { p =>
      val path = p.getAbsolutePath
      spark.range(1000).repartition(1).write.csv(path)
      val bytesReads = new ArrayBuffer[Long]()
      val bytesReadListener = new SparkListener() {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
          bytesReads += taskEnd.taskMetrics.inputMetrics.bytesRead
        }
      }
      sparkContext.addSparkListener(bytesReadListener)
      try {
        spark.read.csv(path).limit(1).collect()
        sparkContext.listenerBus.waitUntilEmpty(1000L)
        assert(bytesReads.sum === 7860)
      } finally {
        sparkContext.removeSparkListener(bytesReadListener)
      }
    }
  }
}

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

package org.apache.spark.sql.sources

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.TaskContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

class CommitFailureTestSource extends SimpleTextSource {
  /**
   * Prepares a write job and returns an
   * [[org.apache.spark.sql.execution.datasources.OutputWriterFactory]].
   * Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
   */
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory =
    new OutputWriterFactory {
      override def newInstance(
          path: String,
          bucketId: Option[Int],
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new SimpleTextOutputWriter(path, context) {
          var failed = false
          TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
            failed = true
            SimpleTextRelation.callbackCalled = true
          }

          override def write(row: Row): Unit = {
            if (SimpleTextRelation.failWriter) {
              sys.error("Intentional task writer failure for testing purpose.")

            }
            super.write(row)
          }

          override def close(): Unit = {
            super.close()
            sys.error("Intentional task commitment failure for testing purpose.")
          }
        }
      }
    }

  override def shortName(): String = "commit-failure-test"
}

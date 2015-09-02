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

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.test.SQLTestUtils


class CommitFailureTestRelationSuite extends SparkFunSuite with SQLTestUtils {
  override def _sqlContext: SQLContext = TestHive
  private val sqlContext = _sqlContext

  // When committing a task, `CommitFailureTestSource` throws an exception for testing purpose.
  val dataSourceName: String = classOf[CommitFailureTestSource].getCanonicalName

  test("SPARK-7684: commitTask() failure should fallback to abortTask()") {
    withTempPath { file =>
      // Here we coalesce partition number to 1 to ensure that only a single task is issued.  This
      // prevents race condition happened when FileOutputCommitter tries to remove the `_temporary`
      // directory while committing/aborting the job.  See SPARK-8513 for more details.
      val df = sqlContext.range(0, 10).coalesce(1)
      intercept[SparkException] {
        df.write.format(dataSourceName).save(file.getCanonicalPath)
      }

      val fs = new Path(file.getCanonicalPath).getFileSystem(SparkHadoopUtil.get.conf)
      assert(!fs.exists(new Path(file.getCanonicalPath, "_temporary")))
    }
  }
}

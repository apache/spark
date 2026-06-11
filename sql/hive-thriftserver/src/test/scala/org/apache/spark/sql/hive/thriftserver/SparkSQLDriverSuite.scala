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

package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils.REDACTION_REPLACEMENT_TEXT

class SparkSQLDriverSuite extends SharedSparkSession {

  test("job description should be redacted by spark.sql.redaction.string.regex") {
    withSQLConf(SQLConf.SQL_STRING_REDACTION_PATTERN.key -> "password=([^\\s]+)") {
      var jobDescription: String = null
      spark.sparkContext.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          jobDescription =
            jobStart.properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION)
        }
      })

      val driver = new SparkSQLDriver(spark)
      try {
        driver.run("SELECT 'password=secret123'")
      } finally {
        driver.close()
      }

      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(jobDescription != null)
      assert(!jobDescription.contains("secret123"))
      assert(jobDescription.contains(REDACTION_REPLACEMENT_TEXT))
    }
  }
}

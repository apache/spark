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

import org.apache.spark.SparkFunSuite

class DataSourceManagerSuite extends SparkFunSuite {
  test("SPARK-46670: DataSourceManager should be self clone-able without lookup") {
    val testAppender = new LogAppender("Cloneable DataSourceManager without lookup")
    withLogAppender(testAppender) {
      new DataSourceManager().clone()
    }
    assert(!testAppender.loggingEvents
      .exists(msg =>
        msg.getMessage.getFormattedMessage.contains("Skipping the lookup of Python Data Sources")))
  }
}

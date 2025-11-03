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

package org.apache.spark.sql.hive.execution

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.hive.test.TestHiveContext

class ConcurrentHiveSuite extends SparkFunSuite {
  ignore("multiple instances not supported") {
    test("Multiple Hive Instances") {
      (1 to 10).map { i =>
        val conf = new SparkConf()
        conf.set(UI_ENABLED, false)
        val ts =
          new TestHiveContext(new SparkContext("local", s"TestSQLContext$i", conf))
        ts.sparkSession.sql("SHOW TABLES").collect()
        ts.sparkSession.sql("SELECT * FROM src").collect()
        ts.sparkSession.sql("SHOW TABLES").collect()
      }
    }
  }
}

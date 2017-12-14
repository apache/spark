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

package org.apache.spark.sql.sources.v2

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class DataSourceV2UtilsSuite extends QueryTest with SharedSQLContext {

  private val keyPrefix = "userDefinedDataSource"

  test("method withSessionConfig() should propagate session configs correctly") {
    // Only match configs with keys start with "spark.datasource.${keyPrefix}".
    withSQLConf(s"spark.datasource.$keyPrefix.foo.bar" -> "false",
      s"spark.datasource.$keyPrefix.whateverConfigName" -> "123",
      s"spark.sql.$keyPrefix.config.name" -> "false",
      s"spark.datasource.another.config.name" -> "123") {
      val confs = DataSourceV2Utils.withSessionConfig(keyPrefix, SQLConf.get)
      assert(confs.size == 2)
      assert(confs.keySet.filter(_.startsWith("spark.datasource")).size == 0)
      assert(confs.keySet.filter(_.startsWith("not.exist.prefix")).size == 0)
      assert(confs.keySet.contains("foo.bar"))
      assert(confs.keySet.contains("whateverConfigName"))
    }
  }
}

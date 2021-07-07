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

package org.apache.spark.sql.connector

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.catalog.SessionConfigSupport
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils
import org.apache.spark.sql.internal.SQLConf

class DataSourceV2UtilsSuite extends SparkFunSuite {

  private val keyPrefix = new DataSourceV2WithSessionConfig().keyPrefix

  test("method withSessionConfig() should propagate session configs correctly") {
    // Only match configs with keys start with "spark.datasource.${keyPrefix}".
    val conf = new SQLConf
    conf.setConfString(s"spark.datasource.$keyPrefix.foo.bar", "false")
    conf.setConfString(s"spark.datasource.$keyPrefix.whateverConfigName", "123")
    conf.setConfString(s"spark.sql.$keyPrefix.config.name", "false")
    conf.setConfString("spark.datasource.another.config.name", "123")
    conf.setConfString(s"spark.datasource.$keyPrefix.", "123")
    val source = new DataSourceV2WithSessionConfig
    val confs = DataSourceV2Utils.extractSessionConfigs(source, conf)
    assert(confs.size == 2)
    assert(!confs.keySet.exists(_.startsWith("spark.datasource")))
    assert(!confs.keySet.exists(_.startsWith("not.exist.prefix")))
    assert(confs.keySet.contains("foo.bar"))
    assert(confs.keySet.contains("whateverConfigName"))
  }
}

class DataSourceV2WithSessionConfig extends SimpleDataSourceV2 with SessionConfigSupport {

  override def keyPrefix: String = "userDefinedDataSource"
}

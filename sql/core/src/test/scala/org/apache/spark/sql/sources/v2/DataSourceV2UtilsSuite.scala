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

import java.net.URI

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class DataSourceV2UtilsSuite extends SparkFunSuite with SharedSQLContext {

  private val keyPrefix = new DataSourceV2WithSessionConfig().keyPrefix

  test("method withSessionConfig() should propagate session configs correctly") {
    // Only match configs with keys start with "spark.datasource.${keyPrefix}".
    val conf = new SQLConf
    conf.setConfString(s"spark.datasource.$keyPrefix.foo.bar", "false")
    conf.setConfString(s"spark.datasource.$keyPrefix.whateverConfigName", "123")
    conf.setConfString(s"spark.sql.$keyPrefix.config.name", "false")
    conf.setConfString("spark.datasource.another.config.name", "123")
    conf.setConfString(s"spark.datasource.$keyPrefix.", "123")
    val cs = classOf[DataSourceV2WithSessionConfig].newInstance()
    val confs = DataSourceV2Utils.extractSessionConfigs(cs.asInstanceOf[DataSourceV2], conf)
    assert(confs.size == 2)
    assert(confs.keySet.filter(_.startsWith("spark.datasource")).size == 0)
    assert(confs.keySet.filter(_.startsWith("not.exist.prefix")).size == 0)
    assert(confs.keySet.contains("foo.bar"))
    assert(confs.keySet.contains("whateverConfigName"))
  }

  test("parseTableLocation") {
    import DataSourceV2Utils.parseTableLocation
    // no location
    assert((None, None) === parseTableLocation(spark, None))

    // file paths
    val s3Path = "s3://bucket/path/file.ext"
    assert((Some(s3Path), None) === parseTableLocation(spark, Some(s3Path)))
    val hdfsPath = "hdfs://nn:8020/path/file.ext"
    assert((Some(hdfsPath), None) === parseTableLocation(spark, Some(hdfsPath)))
    val localPath = "/path/file.ext"
    assert((Some(localPath), None) === parseTableLocation(spark, Some(localPath)))

    // table names
    assert(
      (None, Some(TableIdentifier("t", Some("default")))) === parseTableLocation(spark, Some("t")))
    assert(
      (None, Some(TableIdentifier("t", Some("db")))) === parseTableLocation(spark, Some("db.t")))

    spark.sessionState.catalog.createDatabase(
      CatalogDatabase("test", "test", URI.create("file:/tmp"), Map.empty), ignoreIfExists = true)
    spark.sessionState.catalog.setCurrentDatabase("test")
    assert(
      (None, Some(TableIdentifier("t", Some("test")))) === parseTableLocation(spark, Some("t")))
  }
}

class DataSourceV2WithSessionConfig extends SimpleDataSourceV2 with SessionConfigSupport {

  override def keyPrefix: String = "userDefinedDataSource"
}

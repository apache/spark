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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.SharedSparkSession

trait DatasourceV2SQLBase
  extends QueryTest with SharedSparkSession with BeforeAndAfter {

  protected def catalog(name: String): CatalogPlugin = {
    spark.sessionState.catalogManager.catalog(name)
  }

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testpart", classOf[InMemoryPartitionTableCatalog].getName)
    spark.conf.set(
      "spark.sql.catalog.testcat_atomic", classOf[StagingInMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    spark.conf.set(
      V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[InMemoryTableSessionCatalog].getName)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.sessionState.catalog.reset()
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
  }
}

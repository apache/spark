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

import java.util

import org.apache.spark.sql.{DataFrameWriter, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.{RequiresTableWriteDistribution, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.read.partitioning.{Distribution, SparkHashClusteredDistribution}
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DatasourceV2RequireDistributionSuite extends QueryTest with SharedSparkSession {

  test("append") {
    val data = spark.range(10000L).toDF("id")

    val writer: DataFrameWriter[Row] = data.write.mode("append").format("test-table-w-dist")

    writer.save()
  }

  test("overwrite") {
    val data = spark.range(10000L).toDF("id")

    val writer: DataFrameWriter[Row] = data.write.mode("overwrite").format("test-table-w-dist")

    writer.save()
  }
}





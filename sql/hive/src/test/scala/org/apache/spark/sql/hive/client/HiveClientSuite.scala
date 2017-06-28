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

package org.apache.spark.sql.hive.client

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.types.IntegerType

class HiveClientSuite extends SparkFunSuite {
  private val clientBuilder = new HiveClientBuilder

  private val tryDirectSqlKey = HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname

  test(s"getPartitionsByFilter returns all partitions when $tryDirectSqlKey=false") {
    val testPartitionCount = 5

    val storageFormat = CatalogStorageFormat(
      locationUri = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = Map.empty)

    val hadoopConf = new Configuration()
    hadoopConf.setBoolean(tryDirectSqlKey, false)
    val client = clientBuilder.buildClient(HiveUtils.hiveExecutionVersion, hadoopConf)
    client.runSqlHive("CREATE TABLE test (value INT) PARTITIONED BY (part INT)")

    val partitions = (1 to testPartitionCount).map { part =>
      CatalogTablePartition(Map("part" -> part.toString), storageFormat)
    }
    client.createPartitions(
      "default", "test", partitions, ignoreIfExists = false)

    val filteredPartitions = client.getPartitionsByFilter(client.getTable("default", "test"),
      Seq(EqualTo(AttributeReference("part", IntegerType)(), Literal(3))))

    assert(filteredPartitions.size == testPartitionCount)
  }
}

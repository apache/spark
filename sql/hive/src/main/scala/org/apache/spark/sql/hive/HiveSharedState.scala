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

package org.apache.spark.sql.hive

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SharedState, SQLConf}


/**
 * A class that holds all state shared across sessions in a given
 * [[org.apache.spark.sql.SparkSession]] backed by Hive.
 */
private[hive] class HiveSharedState(override val sparkContext: SparkContext)
  extends SharedState(sparkContext) with Logging {

  // TODO: just share the IsolatedClientLoader instead of the client instance itself

  {
    // Set the Hive metastore warehouse path to the one we use
    val tempConf = new SQLConf
    sparkContext.conf.getAll.foreach { case (k, v) => tempConf.setConfString(k, v) }
    val hiveWarehouseDir = hadoopConf.get("hive.metastore.warehouse.dir")
    if (hiveWarehouseDir != null && !tempConf.contains(SQLConf.WAREHOUSE_PATH.key)) {
      // If hive.metastore.warehouse.dir is set and spark.sql.warehouse.dir is not set,
      // we will respect the value of hive.metastore.warehouse.dir.
      tempConf.setConfString(SQLConf.WAREHOUSE_PATH.key, hiveWarehouseDir)
      sparkContext.conf.set(SQLConf.WAREHOUSE_PATH.key, hiveWarehouseDir)
      logInfo(s"${SQLConf.WAREHOUSE_PATH.key} is not set, but hive.metastore.warehouse.dir " +
        s"is set. Setting ${SQLConf.WAREHOUSE_PATH.key} to the value of " +
        s"hive.metastore.warehouse.dir ($hiveWarehouseDir).")
    } else {
      // If spark.sql.warehouse.dir is set, we will override hive.metastore.warehouse.dir using
      // the value of spark.sql.warehouse.dir.
      // When neither spark.sql.warehouse.dir nor hive.metastore.warehouse.dir is set,
      // we will set hive.metastore.warehouse.dir to the default value of spark.sql.warehouse.dir.
      sparkContext.conf.set("hive.metastore.warehouse.dir", tempConf.warehousePath)
    }

    logInfo(s"Setting Hive metastore warehouse path to '${tempConf.warehousePath}'")
  }

  /**
   * A Hive client used to interact with the metastore.
   */
  // This needs to be a lazy val at here because TestHiveSharedState is overriding it.
  lazy val metadataHive: HiveClient = {
    HiveUtils.newClientForMetadata(sparkContext.conf, hadoopConf)
  }

  /**
   * A catalog that interacts with the Hive metastore.
   */
  override lazy val externalCatalog = new HiveExternalCatalog(metadataHive, hadoopConf)
}

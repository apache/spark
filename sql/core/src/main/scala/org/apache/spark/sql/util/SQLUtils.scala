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

package org.apache.spark.sql.util

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.util.{Utils => CoreUtils}
/**
 * Various utility methods used by Spark SQL.
 */
private[spark] object SQLUtils extends Logging {

  def warehousePath(sparkConf: SparkConf = new SparkConf(loadDefaults = true)): String = {
    val configFile = CoreUtils.getContextOrSparkClassLoader.getResource("hive-site.xml")
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    if (configFile != null) {
      logInfo(s"loading hive config file: $configFile")
      hadoopConf.addResource(configFile)
    }

    // Set the Hive metastore warehouse path to the one we use
    val hiveWarehouseDir = hadoopConf.get("hive.metastore.warehouse.dir")
    if (hiveWarehouseDir != null && !sparkConf.contains(WAREHOUSE_PATH.key)) {
      // If hive.metastore.warehouse.dir is set and spark.sql.warehouse.dir is not set,
      // we will respect the value of hive.metastore.warehouse.dir.
      logInfo(s"${WAREHOUSE_PATH.key} is not set, but hive.metastore.warehouse.dir " +
        s"is set. Setting ${WAREHOUSE_PATH.key} to the value of " +
        s"hive.metastore.warehouse.dir ('$hiveWarehouseDir').")
      hiveWarehouseDir
    } else {
      // If spark.sql.warehouse.dir is set, we will override hive.metastore.warehouse.dir using
      // the value of spark.sql.warehouse.dir.
      // When neither spark.sql.warehouse.dir nor hive.metastore.warehouse.dir is set,
      // we will set hive.metastore.warehouse.dir to the default value of spark.sql.warehouse.dir.
      val sparkWarehouseDir = sparkConf.get(WAREHOUSE_PATH)
      logInfo(s"Setting hive.metastore.warehouse.dir ('$hiveWarehouseDir') to the value of " +
        s"${WAREHOUSE_PATH.key} ('$sparkWarehouseDir').")
      sparkWarehouseDir
    }
  }
}

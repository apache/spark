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
    sparkContext.conf.set("hive.metastore.warehouse.dir", tempConf.warehousePath)
    logInfo(s"Setting Hive metastore warehouse path to '${tempConf.warehousePath}'")
  }

  /**
   * A Hive client used to interact with the metastore.
   */
  // This needs to be a lazy val at here because TestHiveSharedState is overriding it.
  lazy val metadataHive: HiveClient = {
    HiveUtils.newClientForMetadata(sparkContext.conf, sparkContext.hadoopConfiguration)
  }

  /**
   * A catalog that interacts with the Hive metastore.
   */
  override lazy val externalCatalog = new HiveExternalCatalog(metadataHive)

}

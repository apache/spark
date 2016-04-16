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
import org.apache.spark.sql.hive.client.{HiveClient, HiveClientImpl}
import org.apache.spark.sql.internal.SharedState


/**
 * A class that holds all state shared across sessions in a given [[HiveContext]].
 */
private[hive] class HiveSharedState(override val sparkContext: SparkContext)
  extends SharedState(sparkContext) {

  // TODO: just share the IsolatedClientLoader instead of the client instances themselves

  /**
   * A Hive client used for execution.
   */
  val executionHive: HiveClientImpl = {
    HiveContext.newClientForExecution(sparkContext.conf, sparkContext.hadoopConfiguration)
  }

  /**
   * A Hive client used to interact with the metastore.
   */
  // This needs to be a lazy val at here because TestHiveSharedState is overriding it.
  lazy val metadataHive: HiveClient = {
    HiveContext.newClientForMetadata(sparkContext.conf, sparkContext.hadoopConfiguration)
  }

  /**
   * A catalog that interacts with the Hive metastore.
   */
  override lazy val externalCatalog = new HiveExternalCatalog(metadataHive)

}

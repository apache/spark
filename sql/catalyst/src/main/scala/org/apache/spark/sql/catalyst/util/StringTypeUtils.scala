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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SqlApiConf

object StringTypeUtils extends Logging {
  /**
   * Check if default collation is up-to-date with a config.
   */
  private def refreshDefaultCollation(): Unit = {
    if (SparkEnv.get != null && SparkEnv.get.executorId == SparkContext.DRIVER_IDENTIFIER
      && SqlApiConf.get.defaultCollation != DEFAULT_COLLATION) {
      DEFAULT_COLLATION = SqlApiConf.get.defaultCollation
      DEFAULT_COLLATION_ID = CollationFactory.collationNameToId(DEFAULT_COLLATION)
    }
  }

  /**
   * Returns if collation is default collation.
   */
  def isDefaultCollation(collationId: Int): Boolean = {
    refreshDefaultCollation()
    collationId == DEFAULT_COLLATION_ID
  }

  /**
   * Returns default collationId.
   */
  def getDefaultCollationId: Int = {
    refreshDefaultCollation()
    DEFAULT_COLLATION_ID
  }

  private var DEFAULT_COLLATION: String = SqlApiConf.get.defaultCollation
  private var DEFAULT_COLLATION_ID: Int = CollationFactory.collationNameToId(DEFAULT_COLLATION)
}

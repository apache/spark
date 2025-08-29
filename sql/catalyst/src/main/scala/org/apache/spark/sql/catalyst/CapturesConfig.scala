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

package org.apache.spark.sql.catalyst

import scala.collection.mutable

import org.apache.spark.sql.internal.SQLConf

/**
 * Trait used for persisting the conf values in views/UDFs.
 */
trait CapturesConfig {
  private val configPrefixDenyList = Seq(
    SQLConf.MAX_NESTED_VIEW_DEPTH.key,
    "spark.sql.optimizer.",
    "spark.sql.codegen.",
    "spark.sql.execution.",
    "spark.sql.shuffle.",
    "spark.sql.adaptive.",
    // ignore optimization configs used in `RelationConversions`
    "spark.sql.hive.convertMetastoreParquet",
    "spark.sql.hive.convertMetastoreOrc",
    "spark.sql.hive.convertInsertingPartitionedTable",
    "spark.sql.hive.convertInsertingUnpartitionedTable",
    "spark.sql.hive.convertMetastoreCtas",
    SQLConf.ADDITIONAL_REMOTE_REPOSITORIES.key)

  private val configAllowList = Set(
    SQLConf.DISABLE_HINTS.key
  )

  /**
   * Set of single-pass resolver confs that shouldn't be stored during view/UDF/proc creation.
   * This is needed to avoid accidental failures in tentative and dual-run modes when querying the
   * view.
   */
  private val singlePassResolverDenyList = Set(
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key,
    SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key
  )

  /**
   * Convert the provided SQL configs to `properties`. Here we only capture the SQL configs that are
   * modifiable and should be captured, i.e. not in the denyList and in the allowList. We also
   * capture `SESSION_LOCAL_TIMEZONE` whose default value relies on the JVM system timezone and
   * the `ANSI_ENABLED` value.
   *
   * We need to always capture them to make sure we apply the same configs when querying the
   * view/UDF.
   */
  def sqlConfigsToProps(conf: SQLConf, prefix: String): Map[String, String] = {
    val modifiedConfs = getModifiedConf(conf)

    val alwaysCaptured = Seq(SQLConf.SESSION_LOCAL_TIMEZONE, SQLConf.ANSI_ENABLED)
      .filter(c => !modifiedConfs.contains(c.key))
      .map(c => (c.key, conf.getConf(c).toString))

    val props = new mutable.HashMap[String, String]
    for ((key, value) <- modifiedConfs ++ alwaysCaptured) {
      props.put(s"$prefix$key", value)
    }
    props.toMap
  }

  /**
   * Get all configurations that are modifiable and should be captured.
   */
  private def getModifiedConf(conf: SQLConf): Map[String, String] = {
    conf.getAllConfs.filter { case (k, _) =>
      conf.isModifiable(k) && shouldCaptureConfig(k)
    }
  }

  /**
   * Capture view config either of:
   * 1. exists in allowList
   * 2. do not exists in denyList
   */
  private def shouldCaptureConfig(key: String): Boolean = {
    configAllowList.contains(key) || (
      !configPrefixDenyList.exists(prefix => key.startsWith(prefix)) &&
        !singlePassResolverDenyList.contains(key)
      )
  }
}

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

package org.apache.spark.ml.util

import org.apache.spark.SparkEnv
import org.apache.spark.sql.internal.StaticSQLConf

/**
 * Reads `spark.sql.ml.maxNumFeatures` once per [[SparkEnv]] and caches it, so hot paths - notably
 * the per-row `VectorUDT.deserialize` - can enforce the limit without calling `SQLConf.get` on
 * every row. `SQLConf.get` allocates a `ReadOnlySQLConf` on RDD-based executor paths, so reading it
 * per row would add an allocation to a hot path for a limit that is disabled by default.
 *
 * The config is a static conf, so its value is immutable for the lifetime of a `SparkContext` and
 * is therefore safe to cache. The cache is keyed on the active `SparkEnv` so it refreshes if the
 * context is stopped and a new one is created (for example across tests).
 */
private[spark] object MLMaxNumFeatures {

  @volatile private var cachedEnv: SparkEnv = _
  @volatile private var cachedValue: Int = -1

  /** The configured maximum, or -1 (no limit) when unset or when no `SparkEnv` is active. */
  def get: Int = {
    val env = SparkEnv.get
    if (env == null) {
      -1
    } else {
      // Reference-compare the active env against the cached one: on an executor there is a single
      // env for the application's lifetime, so this reads the config exactly once and then only
      // does a volatile read plus a reference comparison per call.
      if (!env.eq(cachedEnv)) {
        cachedValue = env.conf.get(StaticSQLConf.ML_MAX_NUM_FEATURES)
        cachedEnv = env
      }
      cachedValue
    }
  }

  /**
   * Throws an [[IllegalArgumentException]] if `count` exceeds the configured maximum. When the
   * limit is disabled (`-1`, the default) the check is skipped entirely, so there is no behavior
   * change and no exception message is constructed unless the limit is both set and exceeded.
   */
  def check(count: Long, what: String): Unit = {
    val max = get
    if (max > 0 && count > max) {
      throw new IllegalArgumentException(
        s"$what ($count) exceeds the configured maximum ${StaticSQLConf.ML_MAX_NUM_FEATURES.key} " +
          s"($max).")
    }
  }
}

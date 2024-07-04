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

package org.apache.spark

import java.util.regex.Pattern

import org.apache.spark.annotation.{DeveloperApi, Unstable}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, PySparkCurrentOrigin}
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.internal.SQLConf

/**
 * Allows the execution of relational queries, including those expressed in SQL using Spark.
 *
 *  @groupname dataType Data types
 *  @groupdesc Spark SQL data types.
 *  @groupprio dataType -3
 *  @groupname field Field
 *  @groupprio field -2
 *  @groupname row Row
 *  @groupprio row -1
 */
package object sql {

  /**
   * Converts a logical plan into zero or more SparkPlans.  This API is exposed for experimenting
   * with the query planner and is not designed to be stable across spark releases.  Developers
   * writing libraries should instead consider using the stable APIs provided in
   * [[org.apache.spark.sql.sources]]
   */
  @DeveloperApi
  @Unstable
  type Strategy = SparkStrategy

  type DataFrame = Dataset[Row]

  /**
   * Metadata key which is used to write Spark version in the followings:
   * - Parquet file metadata
   * - ORC file metadata
   * - Avro file metadata
   *
   * Note that Hive table property `spark.sql.create.version` also has Spark version.
   */
  private[sql] val SPARK_VERSION_METADATA_KEY = "org.apache.spark.version"

  /**
   * The metadata key which is used to write the current session time zone into:
   * - Parquet file metadata
   * - Avro file metadata
   */
  private[sql] val SPARK_TIMEZONE_METADATA_KEY = "org.apache.spark.timeZone"

  /**
   * Parquet/Avro file metadata key to indicate that the file was written with legacy datetime
   * values.
   */
  private[sql] val SPARK_LEGACY_DATETIME_METADATA_KEY = "org.apache.spark.legacyDateTime"

  /**
   * Parquet file metadata key to indicate that the file with INT96 column type was written
   * with rebasing.
   */
  private[sql] val SPARK_LEGACY_INT96_METADATA_KEY = "org.apache.spark.legacyINT96"

  /**
   * This helper function captures the Spark API and its call site in the user code from the current
   * stacktrace.
   *
   * As adding `withOrigin` explicitly to all Spark API definition would be a huge change,
   * `withOrigin` is used only at certain places where all API implementation surely pass through
   * and the current stacktrace is filtered to the point where first Spark API code is invoked from
   * the user code.
   *
   * As there might be multiple nested `withOrigin` calls (e.g. any Spark API implementations can
   * invoke other APIs) only the first `withOrigin` is captured because that is closer to the user
   * code.
   *
   * @param f The function that can use the origin.
   * @return The result of `f`.
   */
  private[sql] def withOrigin[T](f: => T): T = {
    if (CurrentOrigin.get.stackTrace.isDefined) {
      f
    } else {
      val st = Thread.currentThread().getStackTrace
      var i = 0
      // Find the beginning of Spark code traces
      while (i < st.length && !sparkCode(st(i))) i += 1
      // Stop at the end of the first Spark code traces
      while (i < st.length && sparkCode(st(i))) i += 1
      val origin = Origin(stackTrace = Some(st.slice(
        from = i - 1,
        until = i + SQLConf.get.stackTracesInDataFrameContext)),
        pysparkErrorContext = PySparkCurrentOrigin.get())
      CurrentOrigin.withOrigin(origin)(f)
    }
  }

  private val sparkCodePattern = Pattern.compile("(org\\.apache\\.spark\\.sql\\." +
      "(?:functions|Column|ColumnName|SQLImplicits|Dataset|DataFrameStatFunctions|DatasetHolder)" +
      "(?:|\\..*|\\$.*))" +
      "|(scala\\.collection\\..*)")

  private def sparkCode(ste: StackTraceElement): Boolean = {
    sparkCodePattern.matcher(ste.getClassName).matches()
  }
}

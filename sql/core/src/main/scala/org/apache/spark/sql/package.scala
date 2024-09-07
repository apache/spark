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

import org.apache.spark.annotation.{DeveloperApi, Unstable}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.internal.ExpressionUtils

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
   * Implicit helper that makes it easy to construct a Column from an Expression. This allows
   * developers to create a Column in the same way as in earlier versions of Spark (before 4.0).
   */
  @DeveloperApi
  implicit class ColumnConstructorExt(val c: Column.type) extends AnyVal {
    def apply(e: Expression): Column = ExpressionUtils.column(e)
  }
}

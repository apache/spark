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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec

object PartitioningUtils {
  /**
   * Normalize the column names in partition specification, w.r.t. the real partition column names
   * and case sensitivity. e.g., if the partition spec has a column named `monTh`, and there is a
   * partition column named `month`, and it's case insensitive, we will normalize `monTh` to
   * `month`.
   */
  def normalizePartitionSpec[T](
      partitionSpec: Map[String, T],
      partColNames: Seq[String],
      tblName: String,
      resolver: Resolver): Map[String, T] = {
    val normalizedPartSpec = partitionSpec.toSeq.map { case (key, value) =>
      val normalizedKey = partColNames.find(resolver(_, key)).getOrElse {
        throw new AnalysisException(s"$key is not a valid partition column in table $tblName.")
      }
      normalizedKey -> value
    }

    SchemaUtils.checkColumnNameDuplication(
      normalizedPartSpec.map(_._1), "in the partition schema", resolver)

    normalizedPartSpec.toMap
  }

  /**
   * Verify if the input partition spec exactly matches the existing defined partition spec
   * The columns must be the same but the orders could be different.
   */
  def requireExactMatchedPartitionSpec(
      tableName: String,
      spec: TablePartitionSpec,
      partitionColumnNames: Seq[String]): Unit = {
    val defined = partitionColumnNames.sorted
    if (spec.keys.toSeq.sorted != defined) {
      throw new AnalysisException(
        s"Partition spec is invalid. The spec (${spec.keys.mkString(", ")}) must match " +
        s"the partition spec (${partitionColumnNames.mkString(", ")}) defined in " +
        s"table '$tableName'")
    }
  }
}

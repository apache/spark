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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.analysis.UnresolvedLeafNode
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType

/** Created in the DataFrameReader and DataStreamReader APIs when loading a Spark DataSource. */
case class UnresolvedDataSource(
    format: String,
    userSpecifiedSchema: Option[StructType],
    options: CaseInsensitiveMap[String],
    override val isStreaming: Boolean,
    paths: Seq[String])
  extends UnresolvedLeafNode {

  override def simpleString(maxFields: Int): String = toString

  override def toString: String = {
    s"UnresolvedDataSource format: $format, isStreaming: $isStreaming, " +
      s"paths: ${paths.length} provided"
  }
}

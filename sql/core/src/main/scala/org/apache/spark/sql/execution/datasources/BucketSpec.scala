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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions.Attribute

private[sql] case class BucketSpec(
    numBuckets: Int,
    bucketingColumns: Seq[String],
    sortingColumns: Option[Seq[String]]) {

  def resolvedBucketingColumns(inputSchema: Seq[Attribute]): Seq[Attribute] = {
    bucketingColumns.map { col =>
      inputSchema.find(_.name == col).get
    }
  }

  def resolvedSortingColumns(inputSchema: Seq[Attribute]): Seq[Attribute] = {
    if (sortingColumns.isDefined) {
      sortingColumns.get.map { col =>
        inputSchema.find(_.name == col).get
      }
    } else {
      Nil
    }
  }
}

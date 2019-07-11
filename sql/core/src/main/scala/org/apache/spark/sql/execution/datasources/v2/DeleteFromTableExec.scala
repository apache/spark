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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.SupportsMaintenance
import org.apache.spark.sql.sources.v2.maintain.SupportsDelete
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class DeleteFromTableExec(
    table: SupportsMaintenance,
    options: CaseInsensitiveStringMap,
    deleteWhere: Array[Filter],
    query: SparkPlan) extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    table.newMaintainerBuilder(options).build() match {
      case maintainer: SupportsDelete =>
        maintainer.delete(deleteWhere)
      case _ =>
        throw new SparkException(s"Table does not support delete: $table")
    }

    sparkContext.emptyRDD
  }

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Nil
}

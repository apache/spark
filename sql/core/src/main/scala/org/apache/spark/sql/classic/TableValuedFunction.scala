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
package org.apache.spark.sql.classic

import org.apache.spark.sql
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableValuedFunction

class TableValuedFunction(sparkSession: SparkSession)
  extends sql.TableValuedFunction {

  /** @inheritdoc */
  override def range(end: Long): Dataset[java.lang.Long] = {
    sparkSession.range(end)
  }

  /** @inheritdoc */
  override def range(start: Long, end: Long): Dataset[java.lang.Long] = {
    sparkSession.range(start, end)
  }

  /** @inheritdoc */
  override def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] = {
    sparkSession.range(start, end, step)
  }

  /** @inheritdoc */
  override def range(
      start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] = {
    sparkSession.range(start, end, step, numPartitions)
  }

  private def fn(name: String, args: Seq[Column]): Dataset[Row] = {
    Dataset.ofRows(
      sparkSession,
      UnresolvedTableValuedFunction(name, args.map(sparkSession.expression)))
  }

  /** @inheritdoc */
  override def explode(collection: Column): Dataset[Row] =
    fn("explode", Seq(collection))

  /** @inheritdoc */
  override def explode_outer(collection: Column): Dataset[Row] =
    fn("explode_outer", Seq(collection))

  /** @inheritdoc */
  override def inline(input: Column): Dataset[Row] =
    fn("inline", Seq(input))

  /** @inheritdoc */
  override def inline_outer(input: Column): Dataset[Row] =
    fn("inline_outer", Seq(input))

  /** @inheritdoc */
  override def json_tuple(input: Column, fields: Column*): Dataset[Row] =
    fn("json_tuple", input +: fields)

  /** @inheritdoc */
  override def posexplode(collection: Column): Dataset[Row] =
    fn("posexplode", Seq(collection))

  /** @inheritdoc */
  override def posexplode_outer(collection: Column): Dataset[Row] =
    fn("posexplode_outer", Seq(collection))

  /** @inheritdoc */
  override def stack(n: Column, fields: Column*): Dataset[Row] =
    fn("stack", n +: fields)

  /** @inheritdoc */
  override def collations(): Dataset[Row] =
    fn("collations", Seq.empty)

  /** @inheritdoc */
  override def sql_keywords(): Dataset[Row] =
    fn("sql_keywords", Seq.empty)

  /** @inheritdoc */
  override def variant_explode(input: Column): Dataset[Row] =
    fn("variant_explode", Seq(input))

  /** @inheritdoc */
  override def variant_explode_outer(input: Column): Dataset[Row] =
    fn("variant_explode_outer", Seq(input))
}

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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.DefinedByConstructorParams
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils

/**
 * Classic implementation of the user-facing Catalog. Delegates to the shared
 * [[org.apache.spark.sql.internal.CatalogImpl]] (SQL-only, same as Connect).
 * Adds Classic-specific methods used by tests (makeDataset).
 */
class Catalog(sparkSession: SparkSession) extends org.apache.spark.sql.internal.CatalogImpl(
  sparkSession,
  sparkSession)

private[sql] object Catalog {

  def makeDataset[T <: DefinedByConstructorParams: TypeTag](
      data: Seq[T],
      sparkSession: SparkSession): Dataset[T] = {
    val enc = ExpressionEncoder[T]()
    val toRow = enc.createSerializer()
    val encoded = data.map(d => toRow(d).copy())
    val plan = new LocalRelation(DataTypeUtils.toAttributes(enc.schema), encoded)
    val queryExecution = sparkSession.sessionState.executePlan(plan)
    new Dataset[T](queryExecution, enc)
  }
}

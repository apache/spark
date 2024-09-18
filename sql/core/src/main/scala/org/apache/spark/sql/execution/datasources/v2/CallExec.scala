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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.trees.LeafLike
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, truncatedString}
import org.apache.spark.sql.connector.catalog.{Identifier, ProcedureCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.procedures.Procedure
import org.apache.spark.sql.execution.SparkPlan

/**
 * The physical plan of the CALL statement used in EXPLAIN.
 */
case class CallExec(
    catalog: ProcedureCatalog,
    ident: Identifier,
    procedure: Procedure,
    args: Seq[Expression]) extends SparkPlan with LeafLike[SparkPlan] {

  override def output: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    val name = s"${quoteIfNeeded(catalog.name)}.${ident.quoted}"
    val argsString = truncatedString(args, ", ", maxFields)
    s"Call $name($argsString)"
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
}

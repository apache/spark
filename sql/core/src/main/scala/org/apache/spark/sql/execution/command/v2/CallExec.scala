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

package org.apache.spark.sql.execution.command.v2

import scala.jdk.CollectionConverters.IteratorHasAsScala

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, truncatedString}
import org.apache.spark.sql.connector.catalog.{Identifier, ProcedureCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.procedures.SQLInvocableProcedure
import org.apache.spark.sql.connector.read.LocalScan
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * A physical plan for the CALL command.
 */
case class CallExec(
    catalog: ProcedureCatalog,
    ident: Identifier,
    procedure: SQLInvocableProcedure,
    args: Seq[Expression],
    output: Seq[Attribute]) extends LeafV2CommandExec {

  override def simpleString(maxFields: Int): String = {
    val name = s"${quoteIfNeeded(catalog.name)}.${ident.quoted}"
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    val argsString = truncatedString(args, ", ", maxFields)
    s"Call $outputString $name($argsString)"
  }

  override protected def run(): Seq[InternalRow] = {
    val input = toInternalRow(args)
    val scans = procedure.call(input).asScala.toSeq
    scans match {
      case Nil =>
        Nil
      case Seq(scan: LocalScan) =>
        scan.rows.toSeq
      case Seq(scan) =>
        throw new UnsupportedOperationException(
          s"Only local scans are temporarily supported: ${scan.getClass.getName}")
      case _ =>
        throw new IllegalStateException("Procedure invoked via SQL returned multiple scans")
    }
  }

  private def toInternalRow(exprs: Seq[Expression]): InternalRow = {
    val values = exprs.map(_.eval()).toArray
    new GenericInternalRow(values)
  }
}

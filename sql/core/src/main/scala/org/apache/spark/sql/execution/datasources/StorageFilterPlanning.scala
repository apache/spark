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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, ExpressionSet, GetStructField, If, Literal}
import org.apache.spark.sql.execution.{ScalarSubquery => ExecScalarSubquery, WholeStageCodegenExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

/**
 * The rules file-source and DSv2 scans share for storage filters, see
 * `FileFormat.buildReaderWithStorageFilters` and `SupportsStorageFilters`. Each strategy decides
 * on its own whether its scan can take any.
 */
object StorageFilterPlanning {

  /**
   * The conjuncts of `filters` offered to the scan, each once, as an `ExpressionSet` counts them.
   * Each one:
   *  - is deterministic, since the post-scan `Filter` takes the reader's word on the rows it
   *    checked;
   *  - references at least one column, and only `keyCandidates`;
   *  - is taken by `accepts`.
   *
   * Nothing is offered when the offered conjuncts reference every one of `readColumns`, since the
   * reader then reads the same columns for the same rows either way.
   */
  def offeredConjuncts(
      filters: Seq[Expression],
      keyCandidates: AttributeSet,
      readColumns: AttributeSet,
      accepts: Expression => Boolean): Seq[Expression] = {
    val offered = ExpressionSet(filters).toSeq.filter { expr =>
      val refs = expr.references
      expr.deterministic && refs.nonEmpty && refs.subsetOf(keyCandidates) && accepts(expr)
    }
    if (offered.isEmpty) return Nil
    val storageKeyAttrs = AttributeSet.fromAttributeSets(offered.map(_.references))
    if (readColumns.subsetOf(storageKeyAttrs)) Nil else offered
  }

  /**
   * The column the reader marks the rows it checked in, when a filter is `offered`. The caller
   * places it in the scan's output. Only where whole-stage codegen is on and `output` with the
   * column still fits it, so the column keeps the scan in whole-stage codegen. Elsewhere the
   * Project that drops the column could copy every surviving row. A post-scan conjunct that cannot
   * be generated still puts that Project in a stage of its own, the trade-off SPARK-60034 accepted.
   */
  def checkedColumn(
      conf: SQLConf,
      offered: Seq[Expression],
      output: Seq[Attribute]): Option[AttributeReference] = {
    if (offered.isEmpty) return None
    val checked = AttributeReference(FileFormat.STORAGE_FILTER_CHECKED_COLUMN_NAME, BooleanType)()
    Option.when(conf.wholeStageEnabled &&
      !WholeStageCodegenExec.isTooManyFields(conf, (output :+ checked).toStructType))(checked)
  }

  /**
   * `filters` with each of `offered` skipped on the rows `checked` marks, or as they are without
   * the column. Matched by `ExpressionSet`, as `offered` was built. An `If` keeps two things an
   * `Or` would lose:
   *  - a null mark leaves the conjunct as it was, an `And` around it included;
   *  - subexpression elimination cannot hoist what the conjunct shares with another one ahead of
   *    the skip.
   */
  def skipCheckedRows(
      filters: Seq[Expression],
      offered: Seq[Expression],
      checked: Option[Attribute]): Seq[Expression] = checked match {
    case Some(c) =>
      val offeredSet = ExpressionSet(offered)
      filters.map(f => if (offeredSet.contains(f)) If(c, Literal.TrueLiteral, f) else f)
    case None => filters
  }

  /**
   * `filter` with each scalar subquery replaced by its result, for the readers. A field read out of
   * a result becomes a literal of its own. A subquery `MergeSubplans` merged returns the results of
   * several subqueries in one struct, such as the blooms of several filters, so a field read out of
   * it would otherwise carry the whole struct.
   */
  def withSubqueryResults(filter: Expression): Expression = filter.transformUp {
    case s: ExecScalarSubquery => s.toLiteral
    case g @ GetStructField(_: Literal, _, _) => Literal(g.eval(), g.dataType)
  }
}

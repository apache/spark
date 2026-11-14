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

package org.apache.spark.sql.catalyst.parser

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.ParseTree

import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.util.SparkParserUtils
import org.apache.spark.sql.errors.QueryParsingErrors

/** Checks where the `?` optional-reference operator can appear in a SQL statement. */
object OptionalReferenceValidation {

  /**
   * Throws an error for the optional reference `ctx`, i.e. the parse tree of a reference with a
   * `?` after one of its name parts, if `ctx` is part of an UPDATE, DELETE, MERGE, or AUTO CDC
   * statement, of the condition of INSERT ... REPLACE, of a partition specification, or of an
   * expression that defines a table object.
   *
   * The query of an INSERT, including INSERT OVERWRITE, and of a CREATE OR REPLACE TABLE AS
   * SELECT is allowed to have optional references: like any query, it only reads data, and the
   * rows that these statements replace do not depend on it.
   *
   * {{{
   *   UPDATE t SET a = b?;                                  -- IN_DML
   *   WITH s AS (SELECT a? FROM u) DELETE FROM t WHERE ...; -- IN_DML
   *   TRUNCATE TABLE t PARTITION (p = :p?);                 -- IN_PARTITION_SPEC
   *   CREATE TABLE t (a INT, b INT DEFAULT a?);             -- IN_DDL
   *   INSERT OVERWRITE t SELECT a? FROM s;                  -- allowed
   * }}}
   */
  def validate(ctx: ParserRuleContext): Unit = {
    Iterator
      .iterate(ctx.getParent)(_.getParent)
      .takeWhile(_ != null)
      .flatMap(unsupportedContextError(_, ctx))
      .nextOption()
      .foreach(error => throw error)
  }

  /**
   * Validates the optional named parameter markers in the parse tree `tree` with [[validate]].
   * This is for the markers that are substituted in `sqlText`, the text that `tree` was parsed
   * from, before the statement is parsed into a plan.
   */
  def validateNamedParameterMarkers(tree: ParseTree, sqlText: String): Unit = tree match {
    case namedParameter: NamedParameterLiteralContext if namedParameter.optionalMarker != null =>
      SparkParserUtils.withOrigin(namedParameter, Some(sqlText)) {
        validate(namedParameter)
      }
    case _ =>
      (0 until tree.getChildCount).foreach { index =>
        validateNamedParameterMarkers(tree.getChild(index), sqlText)
      }
  }

  /**
   * Returns the error for the optional reference `ctx` if its ancestor `ancestor` is a statement
   * or a clause that does not support optional references.
   */
  private def unsupportedContextError(
      ancestor: ParserRuleContext,
      ctx: ParserRuleContext): Option[Throwable] = ancestor match {
    case _: DeleteFromTableContext =>
      Some(QueryParsingErrors.optionalReferenceInDmlNotSupportedError("DELETE", ctx))
    case _: UpdateTableContext =>
      Some(QueryParsingErrors.optionalReferenceInDmlNotSupportedError("UPDATE", ctx))
    case _: MergeIntoTableContext =>
      Some(QueryParsingErrors.optionalReferenceInDmlNotSupportedError("MERGE", ctx))
    case _: InsertIntoReplaceBooleanCondContext =>
      Some(QueryParsingErrors.optionalReferenceInDmlNotSupportedError("INSERT", ctx))
    case dmlStatement: DmlStatementContext =>
      // The common table expressions of a statement are siblings of its DELETE, UPDATE, or MERGE.
      unsupportedContextError(dmlStatement.dmlStatementNoWith, ctx)
    case _: AutoCdcParametersContext =>
      Some(QueryParsingErrors.optionalReferenceInDmlNotSupportedError("AUTO CDC", ctx))
    case _: PartitionSpecContext =>
      // An unbound `:name?` would make e.g. TRUNCATE TABLE or INSERT target the NULL partition.
      // Statements that only read a partition, such as SHOW PARTITIONS, reject it as well.
      Some(QueryParsingErrors.optionalReferenceInPartitionSpecNotSupportedError(ctx))
    case _: GeneratedColumnContext =>
      Some(QueryParsingErrors.optionalReferenceInDdlNotSupportedError("GENERATED ALWAYS AS", ctx))
    case _: DefaultExpressionContext =>
      Some(QueryParsingErrors.optionalReferenceInDdlNotSupportedError("DEFAULT", ctx))
    case _: CheckConstraintContext =>
      Some(QueryParsingErrors.optionalReferenceInDdlNotSupportedError("CHECK", ctx))
    case _: ExpressionPropertyListContext =>
      Some(QueryParsingErrors.optionalReferenceInDdlNotSupportedError("OPTIONS", ctx))
    case _ =>
      None
  }
}

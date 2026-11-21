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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.LogKeys
import org.apache.spark.sql.catalyst.analysis.ApplyCharTypePaddingHelper
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Cast, Expression, NamedExpression, OuterReference, SubExprUtils, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan, OverwriteByExpression, Project, ReplaceData, UnionLoop, View, WriteDelta}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMAND, REPLACE_DATA, WRITE_DELTA}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExternalLookupRelation,
  ExtractV2Table}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * This rule performs string padding for char type.
 *
 * When reading values from column/field of type CHAR(N), right-pad the values to length N, if the
 * read-side padding config is turned on.
 *
 * When comparing char type column/field with string literal or char type column/field,
 * right-pad the shorter one to the longer length.
 */
object ApplyCharTypePadding extends Rule[LogicalPlan] {

  private val readSidePaddingOverrideWarned = new AtomicBoolean(false)

  private def warnReadSidePaddingOverride(): Unit = {
    if (readSidePaddingOverrideWarned.compareAndSet(false, true)) {
      logWarning(log"${MDC(LogKeys.CONFIG, SQLConf.READ_SIDE_CHAR_PADDING.key)} is disabled but " +
        log"${MDC(LogKeys.CONFIG2, SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key)} is enabled; " +
        log"read-side CHAR/VARCHAR checks are still applied because standard semantics require " +
        log"a read to observe the value a write would have produced.")
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // standardSemantics takes precedence over legacy charVarcharAsString.
    if (conf.charVarcharAsString && !conf.charVarcharStandardSemantics) {
      return plan
    }

    if (conf.charVarcharStandardSemantics && !conf.readSideCharPadding) {
      warnReadSidePaddingOverride()
    }

    if (conf.readSideCharPadding || conf.charVarcharStandardSemantics) {
      // A DELETE that is not rewritten as a row-level operation does not read its table, so the
      // table is not padded, and the condition gets the padding instead, see padDeleteConditions.
      val newPlan = plan.transformUpWithNewOutput({
        case r: LogicalRelation =>
          ApplyCharTypePaddingHelper.readSidePadding(r, () =>
            r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata)))
        case r: DataSourceV2Relation =>
          ApplyCharTypePaddingHelper.readSidePadding(r, () =>
            r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata)))
        // A lookup-only table is read through an external lookup join, which evaluates the
        // padding projection on the lookup results.
        case r: ExternalLookupRelation =>
          ApplyCharTypePaddingHelper.readSidePadding(r, () =>
            r.copy(relation = r.relation.copy(
              output = r.relation.output.map(CharVarcharUtils.cleanAttrMetadata))))
        case r: HiveTableRelation =>
          ApplyCharTypePaddingHelper.readSidePadding(r, () => {
            val cleanedDataCols = r.dataCols.map(CharVarcharUtils.cleanAttrMetadata)
            val cleanedPartCols = r.partitionCols.map(CharVarcharUtils.cleanAttrMetadata)
            r.copy(dataCols = cleanedDataCols, partitionCols = cleanedPartCols)
          })
      }, skipCond = p => p.analyzed || p.isInstanceOf[DeleteFromTable], canGetOutput = _.resolved)
      padRowLevelConditions(padDeleteConditions(
        ApplyCharTypePaddingHelper.paddingForStringComparison(
          narrowCharColumns(newPlan), padCharCol = false)))
    } else {
      ApplyCharTypePaddingHelper.paddingForStringComparison(
        narrowCharColumns(plan),
        padCharCol = !conf.getConf(SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE))
    }
  }

  /**
   * Set operations of CHAR columns of different lengths have the widest length, see
   * [[SetOperationCharPadding]]. This narrows the values of recursive CTEs and views, whose
   * columns keep shorter lengths, to those lengths.
   */
  private def narrowCharColumns(plan: LogicalPlan): LogicalPlan =
    narrowViewColumns(narrowRecursiveTerms(plan))

  /**
   * The columns of a recursive CTE have the CHAR lengths of its anchor. This narrows values of the
   * recursive term with longer CHAR lengths, such as those of a set operation in it, to them, as
   * [[narrowViewColumns]] does for a view.
   */
  private def narrowRecursiveTerms(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case loop: UnionLoop if loop.resolved =>
        val recursion = loop.recursion
        val projectList = recursion.output.zip(loop.anchor.output).map { case (attr, anchorAttr) =>
          (for {
            rawType <- CharVarcharUtils.getRawType(attr.metadata, attr.dataType)
            anchorType <- CharVarcharUtils.getRawType(anchorAttr.metadata, anchorAttr.dataType)
            narrowed <- narrowToLength(attr, rawType, anchorType)
          } yield Alias(narrowed, attr.name)(
            qualifier = attr.qualifier,
            explicitMetadata = Some(CharVarcharUtils.withRawType(attr.metadata, anchorType))
          )).getOrElse(attr)
        }
        if (projectList == recursion.output) {
          loop
        } else {
          loop.copy(recursion = Project(projectList, recursion))
        }
    }
  }

  // The value narrowed to the CHAR lengths of `toType`, with the data type of the value. A rebuilt
  // struct loses the metadata of its fields, which holds their raw types.
  private def narrowToLength(
      expr: Expression,
      fromType: DataType,
      toType: DataType): Option[Expression] = {
    CharVarcharUtils.narrowCharToLength(expr, fromType, toType).map { narrowed =>
      if (narrowed.dataType == expr.dataType) {
        narrowed
      } else {
        Cast(narrowed, expr.dataType, Some(conf.sessionLocalTimeZone))
      }
    }
  }

  /**
   * A view stores the CHAR types of its columns, and keeps them for the values of its query. A view
   * of a set operation created before the output had the widest length of its children stores the
   * length of the first child. This narrows the values back to that length, as the stored type
   * says, so that comparisons padded to it still match them. The columns keep their expression IDs
   * and metadata.
   */
  private def narrowViewColumns(plan: LogicalPlan): LogicalPlan = {
    def queryAttr(e: Expression): Option[Attribute] = e match {
      case a: Attribute => Some(a)
      case c: Cast => queryAttr(c.child)
      case _ => None
    }
    plan.resolveOperatorsUp {
      case v @ View(_, _, p @ Project(projectList, _), _) =>
        val newProjectList = projectList.map {
          case a @ Alias(child, _) =>
            (for {
              viewType <- CharVarcharUtils.getRawType(a.metadata, a.dataType)
              attr <- queryAttr(child)
              queryType <- CharVarcharUtils.getRawType(attr.metadata, attr.dataType)
              narrowed <- narrowToLength(child, queryType, viewType)
            } yield a.withNewChildren(Seq(narrowed)).asInstanceOf[Alias]).getOrElse(a)
          case other => other
        }
        if (newProjectList == projectList) {
          v
        } else {
          v.copy(child = p.copy(projectList = newProjectList))
        }
    }
  }

  /**
   * A DELETE that is not rewritten as a row-level operation, and an overwrite by expression such
   * as `INSERT INTO ... REPLACE WHERE`, delete with filters translated from their condition, which
   * the data source compares with the stored values. To match what a read of the table returns,
   * the condition evaluates the read-side padding of the columns itself, after its comparisons are
   * padded. The data source can only delete with such a filter if it supports the padding, e.g.
   * `CHAR_READ_SIDE_PADDING`, or, with V1 filters, the IN or equality that `PredicateUtils.toV1`
   * converts it into.
   */
  private def padDeleteConditions(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(COMMAND)) {
      case d: DeleteFromTable => d.copy(condition = padRawTypedColumns(d.condition))
      // Under standard semantics, the read-side check of the columns cannot be translated to a
      // filter, so the overwrite keeps comparing the stored values.
      case o: OverwriteByExpression if !conf.charVarcharStandardSemantics &&
          !conf.getConf(SQLConf.LEGACY_NO_CHAR_PADDING_IN_OVERWRITE_CONDITION) =>
        o.copy(deleteExpr = padRawTypedColumns(o.deleteExpr))
    }
  }

  private def padRawTypedColumns(condition: Expression): Expression = condition.transform {
    // the padded value refers to the attribute without the raw type and is not padded again
    case attr: AttributeReference if CharVarcharUtils.getRawType(attr.metadata).isDefined =>
      CharVarcharUtils.addPaddingForScan(attr).transform {
        case a: AttributeReference => CharVarcharUtils.cleanAttrMetadata(a)
      }
  }

  /**
   * The conditions of a row-level write are not evaluated on top of the padded read in its query:
   * they are pushed down to the scan and to metadata-only deletes, and they select the groups to
   * read at runtime. To compare padded values with the padded literals and columns, as the read
   * does, they must apply the padding of the target columns themselves.
   */
  private def padRowLevelConditions(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsAnyPattern(REPLACE_DATA, WRITE_DELTA)) {
      case rd: ReplaceData =>
        val paddings = targetColumnPaddings(rd.query)
        rd.copy(
          condition = padColumns(rd.condition, paddings),
          groupFilterCondition = rd.groupFilterCondition.map(padColumns(_, paddings)))
      case wd: WriteDelta =>
        val paddings = targetColumnPaddings(wd.query)
        wd.copy(
          condition = padColumns(wd.condition, paddings),
          groupFilterCondition = wd.groupFilterCondition.map(padColumns(_, paddings)))
    }
  }

  // Maps the stored and the padded attribute of each padded target column to the padded value.
  // A condition can refer to either one. The read-side padding rewrites the attributes a node
  // references, and `ReplaceData.references` is `query.outputSet`, so a group-based DELETE refers
  // to the padded alias. A delta-based DELETE does not output the column and keeps the stored one.
  // The aliases of the padding keep the raw type, unlike the new values that an UPDATE also
  // projects directly on top of the read relation.
  private def targetColumnPaddings(query: LogicalPlan): AttributeMap[Expression] = {
    AttributeMap(query.collect {
      case Project(projectList, ExtractV2Table(_: RowLevelOperationTable)) =>
        projectList.collect {
          case alias: Alias if hasRawType(alias) =>
            val padded = alias.child.transform {
              case attr: AttributeReference => CharVarcharUtils.cleanAttrMetadata(attr)
            }
            (alias.toAttribute +: alias.child.references.toSeq).map(_ -> padded)
        }.flatten
    }.flatten)
  }

  private def hasRawType(expr: NamedExpression): Boolean = {
    CharVarcharUtils.getRawType(expr.metadata).isDefined
  }

  private def padColumns(cond: Expression, paddings: AttributeMap[Expression]): Expression = {
    // The padded value refers to the attribute without the raw type, which is not padded again.
    def isPadded(attr: AttributeReference): Boolean = paddings.contains(attr) && hasRawType(attr)

    def pad(expr: Expression): Expression = expr match {
      case attr: AttributeReference if isPadded(attr) =>
        paddings(attr)

      // The analyzer pads the subqueries it resolves, but the group filter subquery of a MERGE is
      // built from a resolved source, which only gets its read-side padding here. `apply` leaves
      // a subquery that is already padded unchanged: its relations no longer have the raw type,
      // and its padded comparisons are not padded again. The outer attributes are collected from
      // the new plan, as the analyzer does, because they may refer to the padded alias.
      case subquery: SubqueryExpression =>
        val newPlan = apply(subquery.plan).transformAllExpressionsWithSubqueries {
          case OuterReference(attr: AttributeReference) if isPadded(attr) =>
            paddings(attr).transform { case a: AttributeReference => OuterReference(a) }
        }
        subquery.withNewPlan(newPlan).withNewOuterAttrs(SubExprUtils.getOuterReferences(newPlan))

      case other =>
        other.mapChildren(pad)
    }

    pad(cond)
  }
}

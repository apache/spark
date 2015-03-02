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

package org.apache.spark.sql.sources

import org.apache.spark.sql.{SaveMode, AnalysisException}
import org.apache.spark.sql.catalyst.analysis.{EliminateSubQueries, Catalog}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Alias}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DataType

/**
 * A rule to do pre-insert data type casting and field renaming. Before we insert into
 * an [[InsertableRelation]], we will use this rule to make sure that
 * the columns to be inserted have the correct data type and fields have the correct names.
 */
private[sql] object PreInsertCastAndRename extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      // We are inserting into an InsertableRelation.
      case i @ InsertIntoTable(
      l @ LogicalRelation(r: InsertableRelation), partition, child, overwrite) => {
        // First, make sure the data to be inserted have the same number of fields with the
        // schema of the relation.
        if (l.output.size != child.output.size) {
          sys.error(
            s"$l requires that the query in the SELECT clause of the INSERT INTO/OVERWRITE " +
              s"statement generates the same number of columns as its schema.")
        }
        castAndRenameChildOutput(i, l.output, child)
      }
  }

  /** If necessary, cast data types and rename fields to the expected types and names. */
  def castAndRenameChildOutput(
      insertInto: InsertIntoTable,
      expectedOutput: Seq[Attribute],
      child: LogicalPlan) = {
    val newChildOutput = expectedOutput.zip(child.output).map {
      case (expected, actual) =>
        val needCast = !DataType.equalsIgnoreNullability(expected.dataType, actual.dataType)
        // We want to make sure the filed names in the data to be inserted exactly match
        // names in the schema.
        val needRename = expected.name != actual.name
        (needCast, needRename) match {
          case (true, _) => Alias(Cast(actual, expected.dataType), expected.name)()
          case (false, true) => Alias(actual, expected.name)()
          case (_, _) => actual
        }
    }

    if (newChildOutput == child.output) {
      insertInto
    } else {
      insertInto.copy(child = Project(newChildOutput, child))
    }
  }
}

/**
 * A rule to do various checks before inserting into or writing to a data source table.
 */
private[sql] case class PreWriteCheck(catalog: Catalog) extends (LogicalPlan => Unit) {
  def failAnalysis(msg: String) = { throw new AnalysisException(msg) }

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case i @ logical.InsertIntoTable(
        l @ LogicalRelation(t: InsertableRelation), partition, query, overwrite) =>
        // Right now, we do not support insert into a data source table with partition specs.
        if (partition.nonEmpty) {
          failAnalysis(s"Insert into a partition is not allowed because $l is not partitioned.")
        } else {
          // Get all input data source relations of the query.
          val srcRelations = query.collect {
            case LogicalRelation(src: BaseRelation) => src
          }
          if (srcRelations.contains(t)) {
            failAnalysis(
              "Cannot insert overwrite into table that is also being read from.")
          } else {
            // OK
          }
        }

      case i @ logical.InsertIntoTable(
        l: LogicalRelation, partition, query, overwrite) if !l.isInstanceOf[InsertableRelation] =>
        // The relation in l is not an InsertableRelation.
        failAnalysis(s"$l does not allow insertion.")

      case CreateTableUsingAsSelect(tableName, _, _, SaveMode.Overwrite, _, query) =>
        // When the SaveMode is Overwrite, we need to check if the table is an input table of
        // the query. If so, we will throw an AnalysisException to let users know it is not allowed.
        if (catalog.tableExists(Seq(tableName))) {
          // Need to remove SubQuery operator.
          EliminateSubQueries(catalog.lookupRelation(Seq(tableName))) match {
            // Only do the check if the table is a data source table
            // (the relation is a BaseRelation).
            case l @ LogicalRelation(dest: BaseRelation) =>
              // Get all input data source relations of the query.
              val srcRelations = query.collect {
                case LogicalRelation(src: BaseRelation) => src
              }
              if (srcRelations.contains(dest)) {
                failAnalysis(
                  s"Cannot overwrite table $tableName that is also being read from.")
              } else {
                // OK
              }

            case _ => // OK
          }
        } else {
          // OK
        }

      case _ => // OK
    }
  }
}

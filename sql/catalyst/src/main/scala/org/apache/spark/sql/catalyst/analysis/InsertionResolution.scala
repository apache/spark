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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, Cast, EqualNullSafe, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, InsertIntoStatement, InsertReplaceWhere, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, Project, V2WriteCommand}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._

/** Resolves the query projection and write mode of INSERT statements. */
private[sql] object InsertionResolution {
  /**
   * Lowers an INSERT into a V2 table whose input query has been resolved.
   * The caller rejects REPLACE ON/USING; generated partition expressions still need resolution.
   * Configuration is deferred to preserve the caller's receiver and read timing.
   */
  def resolveInsertInto(
      i: InsertIntoStatement,
      createProject: (String, InsertIntoStatement) => LogicalPlan,
      conf: => SQLConf): V2WriteCommand = {
    val r = i.table.asInstanceOf[DataSourceV2Relation]
    // ifPartitionNotExists is append with validation, but validation is not supported
    if (i.ifPartitionNotExists) {
      throw QueryCompilationErrors.unsupportedIfNotExistsError(r.table.name)
    }

    // Create a project if this is an INSERT INTO BY NAME query.
    val projectByName = if (i.userSpecifiedCols.nonEmpty) {
      Some(createProject(r.table.name, i))
    } else {
      None
    }
    val isByName = projectByName.nonEmpty || i.byName

    val partCols = partitionColumnNames(r.table)
    validatePartitionSpec(partCols, i.partitionSpec, conf)

    val staticPartitions = i.partitionSpec.filter(_._2.isDefined).transform((_, v) => v.get)
    val query = addStaticPartitionColumns(r, projectByName.getOrElse(i.query), staticPartitions,
      isByName, conf)

    if (!i.overwrite) {
      if (isByName) {
        AppendData.byName(
          r,
          query,
          withSchemaEvolution = i.withSchemaEvolution)
      } else {
        AppendData.byPosition(
          r,
          query,
          withSchemaEvolution = i.withSchemaEvolution)
      }
    // Dynamic partition overwrite applies only to plain INSERT OVERWRITE. REPLACE WHERE always
    // deletes by its condition, so it falls through to the OverwriteByExpression branch below
    // even when the session is in DYNAMIC partition-overwrite mode.
    } else if (i.replaceCriteriaOpt.isEmpty &&
        conf.partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC) {
      if (isByName) {
        OverwritePartitionsDynamic.byName(
          r,
          query,
          withSchemaEvolution = i.withSchemaEvolution)
      } else {
        OverwritePartitionsDynamic.byPosition(
          r,
          query,
          withSchemaEvolution = i.withSchemaEvolution)
      }
    } else {
      val deleteExpr = i.replaceCriteriaOpt match {
        case Some(InsertReplaceWhere(condition)) =>
          if (staticPartitions.nonEmpty) {
            throw SparkException.internalError(
              s"REPLACE WHERE must not carry static partitions, but got: $staticPartitions")
          }
          condition
        case Some(other) => throw SparkException.internalError(
          s"Replace criteria ${other.getClass.getSimpleName} must not reach " +
            "ResolveInsertInto; REPLACE ON/USING are rejected earlier.")
        case None => staticDeleteExpression(r, staticPartitions, conf)
      }
      if (isByName) {
        OverwriteByExpression.byName(
          table = r,
          df = query,
          deleteExpr = deleteExpr,
          withSchemaEvolution = i.withSchemaEvolution)
      } else {
        OverwriteByExpression.byPosition(
          table = r,
          query = query,
          deleteExpr = deleteExpr,
          withSchemaEvolution = i.withSchemaEvolution)
      }
    }
  }

  /** Add a project to use the table column names for INSERT INTO BY NAME */
  def createProjectForByNameQuery(
      tblName: String,
      i: InsertIntoStatement,
      resolver: => Resolver): LogicalPlan = {
    SchemaUtils.checkColumnNameDuplication(i.userSpecifiedCols, resolver)

    if (i.userSpecifiedCols.size != i.query.output.size) {
      if (i.userSpecifiedCols.size > i.query.output.size) {
        throw QueryCompilationErrors.cannotWriteNotEnoughColumnsToTableError(
          tblName, i.userSpecifiedCols, i.query.output)
      } else {
        throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(
          tblName, i.userSpecifiedCols, i.query.output)
      }
    }
    val projectByName = i.userSpecifiedCols.zip(i.query.output)
      .map { case (userSpecifiedCol, queryOutputCol) =>
        val resolvedCol = i.table.resolve(Seq(userSpecifiedCol), resolver)
          .getOrElse(
            throw QueryCompilationErrors.unresolvedAttributeError(
              "UNRESOLVED_COLUMN", userSpecifiedCol, i.table.output.map(_.name), i.origin))
        resolveByNameColumn(
          queryOutputCol, resolvedCol, (child, name) => Alias(child, name)())
      }
    Project(projectByName, i.query)
  }

  /**
   * Projects one resolved input column using the target name and nested field names.
   * The alias factory lets the caller own the new expression ID.
   */
  def resolveByNameColumn(
      queryOutputColumn: Attribute,
      targetColumn: NamedExpression,
      newAlias: (Expression, String) => Alias): Alias = {
    (queryOutputColumn.dataType, targetColumn.dataType) match {
      case (input: StructType, expected: StructType) =>
        newAlias(Cast(queryOutputColumn, renameFieldsInStruct(input, expected)), targetColumn.name)
      case (input: ArrayType, expected: ArrayType) =>
        newAlias(Cast(queryOutputColumn, renameFieldsInType(input, expected)), targetColumn.name)
      case (input: MapType, expected: MapType) =>
        newAlias(Cast(queryOutputColumn, renameFieldsInType(input, expected)), targetColumn.name)
      case _ =>
        newAlias(queryOutputColumn, targetColumn.name)
    }
  }

  private def renameFieldsInStruct(input: StructType, expected: StructType): StructType = {
    if (input.length == expected.length) {
      val newFields = input.zip(expected).map { case (f1, f2) =>
        f1.copy(name = f2.name, dataType = renameFieldsInType(f1.dataType, f2.dataType))
      }
      StructType(newFields)
    } else {
      input
    }
  }

  // Recursively rename fields so that positional INSERT analysis applies at every nesting level,
  // including structs inside arrays and maps.  See SPARK-58816.
  private def renameFieldsInType(input: DataType, expected: DataType): DataType =
    (input, expected) match {
      case (s1: StructType, s2: StructType) =>
        renameFieldsInStruct(s1, s2)
      case (ArrayType(e1, n1), ArrayType(e2, _)) =>
        ArrayType(renameFieldsInType(e1, e2), n1)
      case (MapType(k1, v1, n1), MapType(k2, v2, _)) =>
        MapType(renameFieldsInType(k1, k2), renameFieldsInType(v1, v2), n1)
      case _ =>
        input
    }

  private def partitionColumnNames(table: Table): Seq[String] = {
    // get partition column names. in v2, partition columns are columns that are stored using an
    // identity partition transform because the partition values and the column values are
    // identical. otherwise, partition values are produced by transforming one or more source
    // columns and cannot be set directly in a query's PARTITION clause.
    table.partitioning.flatMap {
      case IdentityTransform(FieldReference(Seq(name))) => Some(name)
      case _ => None
    }.toImmutableArraySeq
  }

  private def validatePartitionSpec(
      partitionColumnNames: Seq[String],
      partitionSpec: Map[String, Option[String]],
      conf: => SQLConf): Unit = {
    // check that each partition name is a partition column. otherwise, it is not valid
    partitionSpec.keySet.foreach { partitionName =>
      partitionColumnNames.find(name => conf.resolver(name, partitionName)) match {
        case Some(_) =>
        case None =>
          throw QueryCompilationErrors.nonPartitionColError(partitionName)
      }
    }
  }

  private def addStaticPartitionColumns(
      relation: DataSourceV2Relation,
      query: LogicalPlan,
      staticPartitions: Map[String, String],
      isByName: Boolean,
      conf: => SQLConf): LogicalPlan = {

    if (staticPartitions.isEmpty) {
      query

    } else {
      // add any static value as a literal column
      val withStaticPartitionValues = {
        // for each static name, find the column name it will replace and check for unknowns.
        val outputNameToStaticName = staticPartitions.keySet.map { staticName =>
          if (isByName) {
            // If this is INSERT INTO BY NAME, the query output's names will be the user specified
            // column names. We need to make sure the static partition column name doesn't appear
            // there to catch the following ambiguous query:
            // INSERT OVERWRITE t PARTITION (c='1') (c) VALUES ('2')
            if (query.output.exists(col => conf.resolver(col.name, staticName))) {
              throw QueryCompilationErrors.staticPartitionInUserSpecifiedColumnsError(staticName)
            }
          }
          relation.output.find(col => conf.resolver(col.name, staticName)) match {
            case Some(attr) =>
              attr.name -> staticName
            case _ =>
              throw QueryCompilationErrors.missingStaticPartitionColumn(staticName)
          }
        }.toMap

        val queryColumns = query.output.iterator

        // for each output column, add the static value as a literal, or use the next input
        // column. this does not fail if input columns are exhausted and adds remaining columns
        // at the end. both cases will be caught by ResolveOutputRelation and will fail the
        // query with a helpful error message.
        relation.output.flatMap { col =>
          outputNameToStaticName.get(col.name).flatMap(staticPartitions.get) match {
            case Some(staticValue) =>
              // SPARK-30844: try our best to follow StoreAssignmentPolicy for static partition
              // values but not completely follow because we can't do static type checking due to
              // the reason that the parser has erased the type info of static partition values
              // and converted them to string.
              val cast = Cast(Literal(staticValue), col.dataType, ansiEnabled = true)
              cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
              Some(Alias(cast, col.name)())
            case _ if queryColumns.hasNext =>
              Some(queryColumns.next())
            case _ =>
              None
          }
        } ++ queryColumns
      }

      Project(withStaticPartitionValues, query)
    }
  }

  private def staticDeleteExpression(
      relation: DataSourceV2Relation,
      staticPartitions: Map[String, String],
      conf: => SQLConf): Expression = {
    if (staticPartitions.isEmpty) {
      Literal(true)
    } else {
      staticPartitions.map { case (name, value) =>
        relation.output.find(col => conf.resolver(col.name, name)) match {
          case Some(attr) =>
            // the delete expression must reference the table's column names, but these attributes
            // are not available when CheckAnalysis runs because the relation is not a child of
            // the logical operation. instead, expressions are resolved after
            // ResolveOutputRelation runs, using the query's column names that will match the
            // table names at that point. because resolution happens after a future rule, create
            // an UnresolvedAttribute.
            EqualNullSafe(
              UnresolvedAttribute.quoted(attr.name),
              Cast(Literal(value), attr.dataType))
          case None =>
            throw QueryCompilationErrors.missingStaticPartitionColumn(name)
        }
      }.reduce(And)
    }
  }
}

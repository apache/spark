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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.{V2CreateTableAsSelectPlan, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.util.{TypeUtils, WriteDistributionAndOrdering}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.expressions.ClusterByTransform
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._

/**
 * Validates partition references, column types (including the FILE type rules), write ordering
 * and generated columns.
 */
private[sql] object CreateTableValidation {
  def validateV2CreateTablePlan(create: V2CreateTablePlan, conf: SQLConf): Unit = {
    val references = create.partitioning.flatMap(_.references).toSet
    val badReferences = references.map(_.fieldNames).flatMap { column =>
      create.tableSchema.findNestedField(column.toImmutableArraySeq) match {
        case Some(_) =>
          None
        case _ =>
          Some(column.quoted)
      }
    }

    if (badReferences.nonEmpty) {
      create.failAnalysis(
        errorClass = "UNSUPPORTED_FEATURE.PARTITION_WITH_NESTED_COLUMN_IS_UNSUPPORTED",
        messageParameters = Map(
          "cols" -> badReferences.map(r => toSQLId(r)).mkString(", ")))
    }

    // PreprocessTableCreation keeps a reference to a missing column as is, so this is the
    // only check that rejects it, also for analyzers that do not run that rule.
    val badOrderingReferences = WriteDistributionAndOrdering.unknownReferences(
      create.tableSchema, create.writeOrdering)

    if (badOrderingReferences.nonEmpty) {
      create.failAnalysis(
        errorClass = "WRITE_ORDERING_WITH_UNKNOWN_COLUMN",
        messageParameters = Map(
          "cols" -> badOrderingReferences.map(parts => toSQLId(parts)).mkString(", ")))
    }

    create.tableSchema.foreach(f => TypeUtils.failWithIntervalType(f.dataType))
    TypeUtils.failUnsupportedDataType(create.tableSchema, conf)
    SchemaUtils.checkIndeterminateCollationInSchema(create.tableSchema)
    create match {
      // The columns of CREATE TABLE AS SELECT come from its query, not from DDL.
      case ctas: V2CreateTableAsSelectPlan =>
        TypeUtils.checkNoUnknownFileTypeWrite(ctas.query.output)
      case _ =>
        TypeUtils.checkFileTypeHasQualifier(create.tableSchema)
    }
    // FILE is not comparable, so it cannot be a partition, clustering or write-ordering column.
    create.partitioning.foreach { transform =>
      val usage = transform match {
        case _: ClusterByTransform => "CLUSTERING_COLUMN"
        case _ => "PARTITION_COLUMN"
      }
      transform.references.foreach { ref =>
        create.tableSchema.findNestedField(ref.fieldNames.toImmutableArraySeq).foreach {
          case (_, field) => TypeUtils.checkNoFileTypeInColumnUsage(field, usage)
        }
      }
    }
    create.writeOrdering.flatMap(_.expression.references).foreach { ref =>
      create.tableSchema.findNestedField(ref.fieldNames.toImmutableArraySeq).foreach {
        case (_, field) => TypeUtils.checkNoFileTypeInColumnUsage(field, "WRITE_ORDERING_COLUMN")
      }
    }

    // Validate generated column expressions
    create.columns.foreach { col =>
      col.generationExpression.foreach { genExpr =>
        def unsupportedExpressionError(reason: String): AnalysisException = {
          new AnalysisException(
            errorClass = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
            messageParameters = Map(
              "fieldName" -> col.name,
              "expressionStr" -> genExpr.originalSQL,
              "reason" -> reason
            )
          )
        }
        // Only built-in functions are allowed in generated columns. Functions that are
        // not built-in already fail earlier in analysis: unknown functions with
        // UNRESOLVED_ROUTINE and persistent SQL functions with UNSUPPORTED_SQL_UDF_USAGE.
        // Here we only need to reject user-defined functions (Scala/Python/Java/Hive UDFs
        // and v2 catalog functions), which resolve successfully.
        StoredExpressionFunctions.findUserDefinedFunctionCall(genExpr.child).foreach {
          case (_, functionName) =>
            throw unsupportedExpressionError(
              s"failed to resolve $functionName to a built-in function")
        }
        genExpr.validate(col.name, col.dataType, create.columns)
      }
    }
  }
}

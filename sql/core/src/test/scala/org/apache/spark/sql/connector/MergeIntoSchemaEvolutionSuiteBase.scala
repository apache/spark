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

package org.apache.spark.sql.connector

import java.util.Locale

import org.apache.spark.sql.{Column => ColumnV1, DataFrame}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, TableInfo}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{identity, reference}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Base trait containing merge into schema evolution tests.
 */
trait MergeIntoSchemaEvolutionSuiteBase extends RowLevelOperationSuiteBase {

  import testImplicits._

  // ---------------------------------------------------------------------------
  // MergeClause abstraction for building merge statements programmatically
  // ---------------------------------------------------------------------------

  protected sealed trait MergeClause {
    def condition: String

    def action: String

    def clause: String

    def sql: String = {
      assert(action != null, "action not specified yet")
      val cond = if (condition != null) s"AND $condition" else ""
      s"WHEN $clause $cond THEN $action"
    }
  }

  protected case class MatchedClause(condition: String, action: String) extends MergeClause {
    override def clause: String = "MATCHED"
  }

  protected case class NotMatchedClause(condition: String, action: String) extends MergeClause {
    override def clause: String = "NOT MATCHED"
  }

  protected case class NotMatchedBySourceClause(condition: String, action: String)
    extends MergeClause {
    override def clause: String = "NOT MATCHED BY SOURCE"
  }

  protected def update(set: String = null, condition: String = null): MergeClause = {
    MatchedClause(condition, s"UPDATE SET $set")
  }

  protected def updateAll(condition: String = null): MergeClause = {
    MatchedClause(condition, "UPDATE SET *")
  }

  protected def delete(condition: String = null): MergeClause = {
    MatchedClause(condition, "DELETE")
  }

  protected def insert(values: String = null, condition: String = null): MergeClause = {
    NotMatchedClause(condition, s"INSERT $values")
  }

  protected def insertAll(condition: String = null): MergeClause = {
    NotMatchedClause(condition, "INSERT *")
  }

  protected def updateNotMatched(set: String = null, condition: String = null): MergeClause = {
    NotMatchedBySourceClause(condition, s"UPDATE SET $set")
  }

  protected def deleteNotMatched(condition: String = null): MergeClause = {
    NotMatchedBySourceClause(condition, "DELETE")
  }

  /**
   * Execute a merge operation. Subclasses implement this to use either SQL or DataFrame API.
   *
   * @param withSchemaEvolution Whether to enable schema evolution
   * @param targetTableName     The target table name
   * @param sourceViewName      The source temp view name
   * @param cond                The join condition (e.g., "t.pk = s.pk")
   * @param clauses             The merge clauses (UPDATE, INSERT, DELETE)
   */
  protected def executeMerge(
      withSchemaEvolution: Boolean,
      targetTableName: String,
      sourceViewName: String,
      cond: String,
      clauses: Seq[MergeClause]): Unit

  /**
   * Helper method to test merge schema evolution scenarios.
   * This generates two tests: one with schema evolution disabled and one enabled.
   *
   * If requiresNestedTypeCoercion is true, it generates two additional tests with
   * MERGE_INTO_NESTED_TYPE_COERCION_ENABLED set to false, expecting both to fail.
   *
   * @param name                                Base name for the generated tests
   * @param targetData                          Function returning DataFrame for target table data
   *                                            (schema inferred from it)
   * @param sourceData                          Function returning DataFrame for source data
   * @param cond                                Merge condition (default: "t.pk = s.pk")
   * @param clauses                             Sequence of MergeClause
   *                                            (use update, delete, insert, etc. helpers)
   * @param expected                            Expected rows when schema evolution is enabled
   *                                            (null if error expected)
   * @param expectedWithoutEvolution            Expected rows without evolution
   *                                            (null if error expected)
   * @param expectedSchema                      Expected schema with evolution
   *                                            (null to skip schema check)
   * @param expectedSchemaWithoutEvolution      Expected schema without evolution
   * @param expectErrorContains                 Error message substring expected with evolution
   *                                            (null if no error)
   * @param expectErrorWithoutEvolutionContains Error message substring expected without evolution
   * @param confs                               Additional SQL configurations to apply during test
   * @param disableAutoSchemaEvolution          If true, sets 'auto-schema-evolution' table property
   *                                            to false
   * @param requiresNestedTypeCoercion          If true, enables coercion for main tests and adds
   *                                            coercion-disabled tests that expect failure
   */
  // scalastyle:off argcount
  protected def testEvolution(name: String)(
      targetData: => DataFrame,
      sourceData: => DataFrame,
      cond: String = "t.pk = s.pk",
      clauses: Seq[MergeClause] = Seq.empty,
      expected: => DataFrame = null,
      expectedWithoutEvolution: => DataFrame = null,
      expectedSchema: StructType = null,
      expectedSchemaWithoutEvolution: StructType = null,
      expectErrorContains: String = null,
      expectErrorWithoutEvolutionContains: String = null,
      confs: Seq[(String, String)] = Seq.empty,
      partitionCols: Seq[String] = Seq.empty,
      disableAutoSchemaEvolution: Boolean = false,
      requiresNestedTypeCoercion: Boolean = false): Unit = {

    def executeMergeAndAssert(
        withSchemaEvolution: Boolean,
        expectedDf: DataFrame,
        schema: StructType,
        errorSubstring: String): Unit = {
      withTable(tableNameAsString) {
        withTempView("source") {
          // Set up target table - infer schema from DataFrame
          val targetDf = targetData
          val columns = CatalogV2Util.structTypeToV2Columns(targetDf.schema)
          createTable(columns, partitionCols)
          if (!targetDf.isEmpty) {
            targetDf.writeTo(tableNameAsString).append()
          }

          // Optionally disable auto-schema-evolution via table property
          if (disableAutoSchemaEvolution) {
            sql(
              s"""ALTER TABLE $tableNameAsString SET TBLPROPERTIES
                 | ('auto-schema-evolution' = 'false')""".stripMargin)
          }

          // Set up source
          sourceData.createOrReplaceTempView("source")

          // Execute merge using abstract method (SQL or DataFrame API)
          if (errorSubstring != null) {
            val ex = intercept[Exception] {
              executeMerge(withSchemaEvolution, tableNameAsString, "source", cond, clauses)
            }
            assert(ex.getMessage.contains(errorSubstring),
              s"Expected error containing '$errorSubstring' but got: ${ex.getMessage}")
          } else {
            executeMerge(withSchemaEvolution, tableNameAsString, "source", cond, clauses)
            checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), expectedDf.collect().toSeq)

            if (schema != null) {
              assert(sql(s"SELECT * FROM $tableNameAsString").schema === schema,
                s"Schema mismatch: expected $schema but got " +
                  s"${sql(s"SELECT * FROM $tableNameAsString").schema}")
            }
          }
        }
      }
    }

    // If coercion is required, enable it for the main tests
    val mainConfs = if (requiresNestedTypeCoercion) {
      Seq(SQLConf.MERGE_INTO_NESTED_TYPE_COERCION_ENABLED.key -> "true") ++ confs
    } else {
      confs
    }

    test(s"schema evolution - $name - without evolution clause") {
      withSQLConf(mainConfs: _*) {
        executeMergeAndAssert(
          withSchemaEvolution = false,
          expectedWithoutEvolution,
          expectedSchemaWithoutEvolution,
          expectErrorWithoutEvolutionContains)
      }
    }

    test(s"schema evolution - $name - with evolution clause") {
      withSQLConf(mainConfs: _*) {
        executeMergeAndAssert(
          withSchemaEvolution = true,
          expected,
          expectedSchema,
          expectErrorContains)
      }
    }

    // If coercion is required, add tests with coercion disabled - both should fail
    if (requiresNestedTypeCoercion) {
      val coercionDisabledConfs =
        Seq(SQLConf.MERGE_INTO_NESTED_TYPE_COERCION_ENABLED.key -> "false") ++ confs

      test(s"schema evolution - $name - coercion disabled - without evolution clause") {
        withSQLConf(coercionDisabledConfs: _*) {
          executeMergeAndAssert(
            withSchemaEvolution = false,
            null,
            null,
            expectErrorWithoutEvolutionContains)
        }
      }

      test(s"schema evolution - $name - coercion disabled - with evolution clause") {
        withSQLConf(coercionDisabledConfs: _*) {
          executeMergeAndAssert(
            withSchemaEvolution = true,
            null,
            null,
            expectErrorWithoutEvolutionContains)
        }
      }
    }
  }
  // scalastyle:on argcount

  /**
   * Helper for nested struct evolution tests that uses JSON strings for data.
   * This is more readable for complex nested structures than Row objects.
   * The targetSchema and sourceSchema are used to parse the JSON and create DataFrames.
   */
  // scalastyle:off argcount
  protected def testNestedStructsEvolution(name: String)(
      target: Seq[String],
      source: Seq[String],
      targetSchema: StructType,
      sourceSchema: StructType,
      cond: String = "t.pk = s.pk",
      clauses: Seq[MergeClause] = Seq.empty,
      result: Seq[String] = null,
      resultSchema: StructType = null,
      resultWithoutEvolution: Seq[String] = null,
      expectErrorContains: String = null,
      expectErrorWithoutEvolutionContains: String = null,
      confs: Seq[(String, String)] = Seq.empty,
      partitionCols: Seq[String] = Seq.empty,
      requiresNestedTypeCoercion: Boolean = false): Unit = {
    def readJson(json: Seq[String], schema: StructType): DataFrame = {
      val df = spark.read.schema(schema).json(json.toDS())
      // the schema is inferred from json data sources, so we need to create another dataframe
      spark.createDataFrame(df.rdd, schema)
    }

    testEvolution(name)(
      targetData = readJson(target, targetSchema),
      sourceData = readJson(source, sourceSchema),
      cond = cond,
      clauses = clauses,
      expected =
        if (result != null) {
          val schema = if (resultSchema != null) resultSchema else targetSchema
          readJson(result, schema)
        } else {
          null
        },
      expectedSchema = resultSchema,
      expectErrorContains = expectErrorContains,
      expectedWithoutEvolution =
        if (resultWithoutEvolution != null) {
          readJson(resultWithoutEvolution, targetSchema)
        } else {
          null
        },
      expectedSchemaWithoutEvolution = targetSchema,
      expectErrorWithoutEvolutionContains = expectErrorWithoutEvolutionContains,
      confs = confs,
      partitionCols = partitionCols,
      requiresNestedTypeCoercion = requiresNestedTypeCoercion
    )
  }
  // scalastyle:on argcount

  def createTable(columns: Array[Column], partitionCols: Seq[String]): Unit = {
    val baseBuilder = new TableInfo.Builder()
      .withColumns(columns)
      .withProperties(extraTableProps)
    val tableInfo = if (partitionCols.isEmpty) {
      baseBuilder.build()
    } else {
      val transforms = Array[Transform](identity(reference(partitionCols)))
      baseBuilder.withPartitions(transforms).build()
    }
    catalog.createTable(ident, tableInfo)
  }
}

/**
 * SQL-based implementation of merge schema evolution tests.
 * Executes merge operations using SQL statements.
 */
trait MergeIntoSchemaEvolutionSQLSuiteBase extends MergeIntoSchemaEvolutionSuiteBase {

  override protected def executeMerge(
      withSchemaEvolution: Boolean,
      targetTableName: String,
      sourceViewName: String,
      cond: String,
      clauses: Seq[MergeClause]): Unit = {
    val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
    val clausesSql = clauses.map(_.sql).mkString("\n")
    val mergeStmt =
      s"""MERGE $schemaEvolutionClause
         |INTO $targetTableName t
         |USING $sourceViewName s
         |ON $cond
         |$clausesSql
         |""".stripMargin
    sql(mergeStmt)
  }
}


/**
 * Scala/DataFrame API-based implementation of merge schema evolution tests.
 * Executes merge operations using the DataFrame merge builder API.
 */
trait MergeIntoSchemaEvolutionScalaSuiteBase extends MergeIntoSchemaEvolutionSuiteBase {

  override protected def executeMerge(
      withSchemaEvolution: Boolean,
      targetTableName: String,
      sourceTableName: String,
      cond: String,
      clauses: Seq[MergeClause]): Unit = {

    val sourceDf = spark.table(sourceTableName)

    val mergeBuilder = sourceDf.mergeInto(targetTableName,
      toExpr(cond, targetTableName, sourceTableName))

    // Apply each clause to the merge builder
    clauses.foreach {
      case MatchedClause(condition, action) =>
        val matched = if (condition != null) {
          mergeBuilder.whenMatched(toExpr(condition, targetTableName, sourceTableName))
        } else {
          mergeBuilder.whenMatched()
        }
        action match {
          case "UPDATE SET *" => matched.updateAll()
          case "DELETE" => matched.delete()
          case s if s.startsWith("UPDATE SET ") =>
            val assignments = parseAssignments(s.stripPrefix("UPDATE SET "),
              targetTableName, sourceTableName)
            matched.update(assignments)
          case _ => throw new IllegalArgumentException(s"Unknown matched action: $action")
        }

      case NotMatchedClause(condition, action) =>
        val notMatched = if (condition != null) {
          mergeBuilder.whenNotMatched(
            toExpr(condition, targetTableName, sourceTableName))
        } else {
          mergeBuilder.whenNotMatched()
        }
        action match {
          case "INSERT *" => notMatched.insertAll()
          case s if s.startsWith("INSERT ") =>
            val insertSpec = s.stripPrefix("INSERT ")
            val assignments = parseInsertAssignments(insertSpec, targetTableName, sourceTableName)
            notMatched.insert(assignments)
          case _ => throw new IllegalArgumentException(s"Unknown not matched action: $action")
        }

      case NotMatchedBySourceClause(condition, action) =>
        val notMatchedBySource = if (condition != null) {
          mergeBuilder.whenNotMatchedBySource(toExpr(condition, targetTableName, sourceTableName))
        } else {
          mergeBuilder.whenNotMatchedBySource()
        }
        action match {
          case "DELETE" => notMatchedBySource.delete()
          case s if s.startsWith("UPDATE SET ") =>
            val assignments = parseAssignments(s.stripPrefix("UPDATE SET "),
              targetTableName, sourceTableName)
            notMatchedBySource.update(assignments)
          case _ =>
            throw new IllegalArgumentException(s"Unknown not matched by source action: $action")
        }
    }

    if (withSchemaEvolution) {
      mergeBuilder.withSchemaEvolution().merge()
    } else {
      mergeBuilder.merge()
    }
  }

  /**
   * Parse UPDATE SET assignments like "col1 = expr1, col2 = expr2" into a Map[String, Column].
   */
  private def parseAssignments(
      setClause: String,
      targetTableName: String,
      sourceTableName: String): Map[String, ColumnV1] = {
    // Handle simple cases like "salary = s.salary" or "info.status = 'inactive'"
    // Split on comma, but be careful of nested expressions
    val parts = splitAssignments(setClause)
    parts.map { part =>
      val eqIdx = part.indexOf('=')
      if (eqIdx < 0) {
        throw new IllegalArgumentException(s"Invalid assignment: $part")
      }
      val col = part.substring(0, eqIdx).trim
      val value = part.substring(eqIdx + 1).trim
      col -> toExpr(value, targetTableName, sourceTableName)
    }.toMap
  }

  /**
   * Parse INSERT (cols) VALUES (exprs) into a Map[String, Column].
   */
  private def parseInsertAssignments(
      insertSpec: String,
      targetTableName: String,
      sourceTableName: String): Map[String, ColumnV1] = {
    // Format: "(col1, col2) VALUES (expr1, expr2)"
    val valuesIdx = insertSpec.toUpperCase(Locale.ROOT).indexOf("VALUES")
    if (valuesIdx < 0) {
      throw new IllegalArgumentException(s"Invalid INSERT spec: $insertSpec")
    }
    val colsPart = insertSpec.substring(0, valuesIdx).trim
    val valuesPart = insertSpec.substring(valuesIdx + 6).trim

    // Remove parentheses
    val cols = colsPart.stripPrefix("(").stripSuffix(")").split(",").map(_.trim)
    val values = splitValues(valuesPart.stripPrefix("(").stripSuffix(")"))

    if (cols.length != values.length) {
      throw new IllegalArgumentException(
        s"Column count ${cols.length} doesn't match value count ${values.length}")
    }

    cols.zip(values).map { case (col, value) =>
      col -> toExpr(value, targetTableName, sourceTableName)
    }.toMap
  }

  /**
   * Split comma-separated assignments, handling nested parentheses.
   */
  private def splitAssignments(str: String): Seq[String] = {
    val result = scala.collection.mutable.ArrayBuffer[String]()
    var depth = 0
    var current = new StringBuilder
    for (c <- str) {
      c match {
        case '(' | '[' => depth += 1; current.append(c)
        case ')' | ']' => depth -= 1; current.append(c)
        case ',' if depth == 0 =>
          result += current.toString.trim
          current = new StringBuilder
        case _ => current.append(c)
      }
    }
    if (current.nonEmpty) {
      result += current.toString.trim
    }
    result.toSeq
  }

  /**
   * Split comma-separated values, handling nested parentheses.
   */
  private def splitValues(str: String): Array[String] = {
    splitAssignments(str).toArray
  }

  /**
   * Helper to replace table aliases in string expressions.
   * t. -> targetTableName., s. -> sourceTableName.
   * Uses regex to only replace when preceded by start of string or non-word, non-dot char.
   */
  protected def replaceAliases(
      str: String,
      targetTableName: String,
      sourceTableName: String): String = {
    str.replaceAll("(?<![\\w.])t\\.", targetTableName + ".")
      .replaceAll("(?<![\\w.])s\\.", sourceTableName + ".")
  }

  /** Helper to convert string expression to Column with alias replacement. */
  protected def toExpr(
      str: String,
      targetTableName: String,
      sourceTableName: String): ColumnV1 = {
    expr(replaceAliases(str, targetTableName, sourceTableName))
  }
}


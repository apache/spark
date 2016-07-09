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

package org.apache.spark.sql.execution.systemcatalog

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.{DEFAULT_DATABASE, INFORMATION_SCHEMA_DATABASE}
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.CatalogImpl
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * INFORMATION_SCHEMA is a database consisting views which provide information about all of the
 * tables, views, columns in a database.
 *
 * These views are designed to be populated by this package in order to be independent from
 * Spark catalog. To keep minimal dependency, currently INFORMATION_SCHEMA views are implemented as
 * Spark temporary views with a database prefix: `SessionCatalog.INFORMATION_SCHEMA_DATABASE`.
 *
 * The following is the class hierarchy in this package rooted at InformationSchemaRelationProvider.
 *
 * InformationSchemaRelationProvider
 *  -> DatabasesRelationProvider
 *  -> TablesRelationProvider
 *  -> ViewsRelationProvider
 *  -> ColumnsRelationProvider
 *  -> SessionVariablesRelationProvider
 */

/**
 * InformationSchema object provides bootstrap and utility functions.
 */
object InformationSchema {

  /**
   * Register INFORMATION_SCHEMA database. SessionCatalog.catalog invokes this function.
   */
  def registerInformationSchema(sparkSession: SparkSession): Unit = {
    sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $INFORMATION_SCHEMA_DATABASE")
    registerView(sparkSession, new DatabasesRelationProvider, Seq("schemata", "databases"))
    registerView(sparkSession, new TablesRelationProvider, Seq("tables"))
    registerView(sparkSession, new ViewsRelationProvider, Seq("views"))
    registerView(sparkSession, new ColumnsRelationProvider, Seq("columns"))
    registerView(sparkSession, new SessionVariablesRelationProvider, Seq("session_variables"))
  }

  /**
   * Register an INFORMATION_SCHEMA relation provider as a temporary view of Spark Catalog.
   */
  private def registerView(
      sparkSession: SparkSession,
      relationProvider: RelationProvider,
      names: Seq[String]) {
    val plan =
      LogicalRelation(relationProvider.createRelation(sparkSession.sqlContext, parameters = null))
        .analyze
    val projectList = plan.output.zip(plan.schema).map {
      case (attr, col) => Alias(attr, col.name)()
    }
    sparkSession.sessionState.executePlan(Project(projectList, plan))

    for (name <- names) {
      sparkSession.sessionState.catalog.createTempView(s"$INFORMATION_SCHEMA_DATABASE.$name",
        plan, overrideIfExists = true)
    }
  }

  /**
   * Only EqualTo filters are handled in INFORMATION_SCHEMA data sources.
   */
  def unhandledFilters(filters: Array[Filter], columnName: String): Array[Filter] = {
    import org.apache.spark.sql.sources.EqualTo
    filters.filter {
      case EqualTo(attribute, _) if attribute.equalsIgnoreCase(columnName) => false
      case _ => true
    }
  }

  /**
   * Return `EqualTo` filtered DataFrame.
   */
  def getFilteredTables(sparkSession: SparkSession, filters: Seq[Expression], columnName: String)
      : DataFrame = {
    import org.apache.spark.sql.catalyst.expressions.EqualTo
    val database = filters.filter {
      case EqualTo(AttributeReference(name, _, _, _), Literal(_, StringType))
        if name.equalsIgnoreCase(columnName) => true
      case _ => false
    }.map(_.asInstanceOf[EqualTo].right.asInstanceOf[Literal].eval().toString()).headOption

    val tableList = if (database.nonEmpty) {
      sparkSession.catalog.listTables(database.get)
    } else {
      // Lookup the all table information from all databases.
      val allTables = new ArrayBuffer[org.apache.spark.sql.catalog.Table]
      val nonSystemDBs = sparkSession.catalog.listDatabases()
        .filter(_.name != INFORMATION_SCHEMA_DATABASE).collect()
      for (db <- nonSystemDBs)
        allTables ++= sparkSession.catalog.listTables(db.name).collect()
      CatalogImpl.makeDataset(allTables, sparkSession)
    }

    tableList.selectExpr("'default'", s"IFNULL(database, '$DEFAULT_DATABASE')", "name",
      "IF(tableType='VIEW', 'VIEW', IF(isTemporary, 'VIEW', 'TABLE'))",
      "IFNULL(description, 'VIEW')", "isTemporary"
    ).toDF("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "VIEW_DEFINITION",
      "isTemporary")
  }
}

/**
 * A base class for all INFORMATION_SCHEMA relation providers.
 */
private abstract class InformationSchemaRelationProvider extends RelationProvider {
  def relationSchema: StructType

  def makeRDD(
      sparkSession: SparkSession,
      requiredColumns: Seq[Attribute],
      filters: Seq[Expression]): RDD[Row]

  def providerUnhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val sparkSession = sqlContext.sparkSession

    new BaseRelation with CatalystScan {
      override def sqlContext: SQLContext = sparkSession.sqlContext

      override def schema: StructType = relationSchema

      override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] =
        makeRDD(sparkSession, requiredColumns, filters)

      override def unhandledFilters(filters: Array[Filter]): Array[Filter] =
        providerUnhandledFilters(filters)
    }
  }
}

/**
 * Provides DATABASES relation consisting all databases.
 * Note that filters are not supported.
 */
private class DatabasesRelationProvider extends InformationSchemaRelationProvider {
  def relationSchema: StructType = StructType(Seq(
    StructField("CATALOG_NAME", StringType, nullable = false),
    StructField("SCHEMA_NAME", StringType, nullable = false)
  ))

  def makeRDD(
      sparkSession: SparkSession,
      requiredColumns: Seq[Attribute],
      filters: Seq[Expression]): RDD[Row] = {
    sparkSession.catalog.listDatabases()
      .selectExpr(
        "'default' as CATALOG_NAME",
        s"regexp_replace(name, '$INFORMATION_SCHEMA_DATABASE.', '') as SCHEMA_NAME")
      .selectExpr(requiredColumns.map(_.name): _*).rdd
  }
}

/**
 * Provides TABLES relation containing all tables and views.
 * Note that only `TABLE_SCHEMA='database_name'` is supported for pushdown predicates.
 */
private class TablesRelationProvider extends InformationSchemaRelationProvider {
  override def relationSchema: StructType = StructType(Seq(
    StructField("TABLE_CATALOG", StringType, nullable = false),
    StructField("TABLE_SCHEMA", StringType, nullable = false),
    StructField("TABLE_NAME", StringType, nullable = false),
    StructField("TABLE_TYPE", StringType, nullable = false)
  ))

  def makeRDD(
      sparkSession: SparkSession,
      requiredColumns: Seq[Attribute],
      filters: Seq[Expression]): RDD[Row] = {
    val x = InformationSchema.getFilteredTables(sparkSession, filters, "TABLE_SCHEMA").collect()
    InformationSchema.getFilteredTables(sparkSession, filters, "TABLE_SCHEMA")
      .selectExpr(requiredColumns.map(_.name): _*).rdd
  }

  override def providerUnhandledFilters(filters: Array[Filter]): Array[Filter] =
    InformationSchema.unhandledFilters(filters, "TABLE_SCHEMA")
}

/**
 * Provides VIEWS relation containing all views.
 * Note that filters are not supported yet.
 * Note that only `TABLE_SCHEMA='database_name'` is supported for pushdown predicates.
 */
private class ViewsRelationProvider extends InformationSchemaRelationProvider {
  override def relationSchema: StructType = StructType(Seq(
    StructField("TABLE_CATALOG", StringType, nullable = false),
    StructField("TABLE_SCHEMA", StringType, nullable = false),
    StructField("TABLE_NAME", StringType, nullable = false),
    StructField("VIEW_DEFINITION", StringType, nullable = false)
  ))

  def makeRDD(
      sparkSession: SparkSession,
      requiredColumns: Seq[Attribute],
      filters: Seq[Expression]): RDD[Row] = {
    val x = InformationSchema.getFilteredTables(sparkSession, filters, "TABLE_SCHEMA").collect()
    InformationSchema.getFilteredTables(sparkSession, filters, "TABLE_SCHEMA")
      .toDF("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "VIEW_DEFINITION",
        "isTemporary")
      .where("isTemporary OR TABLE_TYPE = 'VIEW'")
      .selectExpr(requiredColumns.map(_.name): _*).rdd
  }

  override def providerUnhandledFilters(filters: Array[Filter]): Array[Filter] =
    InformationSchema.unhandledFilters(filters, "TABLE_SCHEMA")
}

/**
 * Provides COLUMNS relation containing all columns.
 * Note that only `TABLE_SCHEMA='database_name'` is supported for pushdown predicates.
 */
private class ColumnsRelationProvider extends InformationSchemaRelationProvider {
  override def relationSchema: StructType = StructType(Seq(
    StructField("TABLE_CATALOG", StringType, nullable = false),
    StructField("TABLE_SCHEMA", StringType, nullable = false),
    StructField("TABLE_NAME", StringType, nullable = false),
    StructField("COLUMN_NAME", StringType, nullable = false),
    StructField("ORDINAL_POSITION", LongType, nullable = false),
    StructField("IS_NULLABLE", BooleanType, nullable = false),
    StructField("DATA_TYPE", StringType, nullable = false)
  ))

  def makeRDD(
      sparkSession: SparkSession,
      requiredColumns: Seq[Attribute],
      filters: Seq[Expression]): RDD[Row] = {
    val tables =
      InformationSchema.getFilteredTables(sparkSession, filters, "TABLE_SCHEMA").collect()
    val result = new ArrayBuffer[Row]
    for (t <- tables) {
      val database = t(1).toString
      val name = t(2).toString
      val columnList = if (database == null) {
        sparkSession.catalog.listColumns(name)
      } else if (database == INFORMATION_SCHEMA_DATABASE) {
        sparkSession.catalog.listColumns(s"$database.$name")
      } else {
        sparkSession.catalog.listColumns(database, name)
      }
      result ++= columnList.rdd.zipWithIndex.map {
        case (col, index) =>
          Row("default", if (database != null) database else DEFAULT_DATABASE,
            name, col.name, index, col.nullable, col.dataType)
      }.collect()
    }
    sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(result), relationSchema)
      .orderBy("TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "ORDINAL_POSITION")
      .selectExpr(requiredColumns.map(_.name): _*).rdd
  }

  override def providerUnhandledFilters(filters: Array[Filter]): Array[Filter] =
    InformationSchema.unhandledFilters(filters, "TABLE_SCHEMA")
}

/**
 * Provides SESSION_VARIABLE relation containing all key-value pairs of session environment.
 * Note that filters are not supported.
 */
private class SessionVariablesRelationProvider extends InformationSchemaRelationProvider {
  override def relationSchema: StructType = StructType(Seq(
    StructField("VARIABLE_NAME", StringType, nullable = false),
    StructField("VARIABLE_VALUE", StringType, nullable = false)
  ))

  def makeRDD(
      sparkSession: SparkSession,
      requiredColumns: Seq[Attribute],
      filters: Seq[Expression]): RDD[Row] = {
    val runtimeConfig = sparkSession.conf.getAll.toArray
    val sqlConfig = sparkSession.sparkContext.conf.getAll
    val allConfig = runtimeConfig ++ sqlConfig
    val rdd = sparkSession.sparkContext.parallelize(allConfig).map { case (k, v) => Row(k, v) }
    sparkSession
      .createDataFrame(rdd, relationSchema)
      .distinct()
      .orderBy("VARIABLE_NAME")
      .selectExpr(requiredColumns.map(_.name): _*).rdd
  }
}

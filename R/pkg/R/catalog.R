#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# catalog.R: SparkSession catalog functions

#' Returns the current default catalog
#'
#' Returns the current default catalog.
#'
#' @return name of the current default catalog.
#' @rdname currentCatalog
#' @name currentCatalog
#' @examples
#' \dontrun{
#' sparkR.session()
#' currentCatalog()
#' }
#' @note since 3.4.0
currentCatalog <- function() {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  callJMethod(catalog, "currentCatalog")
}

#' Sets the current default catalog
#'
#' Sets the current default catalog.
#'
#' @param catalogName name of the catalog
#' @rdname setCurrentCatalog
#' @name setCurrentCatalog
#' @examples
#' \dontrun{
#' sparkR.session()
#' setCurrentCatalog("spark_catalog")
#' }
#' @note since 3.4.0
setCurrentCatalog <- function(catalogName) {
  sparkSession <- getSparkSession()
  if (class(catalogName) != "character") {
    stop("catalogName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
  invisible(handledCallJMethod(catalog, "setCurrentCatalog", catalogName))
}

#' Returns a list of catalog available
#'
#' Returns a list of catalog available.
#'
#' @return a SparkDataFrame of the list of catalog.
#' @rdname listCatalogs
#' @name listCatalogs
#' @examples
#' \dontrun{
#' sparkR.session()
#' listCatalogs()
#' }
#' @note since 3.4.0
listCatalogs <- function() {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  dataFrame(callJMethod(callJMethod(catalog, "listCatalogs"), "toDF"))
}

#' (Deprecated) Create an external table
#'
#' Creates an external table based on the dataset in a data source,
#' Returns a SparkDataFrame associated with the external table.
#'
#' The data source is specified by the \code{source} and a set of options(...).
#' If \code{source} is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used.
#'
#' @param tableName a name of the table.
#' @param path the path of files to load.
#' @param source the name of external data source.
#' @param schema the schema of the data required for some data sources.
#' @param ... additional argument(s) passed to the method.
#' @return A SparkDataFrame.
#' @rdname createExternalTable-deprecated
#' @seealso \link{createTable}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- createExternalTable("myjson", path="path/to/json", source="json", schema)
#' }
#' @name createExternalTable
#' @note createExternalTable since 1.4.0
createExternalTable <- function(tableName, path = NULL, source = NULL, schema = NULL, ...) {
  .Deprecated("createTable", old = "createExternalTable")
  createTable(tableName, path, source, schema, ...)
}

#' Creates a table based on the dataset in a data source
#'
#' Creates a table based on the dataset in a data source. Returns a SparkDataFrame associated with
#' the table.
#'
#' The data source is specified by the \code{source} and a set of options(...).
#' If \code{source} is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used. When a \code{path} is specified, an external table is
#' created from the data at the given path. Otherwise a managed table is created.
#'
#' @param tableName the qualified or unqualified name that designates a table. If no database
#'                  identifier is provided, it refers to a table in the current database.
#' @param path (optional) the path of files to load.
#' @param source (optional) the name of the data source.
#' @param schema (optional) the schema of the data required for some data sources.
#' @param ... additional named parameters as options for the data source.
#' @return A SparkDataFrame.
#' @rdname createTable
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- createTable("myjson", path="path/to/json", source="json", schema)
#'
#' createTable("people", source = "json", schema = schema)
#' insertInto(df, "people")
#' }
#' @name createTable
#' @note createTable since 2.2.0
createTable <- function(tableName, path = NULL, source = NULL, schema = NULL, ...) {
  sparkSession <- getSparkSession()
  options <- varargsToStrEnv(...)
  if (!is.null(path)) {
    options[["path"]] <- path
  }
  if (is.null(source)) {
    source <- getDefaultSqlSource()
  }
  catalog <- callJMethod(sparkSession, "catalog")
  if (is.null(schema)) {
    sdf <- callJMethod(catalog, "createTable", tableName, source, options)
  } else if (class(schema) == "structType") {
    sdf <- callJMethod(catalog, "createTable", tableName, source, schema$jobj, options)
  } else {
    stop("schema must be a structType.")
  }
  dataFrame(sdf)
}

#' Cache Table
#'
#' Caches the specified table in-memory.
#'
#' @param tableName the qualified or unqualified name that designates a table. If no database
#'                  identifier is provided, it refers to a table in the current database.
#' @return SparkDataFrame
#' @rdname cacheTable
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' createOrReplaceTempView(df, "table")
#' cacheTable("table")
#' }
#' @name cacheTable
#' @note cacheTable since 1.4.0
cacheTable <- function(tableName) {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  invisible(handledCallJMethod(catalog, "cacheTable", tableName))
}

#' Uncache Table
#'
#' Removes the specified table from the in-memory cache.
#'
#' @param tableName the qualified or unqualified name that designates a table. If no database
#'                  identifier is provided, it refers to a table in the current database.
#' @return SparkDataFrame
#' @rdname uncacheTable
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' createOrReplaceTempView(df, "table")
#' uncacheTable("table")
#' }
#' @name uncacheTable
#' @note uncacheTable since 1.4.0
uncacheTable <- function(tableName) {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  invisible(handledCallJMethod(catalog, "uncacheTable", tableName))
}

#' Clear Cache
#'
#' Removes all cached tables from the in-memory cache.
#'
#' @rdname clearCache
#' @examples
#' \dontrun{
#' clearCache()
#' }
#' @name clearCache
#' @note clearCache since 1.4.0
clearCache <- function() {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  invisible(callJMethod(catalog, "clearCache"))
}

#' (Deprecated) Drop Temporary Table
#'
#' Drops the temporary table with the given table name in the catalog.
#' If the table has been cached/persisted before, it's also unpersisted.
#'
#' @param tableName The name of the SparkSQL table to be dropped.
#' @seealso \link{dropTempView}
#' @rdname dropTempTable-deprecated
#' @examples
#' \dontrun{
#' sparkR.session()
#' df <- read.df(path, "parquet")
#' createOrReplaceTempView(df, "table")
#' dropTempTable("table")
#' }
#' @name dropTempTable
#' @note dropTempTable since 1.4.0
dropTempTable <- function(tableName) {
  .Deprecated("dropTempView", old = "dropTempTable")
  if (class(tableName) != "character") {
    stop("tableName must be a string.")
  }
  dropTempView(tableName)
}

#' Drops the temporary view with the given view name in the catalog.
#'
#' Drops the temporary view with the given view name in the catalog.
#' If the view has been cached before, then it will also be uncached.
#'
#' @param viewName the name of the temporary view to be dropped.
#' @return TRUE if the view is dropped successfully, FALSE otherwise.
#' @rdname dropTempView
#' @name dropTempView
#' @examples
#' \dontrun{
#' sparkR.session()
#' df <- read.df(path, "parquet")
#' createOrReplaceTempView(df, "table")
#' dropTempView("table")
#' }
#' @note since 2.0.0
dropTempView <- function(viewName) {
  sparkSession <- getSparkSession()
  if (class(viewName) != "character") {
    stop("viewName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
  callJMethod(catalog, "dropTempView", viewName)
}

#' Tables
#'
#' Returns a SparkDataFrame containing names of tables in the given database.
#'
#' @param databaseName (optional) name of the database
#' @return a SparkDataFrame
#' @rdname tables
#' @seealso \link{listTables}
#' @examples
#'\dontrun{
#' sparkR.session()
#' tables("hive")
#' }
#' @name tables
#' @note tables since 1.4.0
tables <- function(databaseName = NULL) {
  # rename column to match previous output schema
  withColumnRenamed(listTables(databaseName), "name", "tableName")
}

#' Table Names
#'
#' Returns the names of tables in the given database as an array.
#'
#' @param databaseName (optional) name of the database
#' @return a list of table names
#' @rdname tableNames
#' @examples
#'\dontrun{
#' sparkR.session()
#' tableNames("hive")
#' }
#' @name tableNames
#' @note tableNames since 1.4.0
tableNames <- function(databaseName = NULL) {
  sparkSession <- getSparkSession()
  callJStatic("org.apache.spark.sql.api.r.SQLUtils",
              "getTableNames",
              sparkSession,
              databaseName)
}

#' Returns the current default database
#'
#' Returns the current default database.
#'
#' @return name of the current default database.
#' @rdname currentDatabase
#' @name currentDatabase
#' @examples
#' \dontrun{
#' sparkR.session()
#' currentDatabase()
#' }
#' @note since 2.2.0
currentDatabase <- function() {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  callJMethod(catalog, "currentDatabase")
}

#' Sets the current default database
#'
#' Sets the current default database.
#'
#' @param databaseName name of the database
#' @rdname setCurrentDatabase
#' @name setCurrentDatabase
#' @examples
#' \dontrun{
#' sparkR.session()
#' setCurrentDatabase("default")
#' }
#' @note since 2.2.0
setCurrentDatabase <- function(databaseName) {
  sparkSession <- getSparkSession()
  if (class(databaseName) != "character") {
    stop("databaseName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
  invisible(handledCallJMethod(catalog, "setCurrentDatabase", databaseName))
}

#' Returns a list of databases available
#'
#' Returns a list of databases available.
#'
#' @return a SparkDataFrame of the list of databases.
#' @rdname listDatabases
#' @name listDatabases
#' @examples
#' \dontrun{
#' sparkR.session()
#' listDatabases()
#' }
#' @note since 2.2.0
listDatabases <- function() {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  dataFrame(callJMethod(callJMethod(catalog, "listDatabases"), "toDF"))
}

#' Returns a list of tables or views in the specified database
#'
#' Returns a list of tables or views in the specified database.
#' This includes all temporary views.
#'
#' @param databaseName (optional) name of the database
#' @return a SparkDataFrame of the list of tables.
#' @rdname listTables
#' @name listTables
#' @seealso \link{tables}
#' @examples
#' \dontrun{
#' sparkR.session()
#' listTables()
#' listTables("default")
#' }
#' @note since 2.2.0
listTables <- function(databaseName = NULL) {
  sparkSession <- getSparkSession()
  if (!is.null(databaseName) && class(databaseName) != "character") {
    stop("databaseName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
  jdst <- if (is.null(databaseName)) {
    callJMethod(catalog, "listTables")
  } else {
    handledCallJMethod(catalog, "listTables", databaseName)
  }
  dataFrame(callJMethod(jdst, "toDF"))
}

#' Returns a list of columns for the given table/view in the specified database
#'
#' Returns a list of columns for the given table/view in the specified database.
#'
#' @param tableName the qualified or unqualified name that designates a table/view. If no database
#'                  identifier is provided, it refers to a table/view in the current database.
#'                  If \code{databaseName} parameter is specified, this must be an unqualified name.
#' @param databaseName (optional) name of the database
#' @return a SparkDataFrame of the list of column descriptions.
#' @rdname listColumns
#' @name listColumns
#' @examples
#' \dontrun{
#' sparkR.session()
#' listColumns("mytable")
#' }
#' @note since 2.2.0
listColumns <- function(tableName, databaseName = NULL) {
  sparkSession <- getSparkSession()
  if (!is.null(databaseName) && class(databaseName) != "character") {
    stop("databaseName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
  jdst <- if (is.null(databaseName)) {
    handledCallJMethod(catalog, "listColumns", tableName)
  } else {
    handledCallJMethod(catalog, "listColumns", databaseName, tableName)
  }
  dataFrame(callJMethod(jdst, "toDF"))
}

#' Returns a list of functions registered in the specified database
#'
#' Returns a list of functions registered in the specified database.
#' This includes all temporary functions.
#'
#' @param databaseName (optional) name of the database
#' @return a SparkDataFrame of the list of function descriptions.
#' @rdname listFunctions
#' @name listFunctions
#' @examples
#' \dontrun{
#' sparkR.session()
#' listFunctions()
#' }
#' @note since 2.2.0
listFunctions <- function(databaseName = NULL) {
  sparkSession <- getSparkSession()
  if (!is.null(databaseName) && class(databaseName) != "character") {
    stop("databaseName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
  jdst <- if (is.null(databaseName)) {
    callJMethod(catalog, "listFunctions")
  } else {
    handledCallJMethod(catalog, "listFunctions", databaseName)
  }
  dataFrame(callJMethod(jdst, "toDF"))
}

#' Recovers all the partitions in the directory of a table and update the catalog
#'
#' Recovers all the partitions in the directory of a table and update the catalog. The name should
#' reference a partitioned table, and not a view.
#'
#' @param tableName the qualified or unqualified name that designates a table. If no database
#'                  identifier is provided, it refers to a table in the current database.
#' @rdname recoverPartitions
#' @name recoverPartitions
#' @examples
#' \dontrun{
#' sparkR.session()
#' recoverPartitions("myTable")
#' }
#' @note since 2.2.0
recoverPartitions <- function(tableName) {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  invisible(handledCallJMethod(catalog, "recoverPartitions", tableName))
}

#' Invalidates and refreshes all the cached data and metadata of the given table
#'
#' Invalidates and refreshes all the cached data and metadata of the given table. For performance
#' reasons, Spark SQL or the external data source library it uses might cache certain metadata about
#' a table, such as the location of blocks. When those change outside of Spark SQL, users should
#' call this function to invalidate the cache.
#'
#' If this table is cached as an InMemoryRelation, drop the original cached version and make the
#' new version cached lazily.
#'
#' @param tableName the qualified or unqualified name that designates a table. If no database
#'                  identifier is provided, it refers to a table in the current database.
#' @rdname refreshTable
#' @name refreshTable
#' @examples
#' \dontrun{
#' sparkR.session()
#' refreshTable("myTable")
#' }
#' @note since 2.2.0
refreshTable <- function(tableName) {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  invisible(handledCallJMethod(catalog, "refreshTable", tableName))
}

#' Invalidates and refreshes all the cached data and metadata for SparkDataFrame containing path
#'
#' Invalidates and refreshes all the cached data (and the associated metadata) for any
#' SparkDataFrame that contains the given data source path. Path matching is by prefix, i.e. "/"
#' would invalidate everything that is cached.
#'
#' @param path the path of the data source.
#' @rdname refreshByPath
#' @name refreshByPath
#' @examples
#' \dontrun{
#' sparkR.session()
#' refreshByPath("/path")
#' }
#' @note since 2.2.0
refreshByPath <- function(path) {
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
  invisible(handledCallJMethod(catalog, "refreshByPath", path))
}

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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, Driver, DriverManager, PreparedStatement, Statement}
import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Util functions for JDBC tables.
 */
object JdbcUtils extends Logging {

  // the property names are case sensitive
  val JDBC_BATCH_FETCH_SIZE = "fetchsize"
  val JDBC_BATCH_INSERT_SIZE = "batchsize"

  /**
   * Returns a factory for creating connections to the given JDBC URL.
   *
   * @param url the JDBC url to connect to.
   * @param properties JDBC connection properties.
   */
  def createConnectionFactory(url: String, properties: Properties): () => Connection = {
    val userSpecifiedDriverClass = Option(properties.getProperty("driver"))
    userSpecifiedDriverClass.foreach(DriverRegistry.register)
    // Performing this part of the logic on the driver guards against the corner-case where the
    // driver returned for a URL is different on the driver and executors due to classpath
    // differences.
    val driverClass: String = userSpecifiedDriverClass.getOrElse {
      DriverManager.getDriver(url).getClass.getCanonicalName
    }
    () => {
      userSpecifiedDriverClass.foreach(DriverRegistry.register)
      val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
        case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
        case d if d.getClass.getCanonicalName == driverClass => d
      }.getOrElse {
        throw new IllegalStateException(
          s"Did not find registered driver with class $driverClass")
      }
      driver.connect(url, properties)
    }
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(conn: Connection, url: String, table: String): Boolean = {
    val dialect = JdbcDialects.get(url)

    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems using JDBC meta data calls, considering "table" could also include
    // the database name. Query used to find table exists can be overridden by the dialects.
    Try {
      val statement = conn.prepareStatement(dialect.getTableExistsQuery(table))
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /**
   * Drops a table from the JDBC database.
   */
  def dropTable(conn: Connection, table: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.executeUpdate(s"DROP TABLE $table")
    } finally {
      statement.close()
    }
  }

  /**
   * Returns a PreparedStatement that inserts a row into table via conn.
   */
  def insertStatement(conn: Connection, table: String, rddSchema: StructType, dialect: JdbcDialect)
      : PreparedStatement = {
    val columns = rddSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders)"
    conn.prepareStatement(sql)
  }

  /**
   * Retrieve standard jdbc types.
   * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return The default JdbcType for this DataType
   */
  def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Option(
        JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  /**
   * Saves a partition of a DataFrame to the JDBC database.  This is done in
   * a single database transaction in order to avoid repeatedly inserting
   * data as much as possible.
   *
   * It is still theoretically possible for rows in a DataFrame to be
   * inserted into the database more than once if a stage somehow fails after
   * the commit occurs but before the stage can return successfully.
   *
   * This is not a closure inside saveTable() because apparently cosmetic
   * implementation changes elsewhere might easily render such a closure
   * non-Serializable.  Instead, we explicitly close over all variables that
   * are used.
   */
  def savePartition(
      getConnection: () => Connection,
      table: String,
      iterator: Iterator[Row],
      rddSchema: StructType,
      nullTypes: Array[Int],
      batchSize: Int,
      dialect: JdbcDialect): Iterator[Byte] = {
    require(batchSize >= 1,
      s"Invalid value `${batchSize.toString}` for parameter " +
      s"`${JdbcUtils.JDBC_BATCH_INSERT_SIZE}`. The minimum value is 1.")

    val conn = getConnection()
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
      conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        logWarning("Exception while detecting transaction support", e)
        true
    }

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      val stmt = insertStatement(conn, table, rddSchema, dialect)
      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              rddSchema.fields(i).dataType match {
                case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                case LongType => stmt.setLong(i + 1, row.getLong(i))
                case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                case ShortType => stmt.setInt(i + 1, row.getShort(i))
                case ByteType => stmt.setInt(i + 1, row.getByte(i))
                case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                case StringType => stmt.setString(i + 1, row.getString(i))
                case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                case ArrayType(et, _) =>
                  // remove type length parameters from end of type name
                  val typeName = getJdbcType(et, dialect).databaseTypeDefinition
                    .toLowerCase.split("\\(")(0)
                  val array = conn.createArrayOf(
                    typeName,
                    row.getSeq[AnyRef](i).toArray)
                  stmt.setArray(i + 1, array)
                case _ => throw new IllegalArgumentException(
                  s"Can't translate non-null value for field $i")
              }
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }

  /**
   * Compute the schema string for this RDD.
   */
  def schemaString(df: DataFrame, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    df.schema.fields foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ: String = getJdbcType(field.dataType, dialect).databaseTypeDefinition
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
   * Saves the RDD to the database in a single transaction.
   */
  def saveTable(
      df: DataFrame,
      url: String,
      table: String,
      properties: Properties) {
    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      getJdbcType(field.dataType, dialect).jdbcNullType
    }

    val rddSchema = df.schema
    val getConnection: () => Connection = createConnectionFactory(url, properties)
    val batchSize = properties.getProperty(JDBC_BATCH_INSERT_SIZE, "1000").toInt
    df.foreachPartition { iterator =>
      savePartition(getConnection, table, iterator, rddSchema, nullTypes, batchSize, dialect)
    }
  }

  /**
   * Check whether a table exists in a given database
   *
   * @return
   */
  @transient
  def checkTableExists(targetDb: String, tableName: String): Boolean = {
    val dbc: Connection = DriverManager.getConnection(targetDb)
    val dbm = dbc.getMetaData()
    // Check if the table exists. If it exists, perform an upsert.
    // Otherwise, do a simple dataframe write to the DB
    val tables = dbm.getTables(null, null, tableName, null)
    val exists = tables.next() // Returns false if next does not exist
    dbc.close()
    exists
  }

  // Provide a reasonable starting batch size for database operations.
  private val DEFAULT_BATCH_SIZE: Int = 200

  // Limit the number of database connections. Some DBs suffer when there are many open
  // connections.
  private val DEFAULT_MAX_CONNECTIONS: Int = 50

  private val DEFAULT_ID_COLUMN: String = "id"

  /**
   * Given an RDD of SQL queries to execute, connect to a database and perform a
   * sequence of batched operations.
   * Usies a maximum number of simultaneous connections as defined by "maxConnections".
   *
   * @param targetDb       The database to connect to, provided as a jdbc URL, e.g.
   *                  "jdbc:postgresql://192.168.0.1:5432/MY_DB_NAME?user=MY_USER&password=PASSWORD"
   * @param statements     An rdd of SQL statements to execute (as strings)
   * @param batchSize      The batch size to use, default 200
   * @param maxConnections Maximum number of simultaneous connections to open to the database
   */
  private def executeStatements(targetDb: String,
                                statements: Dataset[String],
                                batchSize: Int = DEFAULT_BATCH_SIZE,
                                maxConnections: Int = DEFAULT_MAX_CONNECTIONS): Unit = {
    // To avoid overloading database coalesce to a set number of partitions if necessary
    val coalesced = if (statements.rdd.getNumPartitions > maxConnections) {
      statements.coalesce(maxConnections)
    } else {
      statements
    }

    coalesced.mapPartitions(Iterator(_)).foreach { batch =>
      val dbc: Connection = DriverManager.getConnection(targetDb)
      dbc.setAutoCommit(false)

      val st: Statement = dbc.createStatement()

      try {
        batch.grouped(batchSize).foreach { rowBatch =>
          rowBatch.foreach { statement =>
            st.addBatch(statement)
          }
          st.executeBatch()
          dbc.commit()
        }
      }
      finally {
        dbc.close()
      }
    }
  }

  /**
   * Given a row of a database and specified key, generate an insert query
   *
   * @param row       A row of a dataframe
   * @param schema    The schema for the dataframe
   * @param tableName The name of the table to update
   * @return A SQL statement that inserts the referenced row into the table.
   */
  def genInsertScript(row: Row, schema: StructType, tableName: String): String = {
    val schemaString = schema.map(s => s.name).reduce(_ + ", " + _)

    val valString = {
      row.toSeq.map(v => "'" + v.toString.replaceAll("'", "''") + "'").reduce(_ + "," + _)
    }

    val insert = s"INSERT INTO $tableName ($schemaString) VALUES ($valString);"
    insert
  }

  /**
   * Given a row of a database and specified key, generate an update query
   *
   * @param row       A row of a dataframe
   * @param schema    The schema for the dataframe
   * @param tableName The name of the table to update
   * @param keyField  The name of the column that can serve as a primary key
   * @return A SQL statement that will update the referenced row in the table.
   */
  def genUpdateScript(row: Row, schema: StructType, tableName: String, keyField: String): String = {
    val keyVal = row.get(schema.fieldIndex(keyField))
    val zipped = row.toSeq.zip(schema.map(s => s.name))

    val valString = zipped.flatMap(s => {
      // Value fields are bounded with single quotes so escape any single quotes in the value
      val noQuotes: String = s._1.toString.replaceAll("'", "''")
      if (!s._2.equals(keyField)) Seq(s"${s._2}='$noQuotes'") else Seq()
    }).reduce(_ + ",\n" + _)

    val update = s"UPDATE $tableName set \n $valString \n WHERE $keyField = $keyVal;"
    update
  }

  /**
   * Write a given DataFrame into a provided database via JDBC by performing an update command.
   * If the database contains a row with a matching id, then all other columns will be updated.
   * If the database does not contain a row with a matching id an insert is performed instead.
   * Note: This command expects that the database contain an indexed column to use for the update,
   * default is "id".
   * TODO: Add support for arbitrary length keys
   *
   * @param df             The dataframe to write to the database.
   * @param targetDb       The database to update provided as a jdbc URL, e.g.
   *                  "jdbc:postgresql://192.168.0.1:5432/MY_DB_NAME?user=MY_USER&password=PASSWORD"
   * @param tableName      The name of the table to update
   * @param batchSize      The batch size to use, default 200
   * @param maxConnections Maximum number of simultaneous connections to open to the database
   * @param idColumn       The column to use as the primary key for resolving conflicts,
   *                       default is "id".
   */
  @transient
  def updateById(df: DataFrame,
                 targetDb: String,
                 tableName: String,
                 batchSize: Int = DEFAULT_BATCH_SIZE,
                 maxConnections: Int = DEFAULT_MAX_CONNECTIONS,
                 idColumn: String = DEFAULT_ID_COLUMN): Unit = {
    val schema = df.schema
    val tableExists = checkTableExists(targetDb, tableName)

    if (!tableExists) {
      throw new NotImplementedError("Adding data to a non-existing table is not yet supported.")
    }

    val statements = df.map(row => {
      genUpdateScript(row, schema, tableName, idColumn)
    })

    executeStatements(targetDb, statements, batchSize, maxConnections)
  }

  /**
   * Insert a given DataFrame into a provided database via JDBC.
   *
   * @param df        The dataframe to write to the database.
   * @param targetDb  The database to update provided as a jdbc URL, e.g.
   *                  "jdbc:postgresql://192.168.0.1:5432/MY_DB_NAME?user=MY_USER&password=PASSWORD"
   * @param tableName The name of the table to update
   * @param batchSize The batch size to use, default 200
   * @param maxConnections Maximum number of simultaneous connections to open to the database
   */
  @transient
  def insert(df: DataFrame,
             targetDb: String,
             tableName: String,
             batchSize: Int = 200,
             maxConnections: Int = DEFAULT_MAX_CONNECTIONS): Unit = {
    val schema = df.schema
    val tableExists = checkTableExists(targetDb, tableName)

    if (!tableExists) {
      throw new NotImplementedError("Adding data to a non-existing table is not yet supported.")
    }

    val statements = df.map(row => {
      genInsertScript(row, schema, tableName)
    })

    executeStatements(targetDb, statements, batchSize, maxConnections)
  }


  /**
   * Perform an upsert of a DataFrame to a given table.
   * Reads an existing table to determine whether any records need to be updated, performs an update
   * for any existing records, otherwise performs an insert.
   *
   * Because an update is an expensive operation, the most efficient way of performing an update on
   * a table is to first identify which records should be updated and which can be inserted. If
   * possible, an index on a single column should be used to perform the update, for example, an
   * "id" column. Performing an update on a multi-field index is even more computationally
   * expensive.
   *
   * For improved performance, remove any indices from the table being updated (except for the
   * index on the id column) and restore them after the update.
   *
   * @param sqlContext     The active SQL Context
   * @param df             The dataset to write to the database.
   * @param targetDb       The database to update provided as a jdbc URL, e.g.
   *                  "jdbc:postgresql://192.168.0.1:5432/MY_DB_NAME?user=MY_USER&password=PASSWORD"
   * @param properties     JDBC connection properties.
   * @param tableName      The name of the table to update
   * @param primaryKeys    A set representing the primary key for a database - the combination of
   *                       column names that uniquely identifies a row in the dataframe.
   *                       E.g. Set("first_name", "last_name", "address")
   * @param batchSize      The batch size to use, default 200
   * @param maxConnections Maximum number of simultaneous connections to open to the database
   * @param idColumn       The column to use as the key for resolving conflicts, default is "id"
   */
  def upsert(sqlContext: SQLContext,
             df: DataFrame,
             targetDb: String,
             properties: Properties,
             tableName: String,
             primaryKeys: Set[String],
             batchSize: Int = DEFAULT_BATCH_SIZE,
             maxConnections: Int = DEFAULT_MAX_CONNECTIONS,
             idColumn: String = "id"): Unit = {
    val storedDb = sqlContext.read.jdbc(targetDb, tableName, properties)

    // Determine rows to upsert based on a key match in the database
    val toUpsert = df.join(storedDb, primaryKeys.toSeq, "inner").select(df("*"), storedDb("id"))

    // Insert those rows where there is no matching entry in the database already
    // Do a select to ensure that columns are in the same order for except
    // Note: we need to also get rid of timestamps for this comparison
    val upsertKeys = toUpsert.select(primaryKeys.map(col).toSeq: _*)
    val primaryKeysToInsert = df.select(primaryKeys.map(col).toSeq: _*).except(upsertKeys)
    val toInsert = primaryKeysToInsert.join(df, primaryKeys.toSeq, "left_outer")

    // Only perform an update if there are overlapping elements
    if (toUpsert.count() > 0) {
      updateById(toUpsert, targetDb, tableName, batchSize, maxConnections, idColumn)
    }

    insert(toInsert, targetDb, tableName, batchSize, maxConnections)
  }
}

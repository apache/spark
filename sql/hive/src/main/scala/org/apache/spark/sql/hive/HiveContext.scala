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

package org.apache.spark.sql.hive

import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.sql.{Date, Timestamp}

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.io.{DateWritable, TimestampWritable}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateAnalysisOperators, OverrideCatalog, OverrideFunctionRegistry}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{ExecutedCommand, ExtractPythonUdfs, SetCommand, QueryExecutionException}
import org.apache.spark.sql.hive.execution.{HiveNativeCommand, DescribeHiveTableCommand}
import org.apache.spark.sql.sources.DataSourceStrategy
import org.apache.spark.sql.types._

/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HiveContext(sc: SparkContext) extends SQLContext(sc) {
  self =>

  protected[sql] override lazy val conf: SQLConf = new SQLConf {
    override def dialect: String = getConf(SQLConf.DIALECT, "hiveql")
  }

  /**
   * When true, enables an experimental feature where metastore tables that use the parquet SerDe
   * are automatically converted to use the Spark SQL parquet table scan, instead of the Hive
   * SerDe.
   */
  protected[sql] def convertMetastoreParquet: Boolean =
    getConf("spark.sql.hive.convertMetastoreParquet", "true") == "true"

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  override def sql(sqlText: String): SchemaRDD = {
    // TODO: Create a framework for registering parsers instead of just hardcoding if statements.
    if (conf.dialect == "sql") {
      super.sql(sqlText)
    } else if (conf.dialect == "hiveql") {
      new SchemaRDD(this, ddlParser(sqlText).getOrElse(HiveQl.parseSql(sqlText)))
    }  else {
      sys.error(s"Unsupported SQL dialect: ${conf.dialect}.  Try 'sql' or 'hiveql'")
    }
  }

  /**
   * Creates a table using the schema of the given class.
   *
   * @param tableName The name of the table to create.
   * @param allowExisting When false, an exception will be thrown if the table already exists.
   * @tparam A A case class that is used to describe the schema of the table to be created.
   */
  def createTable[A <: Product : TypeTag](tableName: String, allowExisting: Boolean = true) {
    catalog.createTable("default", tableName, ScalaReflection.attributesFor[A], allowExisting)
  }

  /**
   * Invalidate and refresh all the cached the metadata of the given table. For performance reasons,
   * Spark SQL or the external data source library it uses might cache certain metadata about a
   * table, such as the location of blocks. When those change outside of Spark SQL, users should
   * call this function to invalidate the cache.
   */
  def refreshTable(tableName: String): Unit = {
    // TODO: Database support...
    catalog.refreshTable("default", tableName)
  }

  protected[hive] def invalidateTable(tableName: String): Unit = {
    // TODO: Database support...
    catalog.invalidateTable("default", tableName)
  }

  /**
   * Analyzes the given table in the current database to generate statistics, which will be
   * used in query optimizations.
   *
   * Right now, it only supports Hive tables and it only updates the size of a Hive table
   * in the Hive metastore.
   */
  @Experimental
  def analyze(tableName: String) {
    val relation = EliminateAnalysisOperators(catalog.lookupRelation(Seq(tableName)))

    relation match {
      case relation: MetastoreRelation =>
        // This method is mainly based on
        // org.apache.hadoop.hive.ql.stats.StatsUtils.getFileSizeForTable(HiveConf, Table)
        // in Hive 0.13 (except that we do not use fs.getContentSummary).
        // TODO: Generalize statistics collection.
        // TODO: Why fs.getContentSummary returns wrong size on Jenkins?
        // Can we use fs.getContentSummary in future?
        // Seems fs.getContentSummary returns wrong table size on Jenkins. So we use
        // countFileSize to count the table size.
        def calculateTableSize(fs: FileSystem, path: Path): Long = {
          val fileStatus = fs.getFileStatus(path)
          val size = if (fileStatus.isDir) {
            fs.listStatus(path).map(status => calculateTableSize(fs, status.getPath)).sum
          } else {
            fileStatus.getLen
          }

          size
        }

        def getFileSizeForTable(conf: HiveConf, table: Table): Long = {
          val path = table.getPath
          var size: Long = 0L
          try {
            val fs = path.getFileSystem(conf)
            size = calculateTableSize(fs, path)
          } catch {
            case e: Exception =>
              logWarning(
                s"Failed to get the size of table ${table.getTableName} in the " +
                s"database ${table.getDbName} because of ${e.toString}", e)
              size = 0L
          }

          size
        }

        val tableParameters = relation.hiveQlTable.getParameters
        val oldTotalSize =
          Option(tableParameters.get(HiveShim.getStatsSetupConstTotalSize))
            .map(_.toLong)
            .getOrElse(0L)
        val newTotalSize = getFileSizeForTable(hiveconf, relation.hiveQlTable)
        // Update the Hive metastore if the total size of the table is different than the size
        // recorded in the Hive metastore.
        // This logic is based on org.apache.hadoop.hive.ql.exec.StatsTask.aggregateStats().
        if (newTotalSize > 0 && newTotalSize != oldTotalSize) {
          tableParameters.put(HiveShim.getStatsSetupConstTotalSize, newTotalSize.toString)
          val hiveTTable = relation.hiveQlTable.getTTable
          hiveTTable.setParameters(tableParameters)
          val tableFullName =
            relation.hiveQlTable.getDbName + "." + relation.hiveQlTable.getTableName

          catalog.client.alterTable(tableFullName, new Table(hiveTTable))
        }
      case otherRelation =>
        throw new NotImplementedError(
          s"Analyze has only implemented for Hive tables, " +
            s"but $tableName is a ${otherRelation.nodeName}")
    }
  }

  // Circular buffer to hold what hive prints to STDOUT and ERR.  Only printed when failures occur.
  @transient
  protected lazy val outputBuffer =  new java.io.OutputStream {
    var pos: Int = 0
    var buffer = new Array[Int](10240)
    def write(i: Int): Unit = {
      buffer(pos) = i
      pos = (pos + 1) % buffer.size
    }

    override def toString = {
      val (end, start) = buffer.splitAt(pos)
      val input = new java.io.InputStream {
        val iterator = (start ++ end).iterator

        def read(): Int = if (iterator.hasNext) iterator.next() else -1
      }
      val reader = new BufferedReader(new InputStreamReader(input))
      val stringBuilder = new StringBuilder
      var line = reader.readLine()
      while(line != null) {
        stringBuilder.append(line)
        stringBuilder.append("\n")
        line = reader.readLine()
      }
      stringBuilder.toString()
    }
  }

  /**
   * SQLConf and HiveConf contracts:
   *
   * 1. reuse existing started SessionState if any
   * 2. when the Hive session is first initialized, params in HiveConf will get picked up by the
   *    SQLConf.  Additionally, any properties set by set() or a SET command inside sql() will be
   *    set in the SQLConf *as well as* in the HiveConf.
   */
  @transient protected[hive] lazy val (hiveconf, sessionState) =
    Option(SessionState.get())
      .orElse {
        val newState = new SessionState(new HiveConf(classOf[SessionState]))
        // Only starts newly created `SessionState` instance.  Any existing `SessionState` instance
        // returned by `SessionState.get()` must be the most recently started one.
        SessionState.start(newState)
        Some(newState)
      }
      .map { state =>
        setConf(state.getConf.getAllProperties)
        if (state.out == null) state.out = new PrintStream(outputBuffer, true, "UTF-8")
        if (state.err == null) state.err = new PrintStream(outputBuffer, true, "UTF-8")
        (state.getConf, state)
      }
      .get

  override def setConf(key: String, value: String): Unit = {
    super.setConf(key, value)
    runSqlHive(s"SET $key=$value")
  }

  /* A catalyst metadata catalog that points to the Hive Metastore. */
  @transient
  override protected[sql] lazy val catalog = new HiveMetastoreCatalog(this) with OverrideCatalog

  // Note that HiveUDFs will be overridden by functions registered in this context.
  @transient
  override protected[sql] lazy val functionRegistry =
    new HiveFunctionRegistry with OverrideFunctionRegistry

  /* An analyzer that uses the Hive metastore. */
  @transient
  override protected[sql] lazy val analyzer =
    new Analyzer(catalog, functionRegistry, caseSensitive = false) {
      override val extendedRules =
        catalog.CreateTables ::
        catalog.PreInsertionCasts ::
        ExtractPythonUdfs ::
        Nil
    }

  /**
   * Runs the specified SQL query using Hive.
   */
  protected[sql] def runSqlHive(sql: String): Seq[String] = {
    val maxResults = 100000
    val results = runHive(sql, maxResults)
    // It is very confusing when you only get back some of the results...
    if (results.size == maxResults) sys.error("RESULTS POSSIBLY TRUNCATED")
    results
  }

  /**
   * Execute the command using Hive and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  protected def runHive(cmd: String, maxRows: Int = 1000): Seq[String] = synchronized {
    try {
      val cmd_trimmed: String = cmd.trim()
      val tokens: Array[String] = cmd_trimmed.split("\\s+")
      val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
      val proc: CommandProcessor = HiveShim.getCommandProcessor(Array(tokens(0)), hiveconf)

      // Makes sure the session represented by the `sessionState` field is activated. This implies
      // Spark SQL Hive support uses a single `SessionState` for all Hive operations and breaks
      // session isolation under multi-user scenarios (i.e. HiveThriftServer2).
      // TODO Fix session isolation
      if (SessionState.get() != sessionState) {
        SessionState.start(sessionState)
      }

      proc match {
        case driver: Driver =>
          val results = HiveShim.createDriverResultsArray
          val response: CommandProcessorResponse = driver.run(cmd)
          // Throw an exception if there is an error in query processing.
          if (response.getResponseCode != 0) {
            driver.close()
            throw new QueryExecutionException(response.getErrorMessage)
          }
          driver.setMaxRows(maxRows)
          driver.getResults(results)
          driver.close()
          HiveShim.processResults(results)
        case _ =>
          if (sessionState.out != null) {
            sessionState.out.println(tokens(0) + " " + cmd_1)
          }
          Seq(proc.run(cmd_1).getResponseCode.toString)
      }
    } catch {
      case e: Exception =>
        logError(
          s"""
            |======================
            |HIVE FAILURE OUTPUT
            |======================
            |${outputBuffer.toString}
            |======================
            |END HIVE FAILURE OUTPUT
            |======================
          """.stripMargin)
        throw e
    }
  }

  @transient
  private val hivePlanner = new SparkPlanner with HiveStrategies {
    val hiveContext = self

    override def strategies: Seq[Strategy] = experimental.extraStrategies ++ Seq(
      DataSourceStrategy,
      HiveCommandStrategy(self),
      HiveDDLStrategy,
      DDLStrategy,
      TakeOrdered,
      ParquetOperations,
      InMemoryScans,
      ParquetConversion, // Must be before HiveTableScans
      HiveTableScans,
      DataSinks,
      Scripts,
      HashAggregation,
      LeftSemiJoin,
      HashJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin
    )
  }

  @transient
  override protected[sql] val planner = hivePlanner

  /** Extends QueryExecution with hive specific features. */
  protected[sql] abstract class QueryExecution extends super.QueryExecution {

    /**
     * Returns the result as a hive compatible sequence of strings.  For native commands, the
     * execution is simply passed back to Hive.
     */
    def stringResult(): Seq[String] = executedPlan match {
      case ExecutedCommand(desc: DescribeHiveTableCommand) =>
        // If it is a describe command for a Hive table, we want to have the output format
        // be similar with Hive.
        desc.run(self).map {
          case Row(name: String, dataType: String, comment) =>
            Seq(name, dataType,
              Option(comment.asInstanceOf[String]).getOrElse(""))
              .map(s => String.format(s"%-20s", s))
              .mkString("\t")
        }
      case command: ExecutedCommand =>
        command.executeCollect().map(_(0).toString)

      case other =>
        val result: Seq[Seq[Any]] = other.executeCollect().map(_.toSeq).toSeq
        // We need the types so we can output struct field names
        val types = analyzed.output.map(_.dataType)
        // Reformat to match hive tab delimited output.
        result.map(_.zip(types).map(HiveContext.toHiveString)).map(_.mkString("\t")).toSeq
    }

    override def simpleString: String =
      logical match {
        case _: HiveNativeCommand => "<Native command: executed by Hive>"
        case _: SetCommand => "<SET command: executed by Hive, and noted by SQLContext>"
        case _ => super.simpleString
      }
  }
}


private object HiveContext {
  protected val primitiveTypes =
    Seq(StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, ByteType,
      ShortType, DateType, TimestampType, BinaryType)

  protected[sql] def toHiveString(a: (Any, DataType)): String = a match {
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
    case (map: Map[_,_], MapType(kType, vType, _)) =>
      map.map {
        case (key, value) =>
          toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
      }.toSeq.sorted.mkString("{", ",", "}")
    case (null, _) => "NULL"
    case (d: Date, DateType) => new DateWritable(d).toString
    case (t: Timestamp, TimestampType) => new TimestampWritable(t).toString
    case (bin: Array[Byte], BinaryType) => new String(bin, "UTF-8")
    case (decimal: java.math.BigDecimal, DecimalType()) =>
      // Hive strips trailing zeros so use its toString
      HiveShim.createDecimal(decimal).toString
    case (other, tpe) if primitiveTypes contains tpe => other.toString
  }

  /** Hive outputs fields of structs slightly differently than top level attributes. */
  protected def toHiveStructString(a: (Any, DataType)): String = a match {
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
    case (map: Map[_, _], MapType(kType, vType, _)) =>
      map.map {
        case (key, value) =>
          toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
      }.toSeq.sorted.mkString("{", ",", "}")
    case (null, _) => "null"
    case (s: String, StringType) => "\"" + s + "\""
    case (decimal, DecimalType()) => decimal.toString
    case (other, tpe) if primitiveTypes contains tpe => other.toString
  }
}

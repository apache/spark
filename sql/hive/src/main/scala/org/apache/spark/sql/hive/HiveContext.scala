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

import java.io.{BufferedReader, File, InputStreamReader, PrintStream}
import java.sql.Timestamp
import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeTag}

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.stats.StatsSetupConst
import org.apache.hadoop.hive.serde2.io.TimestampWritable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateAnalysisOperators}
import org.apache.spark.sql.catalyst.analysis.{OverrideCatalog, OverrideFunctionRegistry}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.ExtractPythonUdfs
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.{Command => PhysicalCommand}
import org.apache.spark.sql.hive.execution.DescribeHiveTableCommand

/**
 * DEPRECATED: Use HiveContext instead.
 */
@deprecated("""
  Use HiveContext instead.  It will still create a local metastore if one is not specified.
  However, note that the default directory is ./metastore_db, not ./metastore
  """, "1.1")
class LocalHiveContext(sc: SparkContext) extends HiveContext(sc) {

  lazy val metastorePath = new File("metastore").getCanonicalPath
  lazy val warehousePath: String = new File("warehouse").getCanonicalPath

  /** Sets up the system initially or after a RESET command */
  protected def configure() {
    setConf("javax.jdo.option.ConnectionURL",
      s"jdbc:derby:;databaseName=$metastorePath;create=true")
    setConf("hive.metastore.warehouse.dir", warehousePath)
  }

  configure() // Must be called before initializing the catalog below.
}

/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HiveContext(sc: SparkContext) extends SQLContext(sc) {
  self =>

  // Change the default SQL dialect to HiveQL
  override private[spark] def dialect: String = getConf(SQLConf.DIALECT, "hiveql")

  /**
   * When true, enables an experimental feature where metastore tables that use the parquet SerDe
   * are automatically converted to use the Spark SQL parquet table scan, instead of the Hive
   * SerDe.
   */
  private[spark] def convertMetastoreParquet: Boolean =
    getConf("spark.sql.hive.convertMetastoreParquet", "false") == "true"

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  override def sql(sqlText: String): SchemaRDD = {
    // TODO: Create a framework for registering parsers instead of just hardcoding if statements.
    if (dialect == "sql") {
      super.sql(sqlText)
    } else if (dialect == "hiveql") {
      new SchemaRDD(this, HiveQl.parseSql(sqlText))
    }  else {
      sys.error(s"Unsupported SQL dialect: $dialect.  Try 'sql' or 'hiveql'")
    }
  }

  @deprecated("hiveql() is deprecated as the sql function now parses using HiveQL by default. " +
             s"The SQL dialect for parsing can be set using ${SQLConf.DIALECT}", "1.1")
  def hiveql(hqlQuery: String): SchemaRDD = new SchemaRDD(this, HiveQl.parseSql(hqlQuery))

  @deprecated("hql() is deprecated as the sql function now parses using HiveQL by default. " +
             s"The SQL dialect for parsing can be set using ${SQLConf.DIALECT}", "1.1")
  def hql(hqlQuery: String): SchemaRDD = hiveql(hqlQuery)

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
   * Analyzes the given table in the current database to generate statistics, which will be
   * used in query optimizations.
   *
   * Right now, it only supports Hive tables and it only updates the size of a Hive table
   * in the Hive metastore.
   */
  def analyze(tableName: String) {
    val relation = EliminateAnalysisOperators(catalog.lookupRelation(None, tableName))

    relation match {
      case relation: MetastoreRelation => {
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
          val path = table.getPath()
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
          Option(tableParameters.get(StatsSetupConst.TOTAL_SIZE)).map(_.toLong).getOrElse(0L)
        val newTotalSize = getFileSizeForTable(hiveconf, relation.hiveQlTable)
        // Update the Hive metastore if the total size of the table is different than the size
        // recorded in the Hive metastore.
        // This logic is based on org.apache.hadoop.hive.ql.exec.StatsTask.aggregateStats().
        if (newTotalSize > 0 && newTotalSize != oldTotalSize) {
          tableParameters.put(StatsSetupConst.TOTAL_SIZE, newTotalSize.toString)
          val hiveTTable = relation.hiveQlTable.getTTable
          hiveTTable.setParameters(tableParameters)
          val tableFullName =
            relation.hiveQlTable.getDbName() + "." + relation.hiveQlTable.getTableName()

          catalog.client.alterTable(tableFullName, new Table(hiveTTable))
        }
      }
      case otherRelation =>
        throw new NotImplementedError(
          s"Analyze has only implemented for Hive tables, " +
            s"but ${tableName} is a ${otherRelation.nodeName}")
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
   * SQLConf and HiveConf contracts: when the hive session is first initialized, params in
   * HiveConf will get picked up by the SQLConf.  Additionally, any properties set by
   * set() or a SET command inside sql() will be set in the SQLConf *as well as*
   * in the HiveConf.
   */
  @transient protected[hive] lazy val hiveconf = new HiveConf(classOf[SessionState])
  @transient protected[hive] lazy val sessionState = {
    val ss = new SessionState(hiveconf)
    setConf(hiveconf.getAllProperties)  // Have SQLConf pick up the initial set of HiveConf.
    SessionState.start(ss)
    ss.err = new PrintStream(outputBuffer, true, "UTF-8")
    ss.out = new PrintStream(outputBuffer, true, "UTF-8")

    ss
  }

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
  protected def runHive(cmd: String, maxRows: Int = 1000): Seq[String] = {
    try {
      // Session state must be initilized before the CommandProcessor is created .
      SessionState.start(sessionState)

      val cmd_trimmed: String = cmd.trim()
      val tokens: Array[String] = cmd_trimmed.split("\\s+")
      val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
      val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hiveconf)

      proc match {
        case driver: Driver =>
          driver.init()

          val results = new JArrayList[String]
          val response: CommandProcessorResponse = driver.run(cmd)
          // Throw an exception if there is an error in query processing.
          if (response.getResponseCode != 0) {
            driver.destroy()
            throw new QueryExecutionException(response.getErrorMessage)
          }
          driver.setMaxRows(maxRows)
          driver.getResults(results)
          driver.destroy()
          results
        case _ =>
          sessionState.out.println(tokens(0) + " " + cmd_1)
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
  val hivePlanner = new SparkPlanner with HiveStrategies {
    val hiveContext = self

    override val strategies: Seq[Strategy] = Seq(
      CommandStrategy(self),
      HiveCommandStrategy(self),
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

    override lazy val toRdd: RDD[Row] = executedPlan.execute().map(_.copy())

    protected val primitiveTypes =
      Seq(StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, ByteType,
        ShortType, DecimalType, TimestampType, BinaryType)

    protected[sql] def toHiveString(a: (Any, DataType)): String = a match {
      case (struct: Row, StructType(fields)) =>
        struct.zip(fields).map {
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
      case (t: Timestamp, TimestampType) => new TimestampWritable(t).toString
      case (bin: Array[Byte], BinaryType) => new String(bin, "UTF-8")
      case (other, tpe) if primitiveTypes contains tpe => other.toString
    }

    /** Hive outputs fields of structs slightly differently than top level attributes. */
    protected def toHiveStructString(a: (Any, DataType)): String = a match {
      case (struct: Row, StructType(fields)) =>
        struct.zip(fields).map {
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
      case (other, tpe) if primitiveTypes contains tpe => other.toString
    }

    /**
     * Returns the result as a hive compatible sequence of strings.  For native commands, the
     * execution is simply passed back to Hive.
     */
    def stringResult(): Seq[String] = executedPlan match {
      case describeHiveTableCommand: DescribeHiveTableCommand =>
        // If it is a describe command for a Hive table, we want to have the output format
        // be similar with Hive.
        describeHiveTableCommand.hiveString
      case command: PhysicalCommand =>
        command.executeCollect().map(_.head.toString)

      case other =>
        val result: Seq[Seq[Any]] = toRdd.collect().toSeq
        // We need the types so we can output struct field names
        val types = analyzed.output.map(_.dataType)
        // Reformat to match hive tab delimited output.
        val asString = result.map(_.zip(types).map(toHiveString)).map(_.mkString("\t")).toSeq
        asString
    }

    override def simpleString: String =
      logical match {
        case _: NativeCommand => "<Native command: executed by Hive>"
        case _: SetCommand => "<SET command: executed by Hive, and noted by SQLContext>"
        case _ => super.simpleString
      }
  }
}

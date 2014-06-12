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

package org.apache.spark.sql
package hive

import java.io.{BufferedReader, File, InputStreamReader, PrintStream}
import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.{Analyzer, OverrideCatalog}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution._

/**
 * Starts up an instance of hive where metadata is stored locally. An in-process metadata data is
 * created with data stored in ./metadata.  Warehouse data is stored in in ./warehouse.
 */
class LocalHiveContext(sc: SparkContext) extends HiveContext(sc) {

  lazy val metastorePath = new File("metastore").getCanonicalPath
  lazy val warehousePath: String = new File("warehouse").getCanonicalPath

  /** Sets up the system initially or after a RESET command */
  protected def configure() {
    set("javax.jdo.option.ConnectionURL",
      s"jdbc:derby:;databaseName=$metastorePath;create=true")
    set("hive.metastore.warehouse.dir", warehousePath)
  }

  configure() // Must be called before initializing the catalog below.
}

/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HiveContext(sc: SparkContext) extends SQLContext(sc) {
  self =>

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  /**
   * Executes a query expressed in HiveQL using Spark, returning the result as a SchemaRDD.
   */
  def hiveql(hqlQuery: String): SchemaRDD = {
    val result = new SchemaRDD(this, HiveQl.parseSql(hqlQuery))
    // We force query optimization to happen right away instead of letting it happen lazily like
    // when using the query DSL.  This is so DDL commands behave as expected.  This is only
    // generates the RDD lineage for DML queries, but does not perform any execution.
    result.queryExecution.toRdd
    result
  }

  /** An alias for `hiveql`. */
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

  // Circular buffer to hold what hive prints to STDOUT and ERR.  Only printed when failures occur.
  @transient
  protected val outputBuffer =  new java.io.OutputStream {
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
   * set() or a SET command inside hql() or sql() will be set in the SQLConf *as well as*
   * in the HiveConf.
   */
  @transient protected[hive] lazy val hiveconf = new HiveConf(classOf[SessionState])
  @transient protected[hive] lazy val sessionState = {
    val ss = new SessionState(hiveconf)
    set(hiveconf.getAllProperties)  // Have SQLConf pick up the initial set of HiveConf.
    ss
  }

  sessionState.err = new PrintStream(outputBuffer, true, "UTF-8")
  sessionState.out = new PrintStream(outputBuffer, true, "UTF-8")

  override def set(key: String, value: String): Unit = {
    super.set(key, value)
    runSqlHive(s"SET $key=$value")
  }

  /* A catalyst metadata catalog that points to the Hive Metastore. */
  @transient
  override protected[sql] lazy val catalog = new HiveMetastoreCatalog(this) with OverrideCatalog {
    override def lookupRelation(
      databaseName: Option[String],
      tableName: String,
      alias: Option[String] = None): LogicalPlan = {

      LowerCaseSchema(super.lookupRelation(databaseName, tableName, alias))
    }
  }

  /* An analyzer that uses the Hive metastore. */
  @transient
  override protected[sql] lazy val analyzer =
    new Analyzer(catalog, HiveFunctionRegistry, caseSensitive = false)

  /**
   * Runs the specified SQL query using Hive.
   */
  protected def runSqlHive(sql: String): Seq[String] = {
    val maxResults = 100000
    val results = runHive(sql, 100000)
    // It is very confusing when you only get back some of the results...
    if (results.size == maxResults) sys.error("RESULTS POSSIBLY TRUNCATED")
    results
  }

  SessionState.start(sessionState)

  /**
   * Execute the command using Hive and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  protected def runHive(cmd: String, maxRows: Int = 1000): Seq[String] = {
    try {
      val cmd_trimmed: String = cmd.trim()
      val tokens: Array[String] = cmd_trimmed.split("\\s+")
      val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
      val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hiveconf)

      SessionState.start(sessionState)

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
        logger.error(
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
      TakeOrdered,
      ParquetOperations,
      HiveTableScans,
      DataSinks,
      Scripts,
      PartialAggregation,
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
    // TODO: Create mixin for the analyzer instead of overriding things here.
    override lazy val optimizedPlan =
      optimizer(catalog.PreInsertionCasts(catalog.CreateTables(analyzed)))

    override lazy val toRdd: RDD[Row] = {
      def processCmd(cmd: String): RDD[Row] = {
        val output = runSqlHive(cmd)
        if (output.size == 0) {
          emptyResult
        } else {
          val asRows = output.map(r => new GenericRow(r.split("\t").asInstanceOf[Array[Any]]))
          sparkContext.parallelize(asRows, 1)
        }
      }

      logical match {
        case s: SetCommand => eagerlyProcess(s)
        case _ => analyzed match {
          case NativeCommand(cmd) => processCmd(cmd)
          case _ => executedPlan.execute().map(_.copy())
        }
      }
    }

    protected val primitiveTypes =
      Seq(StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, ByteType,
        ShortType, DecimalType)

    protected def toHiveString(a: (Any, DataType)): String = a match {
      case (struct: Row, StructType(fields)) =>
        struct.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ))=>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_,_], MapType(kType, vType)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "NULL"
      case (other, tpe) if primitiveTypes contains tpe => other.toString
    }

    /** Hive outputs fields of structs slightly differently than top level attributes. */
    protected def toHiveStructString(a: (Any, DataType)): String = a match {
      case (struct: Row, StructType(fields)) =>
        struct.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ))=>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_,_], MapType(kType, vType)) =>
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
    def stringResult(): Seq[String] = analyzed match {
      case NativeCommand(cmd) => runSqlHive(cmd)
      case ExplainCommand(plan) => executePlan(plan).toString.split("\n")
      case query =>
        val result: Seq[Seq[Any]] = toRdd.collect().toSeq
        // We need the types so we can output struct field names
        val types = analyzed.output.map(_.dataType)
        // Reformat to match hive tab delimited output.
        val asString = result.map(_.zip(types).map(toHiveString)).map(_.mkString("\t")).toSeq
        asString
    }

    override def simpleString: String =
      logical match {
        case _: NativeCommand => "<Executed by Hive>"
        case _: SetCommand => "<Set Command: Executed by Hive, and noted by SQLContext>"
        case _ => executedPlan.toString
      }
  }
}

package org.apache.spark.sql
package shark

import java.io.{PrintStream, InputStreamReader, BufferedReader, File}
import java.util.{ArrayList => JArrayList}
import scala.language.implicitConversions

import org.apache.spark.SparkContext
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.processors.{CommandProcessorResponse, CommandProcessorFactory}
import org.apache.hadoop.hive.ql.processors.CommandProcessor
import org.apache.hadoop.hive.ql.Driver
import org.apache.spark.rdd.RDD

import catalyst.analysis.{Analyzer, OverrideCatalog}
import catalyst.expressions.GenericRow
import catalyst.plans.logical.{BaseRelation, LogicalPlan, NativeCommand, ExplainCommand}
import catalyst.types._

import org.apache.spark.sql.execution._

import scala.collection.JavaConversions._

/**
 * Starts up an instance of shark where metadata is stored locally. An in-process metadata data is
 * created with data stored in ./metadata.  Warehouse data is stored in in ./warehouse.
 */
class LocalSharkContext(sc: SparkContext) extends SharkContext(sc) {
  override def warehousePath = new File("warehouse").getCanonicalPath
  override def metastorePath = new File("metastore").getCanonicalPath
}

/**
 * An instance of the shark execution engine. This class is responsible for taking queries
 * expressed either in SQL or as raw catalyst logical plans and optimizing them for execution
 * using Spark.  Additionally this class maintains the connection with the hive metadata store.
 */
abstract class SharkContext(sc: SparkContext) extends SparkSqlContext(sc) {
  self =>

  /** The path to the hive warehouse. */
  def warehousePath: String
  /** The path to the local metastore. */
  def metastorePath: String

  override def parseSql(sql: String): LogicalPlan = HiveQl.parseSql(sql)
  override def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  /** Sets up the system initially or after a RESET command */
  protected def configure() {
    // TODO: refactor this so we can work with other databases.
    runSqlHive(
      s"set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$metastorePath;create=true")
    runSqlHive("set hive.metastore.warehouse.dir=" + warehousePath)
  }

  // Circular buffer to hold what hive prints to STDOUT and ERR.  Only printed when failures occur.
  // TODO: Move, or just use a properly configured logger.
  val outputBuffer =  new java.io.OutputStream {
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

        def read(): Int = if (iterator.hasNext) iterator.next else -1
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

  @transient lazy val hiveconf = new HiveConf(classOf[SessionState])
  @transient val sessionState = new SessionState(hiveconf)

  sessionState.err = new PrintStream(outputBuffer, true, "UTF-8")
  sessionState.out = new PrintStream(outputBuffer, true, "UTF-8")

  configure() // Must be called before initializing the catalog below.

  /* A catalyst metadata catalog that points to the Shark/Hive Metastore. */
  override val catalog = new HiveMetastoreCatalog(this) with OverrideCatalog

  /* An analyzer that uses the Shark/Hive metastore. */
  override val analyzer = new Analyzer(catalog, HiveFunctionRegistry, caseSensitive = false)

  def tables: Seq[BaseRelation] = {
    // TODO: Move this functionallity to Catalog. Make client protected.
    val allTables = catalog.client.getAllTables("default")
    allTables.map(catalog.lookupRelation(None, _, None)).collect { case b: BaseRelation => b }
  }

  /**
   * Runs the specified SQL query using Hive.
   */
  def runSqlHive(sql: String): Seq[String] = {
    val maxResults = 100000
    val results = runHive(sql, 100000)
    // It is very confusing when you only get back some of the results...
    if (results.size == maxResults) sys.error("RESULTS POSSIBLY TRUNCATED")
    results
  }

  // TODO: Move this.

  SessionState.start(sessionState)

  /**
   * Execute the command using Hive and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  def runHive(cmd: String, maxRows: Int = 1000): Seq[String] = {
    try {
      val cmd_trimmed: String = cmd.trim()
      val tokens: Array[String] = cmd_trimmed.split("\\s+")
      val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
      val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hiveconf)

      SessionState.start(sessionState)

      if (proc.isInstanceOf[Driver]) {
        val driver: Driver = proc.asInstanceOf[Driver]
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
      } else {
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

  object SharkPlanner extends SparkPlanner with SharkStrategies {
    val sharkContext = self

    override val strategies: Seq[Strategy] = Seq(
      PartitionPrunings,
      HiveTableScans,
      DataSinks,
      Scripts,
      PartialAggregation,
      SparkEquiInnerJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin
    )
  }

  override val planner = SharkPlanner

  protected lazy val emptyResult = sparkContext.parallelize(Seq(new GenericRow(Vector()): Row), 1)

  /** Extends QueryExecution with shark specific features. */
  abstract class QueryExecution extends super.QueryExecution {
    // TODO: Create mixin for the analyzer instead of overriding things here.
    override lazy val optimizedPlan =
      optimizer(catalog.PreInsertionCasts(catalog.CreateTables(analyzed)))

    // TODO: We are loosing schema here.
    override lazy val toRdd: RDD[Row] =
      analyzed match {
        case NativeCommand(cmd) =>
          val output = runSqlHive(cmd)

          if (output.size == 0) {
            emptyResult
          } else {
            val asRows = output.map(r => new GenericRow(r.split("\t")))
            sparkContext.parallelize(asRows, 1)
          }
        case _ =>
          executedPlan.execute()
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
      case ExplainCommand(plan) => new QueryExecution { val logical = plan }.toString.split("\n")
      case query =>
        val result: Seq[Seq[Any]] = toRdd.collect().toSeq
        // We need the types so we can output struct field names
        val types = analyzed.output.map(_.dataType)
        // Reformat to match hive tab delimited output.
        val asString = result.map(_.zip(types).map(toHiveString)).map(_.mkString("\t")).toSeq
        asString
    }
  }
}

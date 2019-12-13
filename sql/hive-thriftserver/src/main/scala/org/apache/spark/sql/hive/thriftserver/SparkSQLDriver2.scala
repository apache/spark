package org.apache.spark.sql.hive.thriftserver

import java.io.{BufferedReader, InputStreamReader}
import java.util.{Arrays, Locale}

import org.apache.commons.lang.StringUtils

import sys.process._
import scala.collection.JavaConverters._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.util.Utils
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.processors.{CommandProcessorFactory, CommandProcessorResponse}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}

import scala.util.{Failure, Success, Try}




private[hive] class SparkSQLDriver2(val context: SQLContext = SparkSQLEnv.sqlContext,
                                    val hadoopConf: Configuration) extends Logging {

  private[hive] var tableSchema: Schema = _
  private[hive] var hiveResponse: Seq[String] = _

  private def getResultSetSchema(query: QueryExecution): Schema = {
    val analyzed = query.analyzed
    logDebug(s"Result Schema: ${analyzed.output}")
    if (analyzed.output.isEmpty) {
      new Schema(Arrays.asList(new FieldSchema("Response code", "string", "")), null)
    } else {
      val fieldSchemas = analyzed.output.map { attr =>
        new FieldSchema(attr.name, attr.dataType.catalogString, "")
      }

      new Schema(fieldSchemas.asJava, null)
    }
  }

  def processCmd(cmd: String): Int = {
    val cmd_cleaned = cmd.trim.toLowerCase(Locale.ROOT)

    cmd_cleaned
      .split("\\s+")
      .toList match {
      case ("quit" | "exit") :: tail =>
        System.exit(0)
        0
      case "source" :: filepath :: tail =>
        processFile(filepath)
      case s :: tail if s startsWith "!" =>
        processShellCmd(cmd_cleaned.tail)
      case _ =>
        processSQLCmd(cmd_cleaned)
    }
  }

  def run(command: String): CommandProcessorResponse = {
    // TODO unify the error code
    try {
      context.sparkContext.setJobDescription(command)
      val execution = context.sessionState.executePlan(context.sql(command).logicalPlan)
      hiveResponse = SQLExecution.withNewExecutionId(context.sparkSession, execution) {
        hiveResultString(execution.executedPlan)
      }
      tableSchema = getResultSetSchema(execution)
      new CommandProcessorResponse(0)
    } catch {
      case ae: AnalysisException =>
        logDebug(s"Failed in [$command]", ae)
        new CommandProcessorResponse(1, ExceptionUtils.getStackTrace(ae), null, ae)
      case cause: Throwable =>
        logError(s"Failed in [$command]", cause)
        new CommandProcessorResponse(1, ExceptionUtils.getStackTrace(cause), null, cause)
    }
  }

  def processFile(file: String): Int = {
    val auxPath = new Path(file)
    val fs = if (auxPath.toUri.isAbsolute) {
      FileSystem.get(auxPath.toUri, hadoopConf)
    } else {
      FileSystem.getLocal(hadoopConf)
    }
    val path = if (!auxPath.toUri.isAbsolute) {
      fs.makeQualified(auxPath)
    } else {
      auxPath
    }

    lazy val br = new BufferedReader(new InputStreamReader(fs.open(path)))

    @scala.annotation.tailrec
    def readLines(reader: BufferedReader,
            outputString: List[String]): List[String] = {
      val line = reader.readLine()
      if (line == null) {
        outputString
      } else {
        readLines(reader, outputString :+ line)
      }
    }


    val resultLines = Try(readLines(br, List[String]()))
    IOUtils.closeStream(br)
    resultLines match {
      case Success(result) => processLines(result)
      case Failure(exception) =>
        logError(exception.getMessage)
        1
      case _ => 1
    }
  }

  def processSQLCmd(cmd: String): Int = {
    run(cmd)
    0
  }

  def processShellCmd(cmd: String): Int = {
    lazy val result = cmd.!!
    Try(result) match {
      case Success(value) =>
        println(value)
        0
      case Failure(exception) =>
        println(exception.getMessage)
        1
      case _ => 1
    }
  }

  def processLine(cmd: String): Int = processLines(List[String](cmd))

  def processLines(cmd: List[String]): Int = {
    val trimmed: String = cmd
      .filterNot(_ startsWith "--")
      .map(_.trim)
      .map(_.replace("\\\\"," "))
      .mkString
      .trim


    val replacementTag = "''"
    val regexPattern = """(["'])(.*?[^\\])\1""".r

    val replace = regexPattern
      .findAllIn(trimmed)
      .toList

    val allin = regexPattern
      .replaceAllIn(trimmed, replacementTag)
      .split(";").toList


    @scala.annotation.tailrec
    def pushBack(lines: List[String],
                 replacements: List[String],
                 accum: List[String]): List[String] = {

      if (lines.isEmpty) {
        accum
      } else {
        if (lines.head.contains(replacementTag) && replacements.nonEmpty) {
          val rep = lines.head.replace(replacementTag, replacements.head)
          pushBack(lines.tail, replacements.tail, accum :+ rep)
        } else {
          pushBack(lines.tail, replacements, accum :+ lines.head)
        }
      }
    }

    val cmds = pushBack(allin, replace, List[String]())
    @scala.annotation.tailrec
    def runCommands(cmd: List[String], prevResult: Int): Int = {
      if (cmd.isEmpty){
        prevResult
      } else if (prevResult == 1) {
        prevResult
      } else {
        runCommands(cmd.tail, processCmd(cmd.head))
      }
    }
    runCommands(cmds, 0)
  }
}

private[hive] object SparkSQLDriver2 {
  def apply(context: SQLContext,
            hadoopConf: Configuration): SparkSQLDriver2 = {
    new SparkSQLDriver2(context, hadoopConf)
  }
}

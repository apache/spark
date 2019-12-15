package org.apache.spark.sql.hive.thriftserver

import java.io.{BufferedReader, InputStreamReader}
import java.util.Locale

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.util.Utils

import scala.sys.process._
import scala.util.{Failure, Success, Try}

private[hive] case class SparkSQLDriver2(context: SQLContext,
                                         hadoopConf: Configuration)
    extends Logging {
  type RowResult = Seq[String]
  def processCmd(cmd: String): Int = {
    val cmd_cleaned = cmd.trim.toLowerCase(Locale.ROOT)
    cmd_cleaned
      .split("\\s+")
      .toList match {
      case ("quit" | "exit") :: _ =>
        System.exit(0)
        0
      case "source" :: filepath :: _ =>
        processFile(filepath)
      case s :: _ if s startsWith "!" =>
        processShellCmd(cmd_cleaned.tail)
      case _ =>
        processSQLCmd(cmd_cleaned)
    }
  }

  private def run(command: String): Option[Seq[RowResult]] = {
    Try {
      context.sparkContext.setJobDescription(command)
      val execution = context.sessionState.executePlan(context.sql(command).logicalPlan)
      val results = SQLExecution.withNewExecutionId(context.sparkSession, execution) {
        hiveResultString(execution.executedPlan).map(_.split("\t").toSeq)
      }

      val schemaValues = execution.analyzed.schema
      if (schemaValues.nonEmpty) {
        Some(schemaValues.map(attr => attr.name) +: results)
      } else {
        None
      }
    } match {
      case Success(value) => value
      case Failure(exception) =>
        println(exception)
        None
      case _ => None
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
    def readLines(reader: BufferedReader, outputString: List[String]): List[String] = {
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
    println(s">>>>>>> $cmd")
    val result = run(cmd)
    if (result.nonEmpty) {
      println(showQueryResults(result.get))
    }
    1
  }

  def processShellCmd(cmd: String): Int = {
    Try(cmd.!!) match {
      case Success(value) =>
        println(value)
        0
      case Failure(exception) =>
        logError(exception.getMessage)
        1
      case _ => 1
    }
  }

  def processLine(cmd: String): Int = processLines(List[String](cmd))

  def processLines(cmd: List[String]): Int = {
    val trimmed: String = cmd
      .filterNot(_ startsWith "--")
      .map(_.trim)
      .map(_.replace("\\\\", " "))
      .mkString
      .trim

    val replacementTag = "''"
    val regexPattern = """(["'])(.*?[^\\])\1""".r

    val replace = regexPattern
      .findAllIn(trimmed)
      .toList

    val allin = regexPattern
      .replaceAllIn(trimmed, replacementTag)
      .split(";")
      .toList
    @scala.annotation.tailrec
    def pushBack(
        lines: List[String],
        replacements: List[String],
        accumulator: List[String]): List[String] = {

      if (lines.isEmpty) {
        accumulator
      } else {
        if (lines.head.contains(replacementTag) && replacements.nonEmpty) {
          val rep = lines.head.replace(replacementTag, replacements.head)
          pushBack(lines.tail, replacements.tail, accumulator :+ rep)
        } else {
          pushBack(lines.tail, replacements, accumulator :+ lines.head)
        }
      }
    }

    val commands = pushBack(allin, replace, List[String]())
    @scala.annotation.tailrec
    def runCommands(cmd: List[String], prevResult: Int): Int = {
      if (cmd.isEmpty) {
        prevResult
      } else if (prevResult == 1) {
        prevResult
      } else {
        runCommands(cmd.tail, processCmd(cmd.head))
      }
    }
    runCommands(commands, 0)
  }

  def showQueryResults(rows: Seq[Seq[String]]): String = {


    val sb = new StringBuilder

    // If no headers/data is provided, an empty String will be returned.
    val numCols =
      if (rows.nonEmpty) {
        rows.head.length
      } else {
        0
      }

    // We set a minimum column width at '3'
    val minimumColWidth = 3
    val colWidths = Array.fill(numCols)(minimumColWidth)

    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), Utils.stringHalfWidth(cell))
      }
    }

    val paddedRows = rows.map { row =>
      row.zipWithIndex.map { case (cell, i) =>
        StringUtils.leftPad(cell, colWidths(i) - Utils.stringHalfWidth(cell) + cell.length)
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    paddedRows.head.addString(sb, "|", "|", "|\n")
    sb.append(sep)

    // data
    paddedRows.tail.foreach(_.addString(sb, "|", "|", "|\n"))
    sb.append(sep)

    sb.toString()
  }

}
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

package org.apache.spark.sql.api.python

import java.io.InputStream
import java.nio.channels.Channels

import net.razorvine.pickle.Pickler

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.PythonRDDServer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.{ExplainMode, QueryExecution}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

private[sql] object PythonSQLUtils extends Logging {
  private lazy val internalRowPickler = {
    EvaluatePython.registerPicklers()
    new Pickler(true, false)
  }

  def parseDataType(typeText: String): DataType = CatalystSqlParser.parseDataType(typeText)

  // This is needed when generating SQL documentation for built-in functions.
  def listBuiltinFunctionInfos(): Array[ExpressionInfo] = {
    FunctionRegistry.functionSet.flatMap(f => FunctionRegistry.builtin.lookupFunction(f)).toArray
  }

  private def listAllSQLConfigs(): Seq[(String, String, String, String)] = {
    val conf = new SQLConf()
    conf.getAllDefinedConfs
  }

  def listRuntimeSQLConfigs(): Array[(String, String, String, String)] = {
    // Py4J doesn't seem to translate Seq well, so we convert to an Array.
    listAllSQLConfigs().filterNot(p => SQLConf.isStaticConfigKey(p._1)).toArray
  }

  def listStaticSQLConfigs(): Array[(String, String, String, String)] = {
    listAllSQLConfigs().filter(p => SQLConf.isStaticConfigKey(p._1)).toArray
  }

  /**
   * Python callable function to read a file in Arrow stream format and create a [[RDD]]
   * using each serialized ArrowRecordBatch as a partition.
   */
  def readArrowStreamFromFile(session: SparkSession, filename: String): JavaRDD[Array[Byte]] = {
    ArrowConverters.readArrowStreamFromFile(session, filename)
  }

  /**
   * Python callable function to read a file in Arrow stream format and create a [[DataFrame]]
   * from an RDD.
   */
  def toDataFrame(
      arrowBatchRDD: JavaRDD[Array[Byte]],
      schemaString: String,
      session: SparkSession): DataFrame = {
    ArrowConverters.toDataFrame(arrowBatchRDD, schemaString, session)
  }

  def explainString(queryExecution: QueryExecution, mode: String): String = {
    queryExecution.explainString(ExplainMode.fromString(mode))
  }

  def toPyRow(row: Row): Array[Byte] = {
    assert(row.isInstanceOf[GenericRowWithSchema])
    internalRowPickler.dumps(EvaluatePython.toJava(
      CatalystTypeConverters.convertToCatalyst(row), row.schema))
  }

  def castTimestampNTZToLong(c: Column): Column = Column(CastTimestampNTZToLong(c.expr))

  def ewm(e: Column, alpha: Double, ignoreNA: Boolean): Column =
    Column(EWM(e.expr, alpha, ignoreNA))

  def lastNonNull(e: Column): Column = Column(LastNonNull(e.expr))

  def nullIndex(e: Column): Column = Column(NullIndex(e.expr))

  def binTimeStamp(
      base: Column,
      offset: Int,
      unit: String,
      leftClosed: Boolean,
      leftLabel: Boolean,
      ts: Column): Column = {
    assert(offset > 0)

    unit match {
      case "Y" =>
        val diff = year(ts) - year(base)
        val mod = if (offset == 1) lit(0) else pmod(diff, lit(offset))

        // let base=2018-12-31, offset=3, then edges are 2018-12-31, 2021-12-31,...
        val cond0 = mod === 0 && month(ts) === 12 && dayofmonth(ts) === 31

        // year label at edges like 2021-12-31
        val y0 = (leftClosed, leftLabel) match {
          case (true, true) => year(ts)
          case (true, false) => year(ts) + offset
          case (false, true) => year(ts) - offset
          case (false, false) => year(ts)
        }

        // year label at internal points like 2022-05-31
        val y1 = if (leftLabel) {
          year(ts) - when(mod === 0, lit(offset)).otherwise(mod)
        } else {
          year(ts) - when(mod === 0, lit(0)).otherwise(mod - offset)
        }

        to_timestamp(make_date(when(cond0, y0).otherwise(y1), lit(12), lit(31)))


      case "M" =>
        val diff = (year(ts) - year(base)) * 12 + month(ts) - month(base)
        val mod = if (offset == 1) lit(0) else pmod(diff, lit(offset))

        // let base=2018-12-31, offset=3, then edges are 2018-12-31, 2019-03-31,...
        val cond0 = mod === 0 && month(ts) === 12 && dayofmonth(ts) === 31

        def createMonthInterval(m: Column) = Column(
          MakeInterval(years = Literal(0), months = m.expr, weeks = Literal(0),
            days = Literal(0), hours = Literal(0), mins = Literal(0), secs = Literal(0))
        )

        // month label at edges like 2019-03-31
        val m0 = (leftClosed, leftLabel) match {
          case (true, true) => date_trunc("MONTH", ts)
          case (true, false) => date_trunc("MONTH", ts) + createMonthInterval(lit(offset))
          case (false, true) => date_trunc("MONTH", ts) - createMonthInterval(lit(offset))
          case (false, false) => date_trunc("MONTH", ts)
        }

        // month label at internal points like 2019-04-20
        val m1 = if (leftLabel) {
          date_trunc("MONTH", ts) -
            createMonthInterval(when(mod === 0, lit(offset)).otherwise(mod))
        } else {
          date_trunc("MONTH", ts) -
            createMonthInterval(when(mod === 0, lit(0)).otherwise(mod - offset))
        }

        to_timestamp(last_day(when(cond0, m0).otherwise(m1)))


      case "D" =>
        val diff = datediff(end = ts, start = base)
        val mod = if (offset == 1) lit(0) else pmod(diff, lit(offset))

        // let base=2018-12-31, offset=3, then edges are 2019-01-03, 2019-01-06,...
        val cond0 = mod === 0

        // day label at edges like 2019-01-03
        val d0 = (leftClosed, leftLabel) match {
          case (true, true) => ts
          case (true, false) => date_add(ts, lit(offset))
          case (false, true) => date_sub(ts, lit(offset))
          case (false, false) => ts
        }

        // day label at internal points like 2019-01-02
        val d1 = if (leftLabel) {
          date_sub(ts, when(mod === 0, lit(offset)).otherwise(mod))
        } else {
          date_sub(ts, when(mod === 0, lit(0)).otherwise(mod - offset))
        }

        date_trunc("DAY", when(cond0, d0).otherwise(d1))


      case "H" =>
        val diff = Column(TimestampDiff("HOUR",
          date_trunc("HOUR", base).expr, date_trunc("HOUR", ts).expr))
        val mod = if (offset == 1) lit(0) else pmod(diff, lit(offset))

        // let base=2018-12-31 00:00:00, offset=3
        // then edges are 2018-12-31 00:00:00, 2018-12-31 03:00:00,...
        val cond0 = mod === 0 && minute(ts) === 0 && second(ts) === 0

        def createHourInterval(h: Column) = Column(
          MakeInterval(years = Literal(0), months = Literal(0), weeks = Literal(0),
            days = Literal(0), hours = h.expr, mins = Literal(0), secs = Literal(0))
        )

        // hour label at edges like 2018-12-31 03:00:00
        val h0 = (leftClosed, leftLabel) match {
          case (true, true) => date_trunc("HOUR", ts)
          case (true, false) => date_trunc("HOUR", ts) + createHourInterval(lit(offset))
          case (false, true) => date_trunc("HOUR", ts) - createHourInterval(lit(offset))
          case (false, false) => date_trunc("HOUR", ts)
        }

        // hour label at internal points like 2018-12-31 04:02:13
        val h1 = if (leftLabel) {
          date_trunc("HOUR", ts) -
            createHourInterval(when(mod === 0, lit(offset)).otherwise(mod))
        } else {
          date_trunc("HOUR", ts) -
            createHourInterval(when(mod === 0, lit(0)).otherwise(mod - offset))
        }

        when(cond0, h0).otherwise(h1)


      case "T" =>
        val diff = Column(TimestampDiff("MINUTE",
          date_trunc("MINUTE", base).expr, date_trunc("MINUTE", ts).expr))
        val mod = if (offset == 1) lit(0) else pmod(diff, lit(offset))

        // let base=2018-12-31 00:00:00, offset=3
        // then edges are 2018-12-31 00:00:00, 2018-12-31 00:03:00,...
        val cond0 = mod === 0 && second(ts) === 0

        def createMinuteInterval(m: Column) = Column(
          MakeInterval(years = Literal(0), months = Literal(0), weeks = Literal(0),
            days = Literal(0), hours = Literal(0), mins = m.expr, secs = Literal(0))
        )

        // minute label at edges like 2018-12-31 00:03:00
        val m0 = (leftClosed, leftLabel) match {
          case (true, true) => date_trunc("MINUTE", ts)
          case (true, false) => date_trunc("MINUTE", ts) + createMinuteInterval(lit(offset))
          case (false, true) => date_trunc("MINUTE", ts) - createMinuteInterval(lit(offset))
          case (false, false) => date_trunc("MINUTE", ts)
        }

        // minute label at internal points like 2018-12-31 00:02:15
        val m1 = if (leftLabel) {
          date_trunc("MINUTE", ts) -
            createMinuteInterval(when(mod === 0, lit(offset)).otherwise(mod))
        } else {
          date_trunc("MINUTE", ts) -
            createMinuteInterval(when(mod === 0, lit(0)).otherwise(mod - offset))
        }

        when(cond0, m0).otherwise(m1)


      case "S" =>
        val diff = Column(TimestampDiff("SECOND",
          date_trunc("SECOND", base).expr, date_trunc("SECOND", ts).expr))
        val mod = if (offset == 1) lit(0) else pmod(diff, lit(offset))

        // let base=2018-12-31 00:00:00, offset=3
        // then edges are 2018-12-31 00:00:03, 2018-12-31 00:03:06,...
        val cond0 = mod === 0

        def createSecondInterval(s: Column) = Column(
          MakeInterval(years = Literal(0), months = Literal(0), weeks = Literal(0),
            days = Literal(0), hours = Literal(0), mins = Literal(0), secs = s.expr)
        )

        // second label at edges like 2018-12-31 00:00:03
        val s0 = (leftClosed, leftLabel) match {
          case (true, true) => date_trunc("SECOND", ts)
          case (true, false) => date_trunc("SECOND", ts) + createSecondInterval(lit(offset))
          case (false, true) => date_trunc("SECOND", ts) - createSecondInterval(lit(offset))
          case (false, false) => date_trunc("SECOND", ts)
        }

        // second label at internal points like 2018-12-31 00:00:11
        val m1 = if (leftLabel) {
          date_trunc("SECOND", ts) -
            createSecondInterval(when(mod === 0, lit(offset)).otherwise(mod))
        } else {
          date_trunc("SECOND", ts) -
            createSecondInterval(when(mod === 0, lit(0)).otherwise(mod - offset))
        }

        when(cond0, s0).otherwise(m1)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported offset alias: $unit")
    }
  }
}

/**
 * Helper for making a dataframe from arrow data from data sent from python over a socket.  This is
 * used when encryption is enabled, and we don't want to write data to a file.
 */
private[sql] class ArrowRDDServer(session: SparkSession) extends PythonRDDServer {

  override protected def streamToRDD(input: InputStream): RDD[Array[Byte]] = {
    // Create array to consume iterator so that we can safely close the inputStream
    val batches = ArrowConverters.getBatchesFromStream(Channels.newChannel(input)).toArray
    // Parallelize the record batches to create an RDD
    JavaRDD.fromRDD(session.sparkContext.parallelize(batches, batches.length))
  }

}

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

package org.apache.spark.sql.execution.command

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CONFIG, CONFIG2, KEY, VALUE}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.IgnoreCachedData
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.errors.QueryCompilationErrors.toSQLId
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Command that runs
 * {{{
 *   set key = value;
 *   set -v;
 *   set;
 * }}}
 */
case class SetCommand(kv: Option[(String, Option[String])])
  extends LeafRunnableCommand with Logging {

  private def keyValueOutput: Seq[Attribute] = {
    val schema = StructType(Array(
      StructField("key", StringType, nullable = false),
        StructField("value", StringType, nullable = false)))
    toAttributes(schema)
  }

  private val (_output, runFunc): (Seq[Attribute], SparkSession => Seq[Row]) = kv match {
    // Configures the deprecated "mapred.reduce.tasks" property.
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, Some(value))) =>
      val runFunc = (sparkSession: SparkSession) => {
        logWarning(
          log"Property ${MDC(CONFIG, SQLConf.Deprecated.MAPRED_REDUCE_TASKS)} is deprecated, " +
            log"automatically converted to ${MDC(CONFIG2, SQLConf.SHUFFLE_PARTITIONS.key)} " +
            log"instead.")
        if (value.toInt < 1) {
          val msg =
            s"Setting negative ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} for automatically " +
              "determining the number of reducers is not supported."
          throw new IllegalArgumentException(msg)
        } else {
          sparkSession.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, value)
          Seq(Row(SQLConf.SHUFFLE_PARTITIONS.key, value))
        }
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Replaced.MAPREDUCE_JOB_REDUCES, Some(value))) =>
      val runFunc = (sparkSession: SparkSession) => {
        logWarning(
          log"Property ${MDC(CONFIG, SQLConf.Replaced.MAPREDUCE_JOB_REDUCES)} is Hadoop's " +
            log"property, automatically converted to " +
            log"${MDC(CONFIG2, SQLConf.SHUFFLE_PARTITIONS.key)} instead.")
        if (value.toInt < 1) {
          val msg =
            s"Setting negative ${SQLConf.Replaced.MAPREDUCE_JOB_REDUCES} for automatically " +
              "determining the number of reducers is not supported."
          throw new IllegalArgumentException(msg)
        } else {
          sparkSession.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, value)
          Seq(Row(SQLConf.SHUFFLE_PARTITIONS.key, value))
        }
      }
      (keyValueOutput, runFunc)

    case Some((key @ SetCommand.VariableName(name), Some(value))) =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.conf.set(name, value)
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)

    // Configures a single property.
    case Some((key, Some(value))) =>
      val runFunc = (sparkSession: SparkSession) => {
        /**
         * Be nice and detect if the key matches a SQL variable.
         * If it does give a meaningful error pointing the user to SET VARIABLE
         */
        val varName = try {
          sparkSession.sessionState.sqlParser.parseMultipartIdentifier(key)
        } catch {
          case _: ParseException =>
          Seq()
        }
        if (varName.nonEmpty && varName.length <= 3) {
          if (sparkSession.sessionState.analyzer.lookupVariable(varName).isDefined) {
            throw new AnalysisException(
              errorClass = "UNSUPPORTED_FEATURE.SET_VARIABLE_USING_SET",
              messageParameters = Map("variableName" -> toSQLId(varName)))
          }
        }
        if (sparkSession.conf.get(CATALOG_IMPLEMENTATION.key).equals("hive") &&
            key.startsWith("hive.")) {
          logWarning(log"'SET ${MDC(KEY, key)}=${MDC(VALUE, value)}' might not work, since Spark " +
            log"doesn't support changing the Hive config dynamically. Please pass the " +
            log"Hive-specific config by adding the prefix spark.hadoop " +
            log"(e.g. spark.hadoop.${MDC(KEY, key)}) when starting a Spark application. For " +
            log"details, see the link: https://spark.apache.org/docs/latest/configuration.html#" +
            log"dynamically-loading-spark-properties.")
        }
        sparkSession.conf.set(key, value)
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)

    // (In Hive, "SET" returns all changed properties while "SET -v" returns all properties.)
    // Queries all key-value pairs that are set in the SQLConf of the sparkSession.
    case None =>
      val runFunc = (sparkSession: SparkSession) => {
        val redactedConf = SQLConf.get.redactOptions(sparkSession.conf.getAll)
        redactedConf.toSeq.sorted.map { case (k, v) => Row(k, v) }
      }
      (keyValueOutput, runFunc)

    // Queries all properties along with their default values and docs that are defined in the
    // SQLConf of the sparkSession.
    case Some(("-v", None)) =>
      val runFunc = (sparkSession: SparkSession) => {
        sparkSession.sessionState.conf.getAllDefinedConfs.sorted.map {
          case (key, defaultValue, doc, version) =>
            Row(
              key,
              Option(defaultValue).getOrElse("<undefined>"),
              doc,
              Option(version).getOrElse("<unknown>"))
        }
      }
      val schema = StructType(Array(
        StructField("key", StringType, nullable = false),
          StructField("value", StringType, nullable = false),
          StructField("meaning", StringType, nullable = false),
          StructField("Since version", StringType, nullable = false)))
      (toAttributes(schema), runFunc)

    // Queries the deprecated "mapred.reduce.tasks" property.
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, None)) =>
      val runFunc = (sparkSession: SparkSession) => {
        logWarning(
          log"Property ${MDC(CONFIG, SQLConf.Deprecated.MAPRED_REDUCE_TASKS)} is deprecated, " +
            log"showing ${MDC(CONFIG2, SQLConf.SHUFFLE_PARTITIONS.key)} instead.")
        Seq(Row(
          SQLConf.SHUFFLE_PARTITIONS.key,
          sparkSession.sessionState.conf.defaultNumShufflePartitions.toString))
      }
      (keyValueOutput, runFunc)

    // Queries a single property.
    case Some((key, None)) =>
      val runFunc = (sparkSession: SparkSession) => {
        val value = sparkSession.conf.getOption(key).getOrElse {
          // Also lookup the `sharedState.hadoopConf` to display default value for hadoop conf
          // correctly. It completes all the session-level configs with `sparkSession.conf`
          // together.
          //
          // Note that, as the write-side does not prohibit to set static hadoop/hive to SQLConf
          // yet, users may get wrong results before reaching here,
          // e.g. 'SET hive.metastore.uris=abc', where 'hive.metastore.uris' is static and 'abc' is
          // of no effect, but will show 'abc' via 'SET hive.metastore.uris' wrongly.
          //
          // Instead of showing incorrect `<undefined>` to users, it's more reasonable to show the
          // effective default values. For example, the hadoop output codec/compression configs
          // take affect from table to table, file to file, so they are not static and users are
          // very likely to change them based the default value they see.
          sparkSession.sharedState.hadoopConf.get(key, "<undefined>")
        }
        val (_, redactedValue) = SQLConf.get.redactOptions(Seq((key, value))).head
        Seq(Row(key, redactedValue))
      }
      (keyValueOutput, runFunc)
  }

  override val output: Seq[Attribute] = _output

  override def run(sparkSession: SparkSession): Seq[Row] = runFunc(sparkSession)

}

object SetCommand {
  val VariableName = """hivevar:([^=]+)""".r
}

/**
 * This command is for resetting SQLConf to the default values. Any configurations that were set
 * via [[SetCommand]] will get reset to default value. Command that runs
 * {{{
 *   reset;
 *   reset spark.sql.session.timeZone;
 * }}}
 */
case class ResetCommand(config: Option[String]) extends LeafRunnableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val globalInitialConfigs = sparkSession.sharedState.conf
    config match {
      case Some(key) =>
        sparkSession.conf.unset(key)
        sparkSession.initialSessionOptions.get(key)
          .orElse(globalInitialConfigs.getOption(key))
          .foreach(sparkSession.conf.set(key, _))
      case None =>
        sparkSession.sessionState.conf.clear()
        SQLConf.mergeSparkConf(sparkSession.sessionState.conf, globalInitialConfigs)
        SQLConf.mergeNonStaticSQLConfigs(sparkSession.sessionState.conf,
          sparkSession.initialSessionOptions)
    }
    Seq.empty[Row]
  }
}

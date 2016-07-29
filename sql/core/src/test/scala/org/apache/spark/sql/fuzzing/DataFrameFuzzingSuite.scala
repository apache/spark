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

package org.apache.spark.sql.fuzzing

import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

object DataFrameFuzzingUtils {

  def randomChoice[T](values: Seq[T]): T = {
    values(Random.nextInt(values.length))
  }

  /**
   * Build a list of column names and types for the given StructType, taking nesting into account.
   * For nested struct fields, this will emit both the column for the struct field itself as well as
   * fields for the nested struct's fields. This process will be performed recursively in order to
   * handle deeply-nested structs.
   */
  def getColumnsAndTypes(struct: StructType): Seq[(String, DataType)] = {
    struct.flatMap { field =>
      val nestedFieldInfos: Seq[(String, DataType)] = field.dataType match {
        case nestedStruct: StructType =>
          Seq((field.name, field.dataType)) ++ getColumnsAndTypes(nestedStruct).map {
            case (nestedColName, dataType) => (field.name + "." + nestedColName, dataType)
          }
        case _ => Seq.empty
      }
      Seq((field.name, field.dataType)) ++ nestedFieldInfos
    }
  }

  def getRandomColumnName(
      df: DataFrame,
      condition: DataType => Boolean = _ => true): Option[String] = {
    val columnsWithTypes = getColumnsAndTypes(df.schema)
    val candidateColumns = columnsWithTypes.filter(c => condition(c._2))
    if (candidateColumns.isEmpty) {
      None
    } else {
      Some(randomChoice(candidateColumns)._1)
    }
  }
}


/**
 * This test suite generates random data frames, then applies random sequences of operations to
 * them in order to construct random queries. We don't have a source of truth for these random
 * queries but nevertheless they are still useful for testing that we don't crash in bad ways.
 */
class DataFrameFuzzingSuite extends QueryTest with SharedSparkContext {


  override protected def spark: SparkSession = sqlContext.sparkSession

  val tempDir = Utils.createTempDir()

  private var sqlContext: SQLContext = _
  private var dataGenerator: RandomDataFrameGenerator = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
    dataGenerator = new RandomDataFrameGenerator(123, sqlContext)
    sqlContext.conf.setConf(SQLConf.SHUFFLE_PARTITIONS, 10)
  }

  def tryToExecute(df: DataFrame): DataFrame = {
    try {
      df.rdd.count()
      df
    } catch {
      case NonFatal(e) =>
        // scalastyle:off println
        println(df.queryExecution)
        // scalastyle:on println
        throw e
    }
  }

  // TODO: make these regexes.
  val ignoredAnalysisExceptionMessages = Seq(
    // TODO: filter only for binary type:
    "cannot sort data type array<",
    "cannot be used in grouping expression",
    "cannot be used in join condition",
    "can only be performed on tables with the same number of columns",
    "number of columns doesn't match",
    "unsupported join type",
    "is neither present in the group by, nor is it an aggregate function",
    "is ambiguous, could be:",
    "unresolved operator 'Project", // TODO
    "unresolved operator 'Union", // TODO: disabled to let me find new errors
    "unresolved operator 'Except", // TODO: disabled to let me find new errors
    "unresolved operator 'Intersect", // TODO: disabled to let me find new errors
    "Cannot resolve column name" // TODO: only ignore for join?
  )

  def getRandomTransformation(df: DataFrame): DataFrameTransformation = {
    (1 to 1000).iterator.map(_ => ReflectiveFuzzing.getTransformation(df)).flatten.next()
  }

  def applyRandomTransform(df: DataFrame): DataFrame = {
    val tf = getRandomTransformation(df)
    // scalastyle:off println
    println("    " + tf)
    // scalastyle:on println
    tf.apply(df)
  }

  def resetConfs(): Unit = {
    sqlContext.conf.getAllDefinedConfs.foreach { case (key, defaultValue, doc) =>
      sqlContext.conf.setConfString(key, defaultValue)
    }
    sqlContext.conf.setConfString("spark.sql.crossJoin.enabled", "true")
    sqlContext.conf.setConfString("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  private val configurations = Seq(
    "default" -> Seq(),
    "no optimization" -> Seq(SQLConf.OPTIMIZER_MAX_ITERATIONS.key -> "0"),
    "disable-wholestage-codegen" -> Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false"),
    "disable-exchange-reuse" -> Seq(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false")
  )

  def replan(df: DataFrame): DataFrame = {
    new Dataset[Row](sqlContext.sparkSession, df.logicalPlan, RowEncoder(df.schema))
  }

  test("fuzz test") {
    for (i <- 1 to 1000) {
      // scalastyle:off println
      println(s"Iteration $i")
      // scalastyle:on println
      try {
        resetConfs()
        var df = dataGenerator.randomDataFrame(
          numCols = Random.nextInt(2) + 1,
          numRows = 20,
          allowComplexTypes = false)
        var depth = 3
        while (depth > 0) {
          df = tryToExecute(applyRandomTransform(df))
          depth -= 1
        }
        val defaultResult = replan(df).collect()
        configurations.foreach { case (confName, confsToSet) =>
          resetConfs()
          withClue(s"configuration = $confName") {
            confsToSet.foreach { case (key, value) =>
              sqlContext.conf.setConfString(key, value)
            }
            checkAnswer(replan(df), defaultResult)
          }
        }
        println(s"Finished all tests successfully for plan:\n${df.logicalPlan}")
      } catch {
        case e: UnresolvedException[_] =>
//            println("skipped due to unresolved")
        case e: Exception
          if ignoredAnalysisExceptionMessages.exists {
            m => Option(e.getMessage).getOrElse("").toLowerCase.contains(m.toLowerCase)
          } =>
//            println("Skipped due to expected AnalysisException " + e)
      }
    }
  }
}

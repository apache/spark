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

import java.lang.reflect.InvocationTargetException
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.runtime.{universe => ru}
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class RandomDataFrameGenerator(seed: Long, sqlContext: SQLContext) {

  private val rand = new Random(seed)
  private val nextId = new AtomicInteger()

  private def hasRandomDataGenerator(dataType: DataType): Boolean = {
    RandomDataGenerator.forType(dataType).isDefined
  }

  def randomChoice[T](values: Seq[T]): T = {
    values(rand.nextInt(values.length))
  }

  private val simpleTypes: Set[DataType] = {
    DataTypeTestUtils.atomicTypes
      .filter(hasRandomDataGenerator)
      // Ignore decimal type since it can lead to OOM (see SPARK-9303). TODO: It would be better to
      // only generate limited precision decimals instead.
      .filterNot(_.isInstanceOf[DecimalType])
  }

  private val arrayTypes: Set[DataType] = {
    DataTypeTestUtils.atomicArrayTypes
      .filter(hasRandomDataGenerator)
      // See above comment about DecimalType
      .filterNot(_.elementType.isInstanceOf[DecimalType]).toSet
  }

  private def randomStructField(
      allowComplexTypes: Boolean = false,
      allowSpacesInColumnName: Boolean = false): StructField = {
    val name = "c" + nextId.getAndIncrement + (if (allowSpacesInColumnName) " space" else "")
    val candidateTypes: Seq[DataType] = Seq(
      simpleTypes,
      arrayTypes.filter(_ => allowComplexTypes),
      // This does not allow complex types, limiting the depth of recursion:
      if (allowComplexTypes) {
        Set[DataType](randomStructType(numCols = rand.nextInt(2) + 1))
      } else {
        Set[DataType]()
      }
    ).flatten
    val dataType = randomChoice(candidateTypes)
    val nullable = rand.nextBoolean()
    StructField(name, dataType, nullable)
  }

  private def randomStructType(
      numCols: Int,
      allowComplexTypes: Boolean = false,
      allowSpacesInColumnNames: Boolean = false): StructType = {
    StructType(Array.fill(numCols)(randomStructField(allowComplexTypes, allowSpacesInColumnNames)))
  }

  def randomDataFrame(
      numCols: Int,
      numRows: Int,
      allowComplexTypes: Boolean = false,
      allowSpacesInColumnNames: Boolean = false): DataFrame = {
    val schema = randomStructType(numCols, allowComplexTypes, allowSpacesInColumnNames)
    val rows = sqlContext.sparkContext.parallelize(1 to numRows).mapPartitions { iter =>
      val rowGenerator = RandomDataGenerator.forType(schema, nullable = false, seed = Some(42)).get
      iter.map(_ => rowGenerator().asInstanceOf[Row])
    }
    sqlContext.createDataFrame(rows, schema)
  }

}


/**
 * This test suite generates random data frames, then applies random sequences of operations to
 * them in order to construct random queries. We don't have a source of truth for these random
 * queries but nevertheless they are still useful for testing that we don't crash in bad ways.
 */
class DataFrameFuzzingSuite extends SparkFunSuite with SharedSparkContext {

  val tempDir = Utils.createTempDir()

  private var sqlContext: SQLContext = _
  private var dataGenerator: RandomDataFrameGenerator = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
    dataGenerator = new RandomDataFrameGenerator(123, sqlContext)
    sqlContext.conf.setConf(SQLConf.SHUFFLE_PARTITIONS, 10)
  }

  def randomChoice[T](values: Seq[T]): T = {
    values(Random.nextInt(values.length))
  }

  val m = ru.runtimeMirror(this.getClass.getClassLoader)

  val whitelistedParameterTypes = Set(
    m.universe.typeOf[DataFrame],
    m.universe.typeOf[Seq[Column]],
    m.universe.typeOf[Column],
    m.universe.typeOf[String],
    m.universe.typeOf[Seq[String]]
  )

  val dataFrameTransformations: Seq[ru.MethodSymbol] = {
    val dfType = m.universe.typeOf[DataFrame]
    dfType.members
      .filter(_.isPublic)
      .filter(_.isMethod)
      .map(_.asMethod)
      .filter(_.returnType =:= dfType)
      .filterNot(_.isConstructor)
      .filter { m =>
        m.paramss.flatten.forall { p =>
          whitelistedParameterTypes.exists { t => p.typeSignature <:< t }
        }
      }
      .filterNot(_.name.toString == "drop") // since this can lead to a DataFrame with no columns
      .filterNot(_.name.toString == "describe") // since we cannot run all queries on describe output
      .filterNot(_.name.toString == "dropDuplicates")
      .filter(_.name.toString == "join")
      .toSeq
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

  class NoDataGeneratorException extends Exception

  def getParamValues(
      df: DataFrame,
      method: ru.MethodSymbol,
      typeConstraint: DataType => Boolean = _ => true): Seq[Any] = {
    val params = method.paramss.flatten // We don't use multiple parameter lists
    def randColName(): String =
      getRandomColumnName(df, typeConstraint).getOrElse(throw new NoDataGeneratorException)
    params.map { p =>
      val t = p.typeSignature
      if (t =:= ru.typeOf[DataFrame]) {
        randomChoice(Seq(
          df,
          //tryToExecute(applyRandomTransformationToDataFrame(df)),
          dataGenerator.randomDataFrame(numCols = Random.nextInt(4) + 1, numRows = 100)
        )) // ++ Try(applyRandomTransformationToDataFrame(df)).toOption.toSeq)
      } else if (t =:= ru.typeOf[Column]) {
        df.col(randColName())
      } else if (t =:= ru.typeOf[String]) {
        if (p.name == "joinType") {
          randomChoice(JoinType.supportedJoinTypes)
        } else {
          randColName()
        }
      } else if (t <:< ru.typeOf[Seq[Column]]) {
        Seq.fill(Random.nextInt(2) + 1)(df.col(randColName()))
      } else if (t <:< ru.typeOf[Seq[String]]) {
        Seq.fill(Random.nextInt(2) + 1)(randColName())
      } else {
        sys.error("ERROR!")
      }
    }
  }

  def applyRandomTransformationToDataFrame(df: DataFrame): DataFrame = {
    val method = randomChoice(dataFrameTransformations)
    val reflectedMethod: ru.MethodMirror = m.reflect(df).reflectMethod(method)
    def callMethod(paramValues: Seq[Any]): DataFrame = {
      try {
        val df2 = reflectedMethod.apply(paramValues: _*).asInstanceOf[DataFrame]
        println(s"Applied method $method with values $paramValues")
        df2
      } catch {
        case e: InvocationTargetException =>
          throw e.getCause
      }
    }
    try {
      val paramValues = getParamValues(df, method)
      try {
        callMethod(paramValues)
      } catch {
        case NonFatal(e) =>
          println(s"Encountered error when calling $method with values $paramValues")
          println(df.queryExecution)
          throw e
      }
    } catch {
      case e: AnalysisException if e.getMessage.contains("is not a boolean") =>
        callMethod(getParamValues(df, method, _ == BooleanType))
      case e: AnalysisException if e.getMessage.contains("is not supported for columns of type") =>
        callMethod(getParamValues(df, method, _.isInstanceOf[AtomicType]))
    }
  }

  def tryToExecute(df: DataFrame): DataFrame = {
    try {
      df.rdd.count()
      df
    } catch {
      case NonFatal(e) =>
        println(df.queryExecution)
        throw new Exception(e)
    }
  }

  val ignoredAnalysisExceptionMessages = Seq(
    "can only be performed on tables with the same number of columns",
    "number of columns doesn't match",
    "unsupported join type",
    "is neither present in the group by, nor is it an aggregate function",
    "is ambiguous, could be:",
    "unresolved operator 'Project", //TODO
    "unresolved operator 'Union", // TODO: disabled to let me find new errors
    "unresolved operator 'Except", // TODO: disabled to let me find new errors
    "unresolved operator 'Intersect", // TODO: disabled to let me find new errors
    "Cannot resolve column name" // TODO: only ignore for join?
  )


  test("fuzz test") {
      for (_ <- 1 to 1000) {
        println("-" * 80)
        try {
          val df = dataGenerator.randomDataFrame(
            numCols = Random.nextInt(2) + 1,
            numRows = 20,
            allowComplexTypes = false)
          val df1 = tryToExecute(applyRandomTransformationToDataFrame(df))
          val df2 = tryToExecute(applyRandomTransformationToDataFrame(df1))
        } catch {
          case e: NoDataGeneratorException =>
            println("skipped due to lack of data generator")
          case e: UnresolvedException[_] =>
            println("skipped due to unresolved")
          case e: Exception
            if ignoredAnalysisExceptionMessages.exists {
              m => Option(e.getMessage).getOrElse("").toLowerCase.contains(m.toLowerCase)
            } => println("Skipped due to expected AnalysisException")
        }
      }
    }
}

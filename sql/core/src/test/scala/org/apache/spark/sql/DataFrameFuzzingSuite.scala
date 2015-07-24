package org.apache.spark.sql

import java.lang.reflect.InvocationTargetException

import org.apache.spark.sql.test.TestSQLContext

import scala.reflect.runtime.{universe => ru}

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

import scala.util.control.NonFatal

/**
 * This test suite generates random data frames, then applies random sequences of operations to
 * them in order to construct random queries. We don't have a source of truth for these random
 * queries but nevertheless they are still useful for testing that we don't crash in bad ways.
 */
class DataFrameFuzzingSuite extends SparkFunSuite {

  def randomChoice[T](values: Seq[T]): T = {
    values(Random.nextInt(values.length))
  }

  val randomValueGenerators: Map[Class[_], () => Any] = Map(
    classOf[String] -> (() => Random.nextString(10))
  )

  def generateRandomDataFrame(): DataFrame = {
    val allTypes = DataTypeTestUtils.atomicTypes
      .filterNot(_.isInstanceOf[DecimalType]) // casts can lead to OOM
      .filterNot(_.isInstanceOf[BinaryType]) // leads to spurious errors in string reverse
    val dataTypesWithGenerators = allTypes.filter { dt =>
      RandomDataGenerator.forType(dt, nullable = true, seed = None).isDefined
    }
    def randomType(): DataType = randomChoice(dataTypesWithGenerators.toSeq)
    val numColumns = 1 + Random.nextInt(3)
    val schema =
      new StructType((1 to numColumns).map(i => new StructField(s"c$i", randomType())).toArray)
    val rowGenerator = RandomDataGenerator.forType(schema).get
    val rows: Seq[Row] = Seq.fill(10)(rowGenerator().asInstanceOf[Row])
    TestSQLContext.createDataFrame(TestSQLContext.sparkContext.parallelize(rows), schema)
  }

  val df = generateRandomDataFrame()

  val m = ru.runtimeMirror(this.getClass.getClassLoader)

  val whitelistedColumnTypes = Set(
    m.universe.typeOf[DataFrame],
    m.universe.typeOf[Column]
  )

  val dataFrameTransformations = {
    val dfType = m.universe.typeOf[DataFrame]
    dfType.members
      .filter(_.isPublic)
      .filter(_.isMethod)
      .map(_.asMethod)
      .filter(_.returnType =:= dfType)
      .filterNot(_.isConstructor)
      .filter { m =>
        m.paramss.flatten.forall { p =>
          whitelistedColumnTypes.exists { t => t =:= p.typeSignature.erasure }
        }
      }
      .toSeq
  }

  def applyRandomTransformationToDataFrame(df: DataFrame): DataFrame = {
    val method = randomChoice(dataFrameTransformations)
    val params = method.paramss.flatten // We don't use multiple parameter lists
    val paramTypes = params.map(_.typeSignature)
    val paramValues = paramTypes.map { t =>
      if (m.universe.typeOf[DataFrame] =:= t.erasure) {
        df
      } else if (m.universe.typeOf[Column] =:= t.erasure) {
        df.col(randomChoice(df.columns))
      } else {
        sys.error("ERROR!")
      }
    }
    val reflectedMethod: ru.MethodMirror = m.reflect(df).reflectMethod(method)
    try {
      reflectedMethod.apply(paramValues: _*).asInstanceOf[DataFrame]
    } catch {
      case e: InvocationTargetException =>
        throw e.getCause
    }
  }

  for (_ <- 1 to 1000) {
    try {
      val df2 = applyRandomTransformationToDataFrame(df)
      try {
        df2.collectAsList()
      } catch {
        case NonFatal(e) =>
          println(df2.queryExecution)
          println(df)
          println(df.collectAsList())
          throw e
      }
    } catch {
      case e: AnalysisException => null
    }
  }

}

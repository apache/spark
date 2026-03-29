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

package org.apache.spark.ml.stat

import scala.annotation.varargs

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.function.Function
import org.apache.spark.ml.util._
import org.apache.spark.mllib.stat.{Statistics => OldStatistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col

/**
 * Conduct the two-sided Kolmogorov Smirnov (KS) test for data sampled from a
 * continuous distribution. By comparing the largest difference between the empirical cumulative
 * distribution of the sample data and the theoretical distribution we can provide a test for the
 * the null hypothesis that the sample data comes from that theoretical distribution.
 * For more information on KS Test:
 * @see <a href="https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test">
 * Kolmogorov-Smirnov test (Wikipedia)</a>
 */
@Since("2.4.0")
object KolmogorovSmirnovTest {

  /** Used to construct output schema of test */
  private case class KolmogorovSmirnovTestResult(
      pValue: Double,
      statistic: Double)

  private def getSampleRDD(dataset: DataFrame, sampleCol: String): RDD[Double] = {
    SchemaUtils.checkNumericType(dataset.schema, sampleCol)
    import dataset.sparkSession.implicits._
    dataset.select(col(sampleCol).cast("double")).as[Double].rdd
  }

  /**
   * Conduct the two-sided Kolmogorov-Smirnov (KS) test for data sampled from a
   * continuous distribution. By comparing the largest difference between the empirical cumulative
   * distribution of the sample data and the theoretical distribution we can provide a test for the
   * the null hypothesis that the sample data comes from that theoretical distribution.
   *
   * @param dataset A `Dataset` or a `DataFrame` containing the sample of data to test
   * @param sampleCol Name of sample column in dataset, of any numerical type
   * @param cdf a `Double => Double` function to calculate the theoretical CDF at a given value
   * @return DataFrame containing the test result for the input sampled data.
   *         This DataFrame will contain a single Row with the following fields:
   *          - `pValue: Double`
   *          - `statistic: Double`
   */
  @Since("2.4.0")
  def test(dataset: Dataset[_], sampleCol: String, cdf: Double => Double): DataFrame = {
    val spark = dataset.sparkSession

    val rdd = getSampleRDD(dataset.toDF(), sampleCol)
    val testResult = OldStatistics.kolmogorovSmirnovTest(rdd, cdf)
    spark.createDataFrame(Seq(KolmogorovSmirnovTestResult(
      testResult.pValue, testResult.statistic)))
  }

  /**
   * Java-friendly version of `test(dataset: DataFrame, sampleCol: String, cdf: Double => Double)`
   */
  @Since("2.4.0")
  def test(
      dataset: Dataset[_],
      sampleCol: String,
      cdf: Function[java.lang.Double, java.lang.Double]): DataFrame = {
    test(dataset, sampleCol, (x: Double) => cdf.call(x).toDouble)
  }

  /**
   * Convenience function to conduct a one-sample, two-sided Kolmogorov-Smirnov test for probability
   * distribution equality. Currently supports the normal distribution, taking as parameters
   * the mean and standard deviation.
   *
   * @param dataset A `Dataset` or a `DataFrame` containing the sample of data to test
   * @param sampleCol Name of sample column in dataset, of any numerical type
   * @param distName a `String` name for a theoretical distribution, currently only support "norm".
   * @param params `Double*` specifying the parameters to be used for the theoretical distribution.
    *              For "norm" distribution, the parameters includes mean and variance.
   * @return DataFrame containing the test result for the input sampled data.
   *         This DataFrame will contain a single Row with the following fields:
   *          - `pValue: Double`
   *          - `statistic: Double`
   */
  @Since("2.4.0")
  @varargs
  def test(
      dataset: Dataset[_],
      sampleCol: String, distName: String,
      params: Double*): DataFrame = {
    val spark = dataset.sparkSession

    val rdd = getSampleRDD(dataset.toDF(), sampleCol)
    val testResult = OldStatistics.kolmogorovSmirnovTest(rdd, distName, params: _*)
    spark.createDataFrame(Seq(KolmogorovSmirnovTestResult(
      testResult.pValue, testResult.statistic)))
  }
}

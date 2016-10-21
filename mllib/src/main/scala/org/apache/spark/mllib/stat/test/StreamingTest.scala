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

package org.apache.spark.mllib.stat.test

import scala.beans.BeanInfo

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.StatCounter

/**
 * Class that represents the group and value of a sample.
 *
 * @param isExperiment if the sample is of the experiment group.
 * @param value numeric value of the observation.
 */
@Since("1.6.0")
@BeanInfo
case class BinarySample @Since("1.6.0") (
    @Since("1.6.0") isExperiment: Boolean,
    @Since("1.6.0") value: Double) {
  override def toString: String = {
    s"($isExperiment, $value)"
  }
}

/**
 * Performs online 2-sample significance testing for a stream of (Boolean, Double) pairs. The
 * Boolean identifies which sample each observation comes from, and the Double is the numeric value
 * of the observation.
 *
 * To address novelty affects, the `peacePeriod` specifies a set number of initial
 * [[org.apache.spark.rdd.RDD]] batches of the [[DStream]] to be dropped from significance testing.
 *
 * The `windowSize` sets the number of batches each significance test is to be performed over. The
 * window is sliding with a stride length of 1 batch. Setting windowSize to 0 will perform
 * cumulative processing, using all batches seen so far.
 *
 * Different tests may be used for assessing statistical significance depending on assumptions
 * satisfied by data. For more details, see [[StreamingTestMethod]]. The `testMethod` specifies
 * which test will be used.
 *
 * Use a builder pattern to construct a streaming test in an application, for example:
 * {{{
 *   val model = new StreamingTest()
 *     .setPeacePeriod(10)
 *     .setWindowSize(0)
 *     .setTestMethod("welch")
 *     .registerStream(DStream)
 * }}}
 */
@Since("1.6.0")
class StreamingTest @Since("1.6.0") () extends Logging with Serializable {
  private var peacePeriod: Int = 0
  private var windowSize: Int = 0
  private var testMethod: StreamingTestMethod = WelchTTest

  /** Set the number of initial batches to ignore. Default: 0. */
  @Since("1.6.0")
  def setPeacePeriod(peacePeriod: Int): this.type = {
    this.peacePeriod = peacePeriod
    this
  }

  /**
   * Set the number of batches to compute significance tests over. Default: 0.
   * A value of 0 will use all batches seen so far.
   */
  @Since("1.6.0")
  def setWindowSize(windowSize: Int): this.type = {
    this.windowSize = windowSize
    this
  }

  /** Set the statistical method used for significance testing. Default: "welch" */
  @Since("1.6.0")
  def setTestMethod(method: String): this.type = {
    this.testMethod = StreamingTestMethod.getTestMethodFromName(method)
    this
  }

  /**
   * Register a [[DStream]] of values for significance testing.
   *
   * @param data stream of BinarySample(key,value) pairs where the key denotes group membership
   *             (true = experiment, false = control) and the value is the numerical metric to
   *             test for significance
   * @return stream of significance testing results
   */
  @Since("1.6.0")
  def registerStream(data: DStream[BinarySample]): DStream[StreamingTestResult] = {
    val dataAfterPeacePeriod = dropPeacePeriod(data)
    val summarizedData = summarizeByKeyAndWindow(dataAfterPeacePeriod)
    val pairedSummaries = pairSummaries(summarizedData)

    testMethod.doTest(pairedSummaries)
  }

  /**
   * Register a [[JavaDStream]] of values for significance testing.
   *
   * @param data stream of BinarySample(isExperiment,value) pairs where the isExperiment denotes
   *             group (true = experiment, false = control) and the value is the numerical metric
   *             to test for significance
   * @return stream of significance testing results
   */
  @Since("1.6.0")
  def registerStream(data: JavaDStream[BinarySample]): JavaDStream[StreamingTestResult] = {
    JavaDStream.fromDStream(registerStream(data.dstream))
  }

  /** Drop all batches inside the peace period. */
  private[stat] def dropPeacePeriod(
      data: DStream[BinarySample]): DStream[BinarySample] = {
    data.transform { (rdd, time) =>
      if (time.milliseconds > data.slideDuration.milliseconds * peacePeriod) {
        rdd
      } else {
        data.context.sparkContext.parallelize(Seq())
      }
    }
  }

  /** Compute summary statistics over each key and the specified test window size. */
  private[stat] def summarizeByKeyAndWindow(
      data: DStream[BinarySample]): DStream[(Boolean, StatCounter)] = {
    val categoryValuePair = data.map(sample => (sample.isExperiment, sample.value))
    if (this.windowSize == 0) {
      categoryValuePair.updateStateByKey[StatCounter](
        (newValues: Seq[Double], oldSummary: Option[StatCounter]) => {
          val newSummary = oldSummary.getOrElse(new StatCounter())
          newSummary.merge(newValues)
          Some(newSummary)
        })
    } else {
      val windowDuration = data.slideDuration * this.windowSize
      categoryValuePair
        .groupByKeyAndWindow(windowDuration)
        .mapValues { values =>
          val summary = new StatCounter()
          values.foreach(value => summary.merge(value))
          summary
        }
    }
  }

  /**
   * Transform a stream of summaries into pairs representing summary statistics for control group
   * and experiment group up to this batch.
   */
  private[stat] def pairSummaries(summarizedData: DStream[(Boolean, StatCounter)])
      : DStream[(StatCounter, StatCounter)] = {
    summarizedData
      .map[(Int, StatCounter)](x => (0, x._2))
      .groupByKey()  // should be length two (control/experiment group)
      .map(x => (x._2.head, x._2.last))
  }
}

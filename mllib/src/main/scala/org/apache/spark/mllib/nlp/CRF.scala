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

package org.apache.spark.mllib.nlp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.annotation.DeveloperApi

private[mllib] class CRF extends Serializable{
  private val freq: Integer = 1
  private val maxiter: Integer = 100000
  private val cost: Double = 1.0
  private val eta: Double = 0.0001
  private val C: Float = 1
  private val threadNum: Integer = Runtime.getRuntime.availableProcessors()
  private val threadPool: Array[CRFThread] = new Array[CRFThread](threadNum)
  private var featureIdx: FeatureIndex = new FeatureIndex()
  private var modelTxt: ArrayBuffer[String] = null

  /**
   * Internal method to verify the CRF model
   * @param test the same source in the CRFLearn
   * @param model the output from CRFLearn
   * @return the source with predictive labels
   */
  def verify(test: String,
             model: String): String = {
    var tagger: Tagger = new Tagger()
    featureIdx = featureIdx.openTagSet(test)
    tagger.open(featureIdx)
    tagger = tagger.read(test)
    featureIdx.openFromArray(model)
    tagger.parse()
    tagger.createOutput()
  }

  /**
   * Internal method to train the CRF model
   * @param template the template to train the model
   * @param train the source for the training
   * @return the model of the source
   */
  def learn(template: String,
            train: String): String = {
    var tagger: Tagger = new Tagger()
    var taggerList: ArrayBuffer[Tagger] = new ArrayBuffer[Tagger]()
    featureIdx.openTemplate(template)
    featureIdx = featureIdx.openTagSet(train)
    tagger.open(featureIdx)
    tagger = tagger.read(train)
    featureIdx.buildFeatures(tagger)
    taggerList += tagger
    tagger = null
    featureIdx.shrink(freq)
    featureIdx.initAlpha(featureIdx.maxid)
    runCRF(taggerList, featureIdx, featureIdx.alpha)
    val sc: SparkContext = CRF.getSparkContext
    modelTxt = featureIdx.saveModelTxt(sc)
    val model: String = featureIdx.saveModel()
    model
  }

  /**
   * Get model details in text format
   * @return the model and parameters in the training
   *         for debugging and trouble solving
   */
  def getModelTxt(): ArrayBuffer[String] = {
    modelTxt
  }

  /**
   * Parse segments in the unit sentences or paragraphs
   * @param tagger the tagger in the template
   * @param featureIndex the index of the feature
   * @param alpha the model
   */

  def runCRF(tagger: ArrayBuffer[Tagger], featureIndex: FeatureIndex,
             alpha: ArrayBuffer[Double]): Unit = {
    var diff: Double = 0.0
    var old_obj: Double = 1e37
    var converge: Int = 0
    var itr: Int = 0
    var all: Int = 0
    val opt = new Optimizer()
    var i: Int = 0
    var k: Int = 0

    while (i < tagger.length) {
      all += tagger(i).x.size
      i += 1
    }
    i = 0

    while (itr <= maxiter) {
      while (i < threadNum) {
        threadPool(i) = new CRFThread()
        threadPool(i).start_i = i
        threadPool(i).size = tagger.size
        threadPool(i).x = tagger
        threadPool(i).start()
        threadPool(i).join()
        if (i > 0) {
          threadPool(0).obj += threadPool(i).obj
          threadPool(0).err += threadPool(i).err
          threadPool(0).zeroOne += threadPool(i).zeroOne
        }
        while (k < featureIndex.maxid) {
          if (i > 0) {
            threadPool(0).expected(k) += threadPool(i).expected(k)
          }
          threadPool(0).obj += alpha(k) * alpha(k) / 2.0 * C
          threadPool(0).expected(k) += alpha(k) / C
          k += 1
        }
        k = 0
        i += 1
      }
      i = 0
      if (itr == 0) {
        diff = 1.0
      } else {
        diff = math.abs((old_obj - threadPool(0).obj) / old_obj)
      }
      old_obj = threadPool(0).obj
      printf("iter=%d, terr=%2.5f, serr=%2.5f, act=%d, obj=%2.5f,diff=%2.5f\n",
        itr, 1.0 * threadPool(0).err / all,
        1.0 * threadPool(0).zeroOne / tagger.size, featureIndex.maxid,
        threadPool(0).obj, diff)
      if (diff < eta) {
        converge += 1
      } else {
        converge = 0
      }
      if (converge == 3) {
        itr = maxiter + 1 // break
      }
      if (diff == 0) {
        itr = maxiter + 1 // break
      }
      opt.optimizer(featureIndex.maxid, alpha, threadPool(0).obj, threadPool(0).expected, C)
      itr += 1
    }
  }

  /**
   * Use multiple threads to parse the segments
   * in a unit sentence or paragraph.
   */
  class CRFThread extends Thread {
    var x: ArrayBuffer[Tagger] = null
    var start_i: Int = 0
    var err: Int = 0
    var zeroOne: Int = 0
    var size: Int = 0
    var obj: Double = 0.0
    var expected: ArrayBuffer[Double] = new ArrayBuffer[Double]()

    def initExpected(): Unit = {
      var i: Int = 0
      while (i < featureIdx.maxid) {
        expected.append(0.0)
        i += 1
      }
    }

    /**
     * Train CRF model and calculate the expectations
     */
    override def run(): Unit = {
      var idx: Int = 0
      initExpected()
      while (idx >= start_i && idx < size) {
        obj += x(idx).gradient(expected)
        err += x(idx).eval()
        if (err != 0) {
          zeroOne += 1
        }
        idx = idx + threadNum
      }
    }
  }

}


@DeveloperApi
object CRF {
  @transient var sc: SparkContext = _

  /**
   * Train CRF Model
   * A word's context is its sentence or its nearby
   * paragraph and the sentence or nearby paragraph will
   * not be very long. So A sentence or paragraph will be
   * processed in a node to reduce partition and networking
   * costs. Multiple sentences or paragraphs are
   * collected from multiple nodes to create the overall result.
   * A unit template is a string in the RDD.
   * A unit sentence or paragraph is a string in the RDD.
   *
   * Feature file format
   * word|word characteristic|designated label
   * Examples are in CRFTests.scala
   *
   * @param templates Source templates for training the model
   * @param features Source files for training the model
   * @return Model of a unit
   */
  def runCRF(templates: RDD[String],
             features: RDD[String]): RDD[String] = {
    val crf = new CRF()
    val template: Array[String] = templates.toLocalIterator.toArray
    sc = features.sparkContext
    val finalArray = features.flatMap { iter =>
      var i: Int = 0
      var str: String = ""
      val output: ArrayBuffer[String] = new ArrayBuffer[String]()
      while (i < template.length) {
        str = crf.learn(template(i), iter)
        val model: Array[String] = str.split("\n")
        output.appendAll(model)
        i += 1
      }
      output
    }.collect()
    sc.parallelize(finalArray)
  }

  /**
   * Verify CRF model.
   * The model is the result from CRF train. If the predicted
   * labels match the designated labels in the test file, the
   * model is valid.
   *
   * Test result format:
   * word|word characteristic|designated label|predicted label
   * Examples are in CRFTests.scala
   *
   * @param tests  Source files to be verified
   * @param models Model files after call the CRF learn
   * @return Source files with the predictive labels
   */
  def verifyCRF(tests: RDD[String],
                models: RDD[String]): RDD[String] = {
    val crf = new CRF()
    val test: Array[String] = tests.toLocalIterator.toArray
    val model: Array[String] = models.toLocalIterator.toArray
    sc = tests.sparkContext
    val finalArray = test.indices.map(idx => {
      var str: String = ""
      str = crf.verify(test(idx), model(idx))
      str
    })
    sc.parallelize(finalArray)
  }

  /**
   * Get CRF model detail in texts. It is for debugging.
   * @return The parameters and output in the training.
   */
  def getModelTxt(): CRFModel = {
    val crf = new CRF()
    val modelRDD = sc.parallelize(crf.getModelTxt())
    new CRFModel(modelRDD)
  }

  /**
   * Get spark context
   * @return the current spark context
   */
  def getSparkContext: SparkContext = {
    if (sc != null) {
      sc
    } else {
      null
    }
  }
}

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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

private[mllib] class CRF extends Serializable{
  private val freq: Int = 1
  private val maxiter: Int = 100000
  private val cost: Double = 1.0
  private val eta: Double = 0.0001
  private val C: Float = 1
  private val threadNum: Int = Runtime.getRuntime.availableProcessors()
  private val threadPool: Array[CRFThread] = new Array[CRFThread](threadNum)
  private var featureIdx: FeatureIndex = new FeatureIndex()


  /**
   * Internal method to verify the CRF model
   * @param test the same source in the CRFLearn
   * @param model the output from CRFLearn
   * @return the source with predictive labels
   */
  def verify(test: Array[String],
             model: Array[String]): Array[String] = {
    var tagger: Tagger = new Tagger()
    // featureIdx = featureIdx.openTagSet(test)
    tagger = tagger.read(test)
    featureIdx = featureIdx.openFromArray(model)
    tagger.open(featureIdx)
    tagger.parse()
    tagger.createOutput()
  }

  /**
   * Internal method to train the CRF model
   * @param template the template to train the model
   * @param train the source for the training
   * @return the model of the source
   */
  def learn(template: Array[String],
            train: Array[String]): Array[String] = {
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
    featureIdx.saveModel
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
      while(i < threadNum) {
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
   * Feature file format
   * word|word characteristic|designated label
   *
   * @param templates Source templates for training the model
   * @param features Source files for training the model
   * @return Model of a unit
   */
  def runCRF(templates: RDD[Array[String]],
             features: RDD[Array[String]]): CRFModel = {
    val crf = new CRF()
    val template: Array[Array[String]] = templates.toLocalIterator.toArray
    sc = features.sparkContext
    val finalArray = features.flatMap { iter =>
      var i: Int = 0
      val output: ArrayBuffer[Array[String]] = new ArrayBuffer[Array[String]]()
      while (i < template.length) {
        val model: Array[String] = crf.learn(template(i), iter)
        output.append(model)
        i += 1
      }
      output
    }.collect()
    new CRFModel(finalArray)
  }

  /**
   * Verify CRF model
   * Test result format:
   * word|word characteristic|designated label|predicted label
   *
   * @param tests  Source files to be verified
   * @param models Model files after call the CRF learn
   * @return Source files with the predictive labels
   */
  def verifyCRF(tests: RDD[Array[String]],
                models: RDD[Array[String]]): CRFModel = {
    val crf = new CRF()
    val test: Array[Array[String]] = tests.toLocalIterator.toArray
    val model: Array[Array[String]] = models.toLocalIterator.toArray
    sc = tests.sparkContext
    val finalArray = test.indices.map(idx => {
      val result: Array[String] = crf.verify(test(idx), model(idx))
      result
    }).toArray
    new CRFModel(finalArray)
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

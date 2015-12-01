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

private[mllib] class CRF {
  private val freq: Integer = 1
  private val maxiter: Integer = 100000
  private val cost: Double = 1.0
  private val eta: Double = 0.0001
  private val C: Float = 1
  private val threadNum: Integer = Runtime.getRuntime.availableProcessors()
  private val threadPool: Array[CRFThread] = new Array[CRFThread](threadNum)
  private var featureIdx: FeatureIndex = new FeatureIndex()
  private var modelTxt: RDD[String] = null

  /**
   * Verify the CRF model
   * @param test
   * @param model
   * @return
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
   * Train the CRF model
   * @param template
   * @param train
   * @return
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
   * @return
   */
  def getModelTxt(): RDD[String] = {
    modelTxt
  }

  /**
   * Parse segments in the sentences or paragraph
   * @param tagger
   * @param featureIndex
   * @param alpha
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
   * Get part of templates and paragraphs from RDD
   * Train CRF Model
   * @param template
   * @param train
   * @return
   */
  def runCRF(template: RDD[String],
             train: RDD[String]): RDD[String] = {
    val crf = new CRF()
    val templates: Array[String] = template.toLocalIterator.toArray
    val features: Array[String] = train.toLocalIterator.toArray
    val model: ArrayBuffer[String] = new ArrayBuffer[String]()
    var i: Int = 0
    var j: Int = 0
    sc = template.sparkContext
    while (i < templates.length) {
      while (j < features.length) {
        model.append(crf.learn(templates(i), features(j)))
        j += 1
      }
      i += 1
    }
    sc.parallelize(model)
  }

  /**
   * Verify CRF model after call runCRF and get the model.
   * @param test
   * @param model
   * @return
   */
  def verifyCRF(test: RDD[String],
                model: RDD[String]): RDD[String] = {
    val crf = new CRF()
    val tests: Array[String] = test.toLocalIterator.toArray
    val models: Array[String] = model.toLocalIterator.toArray
    val result: ArrayBuffer[String] = new ArrayBuffer[String]()
    var i: Int = 0
    var j: Int = 0
    sc = test.sparkContext
    while (i < tests.length) {
      while (j < models.length) {
        result.append(crf.verify(tests(i), models(j)))
        j += 1
      }
      i += 1
    }
    sc.parallelize(result)
  }

  /**
   * Get CRF model detail in texts.
   * @return
   */
  def getModelTxt(): CRFModel = {
    val crf = new CRF()
    new CRFModel(crf.getModelTxt())
  }

  def getSparkContext: SparkContext = {
    if (sc != null) {
      sc
    } else {
      null
    }
  }
}

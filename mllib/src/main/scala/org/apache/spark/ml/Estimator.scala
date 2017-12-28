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

package org.apache.spark.ml

import java.util.{Iterator => JIterator, NoSuchElementException}
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.varargs

import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.sql.Dataset

/**
 * :: DeveloperApi ::
 * Abstract class for estimators that fit models to data.
 */
@DeveloperApi
abstract class Estimator[M <: Model[M]] extends PipelineStage {

  /**
   * Fits a single model to the input data with optional parameters.
   *
   * @param dataset input dataset
   * @param firstParamPair the first param pair, overrides embedded params
   * @param otherParamPairs other param pairs.  These values override any specified in this
   *                        Estimator's embedded ParamMap.
   * @return fitted model
   */
  @Since("2.0.0")
  @varargs
  def fit(dataset: Dataset[_], firstParamPair: ParamPair[_], otherParamPairs: ParamPair[_]*): M = {
    val map = new ParamMap()
      .put(firstParamPair)
      .put(otherParamPairs: _*)
    fit(dataset, map)
  }

  /**
   * Fits a single model to the input data with provided parameter map.
   *
   * @param dataset input dataset
   * @param paramMap Parameter map.
   *                 These values override any specified in this Estimator's embedded ParamMap.
   * @return fitted model
   */
  @Since("2.0.0")
  def fit(dataset: Dataset[_], paramMap: ParamMap): M = {
    copy(paramMap).fit(dataset)
  }

  /**
   * Fits a model to the input data.
   */
  @Since("2.0.0")
  def fit(dataset: Dataset[_]): M

  /**
   * Fits multiple models to the input data with multiple sets of parameters.
   * The default implementation uses a for loop on each parameter map.
   * Subclasses could override this to optimize multi-model training.
   *
   * @param dataset input dataset
   * @param paramMaps An array of parameter maps.
   *                  These values override any specified in this Estimator's embedded ParamMap.
   * @return fitted models, matching the input parameter maps
   */
  @Since("2.0.0")
  def fit(dataset: Dataset[_], paramMaps: Array[ParamMap]): Seq[M] = {
    val modelIter = fitMultiple(dataset, paramMaps)
    val models = paramMaps.map { _ =>
      val (index, model) = modelIter.next()
      (index.toInt, model)
    }
    paramMaps.indices.map(models.toMap)
}

  /**
   * Fits multiple models to the input data with multiple sets of parameters. The default
   * implementation calls `fit` once for each call to the iterator's `next` method. Subclasses
   * could override this to optimize multi-model training.
   *
   * @param dataset input dataset
   * @param paramMaps An array of parameter maps.
   *                  These values override any specified in this Estimator's embedded ParamMap.
   * @return An iterator which produces one model per call to `next`. The models may be produced in
   *         a different order than the order of the parameters in paramMap. The next method of
   *         the iterator will return a tuple of the form `(index, model)` where model corresponds
   *         to `paramMaps(index)`. This Iterator should be thread safe, meaning concurrent calls
   *         to `next` should always produce unique values of `index`.
   *
   * :: Experimental ::
   */
  @Experimental
  @Since("2.3.0")
  def fitMultiple(
      dataset: Dataset[_],
      paramMaps: Array[ParamMap]): JIterator[(Integer, M)] = {

    val numModel = paramMaps.length
    val counter = new AtomicInteger(0)
    new JIterator[(Integer, M)] {
      def next(): (Integer, M) = {
        var index: Int = _
        do {
          index = counter.get()
          if (index >= numModel) {
            throw new NoSuchElementException("Iterator finished.")
          }
        } while (!counter.compareAndSet(index, index + 1))
        (index, fit(dataset, paramMaps(index)))
      }

      override def hasNext: Boolean = counter.get() < numModel
    }
  }

  override def copy(extra: ParamMap): Estimator[M]
}

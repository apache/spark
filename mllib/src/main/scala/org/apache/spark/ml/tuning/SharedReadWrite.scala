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

package org.apache.spark.ml.tuning

import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JObject}

import org.apache.spark.SparkContext
import org.apache.spark.ml._
import org.apache.spark.ml.classification.OneVsRestParams
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.feature.RFormulaModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util._

private[tunning] trait SharedReadWrite {
  /**
   * Examine the given estimator (which may be a compound estimator) and extract a mapping
   * from UIDs to corresponding [[Params]] instances.
   */
  def getUidMap(instance: Params): Map[String, Params] = {
    val uidList = getUidMapImpl(instance)
    val uidMap = uidList.toMap
    if (uidList.size != uidMap.size) {
      throw new RuntimeException("CrossValidator.load found a compound estimator with stages" +
        s" with duplicate UIDs.  List of UIDs: ${uidList.map(_._1).mkString(", ")}")
    }
    uidMap
  }

  def getUidMapImpl(instance: Params): List[(String, Params)] = {
    val subStages: Array[Params] = instance match {
      case p: Pipeline => p.getStages.asInstanceOf[Array[Params]]
      case pm: PipelineModel => pm.stages.asInstanceOf[Array[Params]]
      case v: ValidatorParams => Array(v.getEstimator, v.getEvaluator)
      case ovr: OneVsRestParams =>
        // TODO: SPARK-11892: This case may require special handling.
        throw new UnsupportedOperationException("CrossValidator write will fail because it" +
          " cannot yet handle an estimator containing type: ${ovr.getClass.getName}")
      case rform: RFormulaModel =>
        // TODO: SPARK-11891: This case may require special handling.
        throw new UnsupportedOperationException("CrossValidator write will fail because it" +
          " cannot yet handle an estimator containing an RFormulaModel")
      case _: Params => Array()
    }
    val subStageMaps = subStages.map(getUidMapImpl).foldLeft(List.empty[(String, Params)])(_ ++ _)
    List((instance.uid, instance)) ++ subStageMaps
  }

  /**
   * Check that [[ValidatorParams.evaluator]] and [[ValidatorParams.estimator]] are Writable.
   * This does not check [[ValidatorParams.estimatorParamMaps]].
   */
  def validateParams(instance: ValidatorParams): Unit = {
    def checkElement(elem: Params, name: String): Unit = elem match {
      case stage: MLWritable => // good
      case other =>
        throw new UnsupportedOperationException("CrossValidator write will fail " +
          s" because it contains $name which does not implement Writable." +
          s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
    }
    checkElement(instance.getEvaluator, "evaluator")
    checkElement(instance.getEstimator, "estimator")
    // Check to make sure all Params apply to this estimator.  Throw an error if any do not.
    // Extraneous Params would cause problems when loading the estimatorParamMaps.
    val uidToInstance: Map[String, Params] = getUidMap(instance)
    instance.getEstimatorParamMaps.foreach { case pMap: ParamMap =>
      pMap.toSeq.foreach { case ParamPair(p, v) =>
        require(uidToInstance.contains(p.parent), s"ValidatorParams save requires all Params in" +
          s" estimatorParamMaps to apply to this ValidatorParams, its Estimator, or its" +
          s" Evaluator. An extraneous Param was found: $p")
      }
    }
  }

  def saveImpl(
      path: String,
      instance: ValidatorParams,
      sc: SparkContext,
      extraMetadata: Option[JObject] = None): Unit = {
    import org.json4s.JsonDSL._

    val estimatorParamMapsJson = compact(render(
      instance.getEstimatorParamMaps.map { case paramMap =>
        paramMap.toSeq.map { case ParamPair(p, v) =>
          Map("parent" -> p.parent, "name" -> p.name, "value" -> p.jsonEncode(v))
        }
      }.toSeq
    ))

    val validatorSpecificParams = instance match {
      case cv: CrossValidatorParams =>
        List("numFolds" -> parse(cv.numFolds.jsonEncode(cv.getNumFolds)))
      case tvs: TrainValidationSplitParams =>
        List("trainRatio" -> parse(tvs.trainRatio.jsonEncode(tvs.getTrainRatio)))
      case _ => List()
    }

    val jsonParams = validatorSpecificParams ++ List(
      "estimatorParamMaps" -> parse(estimatorParamMapsJson))

    DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))

    val evaluatorPath = new Path(path, "evaluator").toString
    instance.getEvaluator.asInstanceOf[MLWritable].save(evaluatorPath)
    val estimatorPath = new Path(path, "estimator").toString
    instance.getEstimator.asInstanceOf[MLWritable].save(estimatorPath)
  }

  def load(
      path: String,
      sc: SparkContext,
      expectedClassName: String): (Metadata, Estimator[_], Evaluator, Array[ParamMap]) = {

    val metadata = DefaultParamsReader.loadMetadata(path, sc, expectedClassName)

    implicit val format = DefaultFormats
    val evaluatorPath = new Path(path, "evaluator").toString
    val evaluator = DefaultParamsReader.loadParamsInstance[Evaluator](evaluatorPath, sc)
    val estimatorPath = new Path(path, "estimator").toString
    val estimator = DefaultParamsReader.loadParamsInstance[Estimator[_]](estimatorPath, sc)

    val uidToParams = Map(evaluator.uid -> evaluator) ++ getUidMap(estimator)

    val estimatorParamMaps: Array[ParamMap] =
      (metadata.params \ "estimatorParamMaps").extract[Seq[Seq[Map[String, String]]]].map {
        pMap =>
          val paramPairs = pMap.map { case pInfo: Map[String, String] =>
            val est = uidToParams(pInfo("parent"))
            val param = est.getParam(pInfo("name"))
            val value = param.jsonDecode(pInfo("value"))
            param -> value
          }
          ParamMap(paramPairs: _*)
      }.toArray

    (metadata, estimator, evaluator, estimatorParamMaps)
  }
}

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
import org.json4s.{DefaultFormats, _}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{Param, ParamMap, ParamPair, Params}
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.sql.types.StructType

/**
 * Common params for [[TrainValidationSplitParams]] and [[CrossValidatorParams]].
 */
private[ml] trait ValidatorParams extends HasSeed with Params {

  /**
   * param for the estimator to be validated
   *
   * @group param
   */
  val estimators: Param[Array[Estimator[_]]] =
    new Param(this, "estimators", "estimators for selection")

  /** @group getParam */
  def getEstimator: Estimator[_] = $(estimators).head

  /** @group getParam */
  def getEstimators: Array[Estimator[_]] = $(estimators)

  /**
   * param for estimator param maps
   *
   * @group param
   */
  val estimatorsParamMaps: Param[Array[Array[ParamMap]]] =
    new Param(this, "estimatorsParamMaps", "param maps for the estimators")

  /** @group getParam */
  def getEstimatorParamMaps: Array[ParamMap] = $(estimatorsParamMaps).head

  /** @group getParam */
  def getEstimatorsParamMaps: Array[Array[ParamMap]] = $(estimatorsParamMaps)

  /**
   * param for the evaluator used to select hyper-parameters that maximize the validated metric
   *
   * @group param
   */
  val evaluator: Param[Evaluator] = new Param(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")

  /** @group getParam */
  def getEvaluator: Evaluator = $(evaluator)

  protected def getModelToEstIndex: Array[Int] =
    $(estimatorsParamMaps).map(_.length).zipWithIndex.flatMap(idx => List.fill(idx._1)(idx._2))

  protected def getModelCount: Int = $(estimatorsParamMaps).flatten.length

  protected def transformSchemaImpl(schema: StructType): StructType = {
    def transformSchemaIdx(schema: StructType, idx: Int): StructType = {
      require($(estimatorsParamMaps)(idx).nonEmpty,
        s"Validator requires non-empty estimatorParamMaps")
      val firstEstimatorParamMap = $(estimatorsParamMaps)(idx).head
      val est = $(estimators)(idx)
      for (paramMap <- $(estimatorsParamMaps)(idx).tail) {
        est.copy(paramMap).transformSchema(schema)
      }
      est.copy(firstEstimatorParamMap).transformSchema(schema)
    }

    require($(estimators).length == $(estimatorsParamMaps).length,
      s"Number of estimators must match number of estimatorParamMaps")
    $(estimators).indices.map(idx => transformSchemaIdx(schema, idx))
      .foldLeft(schema)((acc, curr) => acc.merge(curr))
  }

  /**
   * Instrumentation logging for tuning params including the inner estimator and evaluator info.
   */
  protected def logTuningParams(instrumentation: Instrumentation[_], idx: Int = 0): Unit = {
    instrumentation.logNamedValue("estimator", $(estimators).length match {
      case len if len > idx => $(estimators)(idx).getClass.getCanonicalName
      case _ => $(estimators).getClass.getCanonicalName
    })
    instrumentation.logNamedValue("evaluator", $(evaluator).getClass.getCanonicalName)
    instrumentation.logNamedValue("estimatorParamMapsLength", $(estimatorsParamMaps).length match {
      case len if len > idx => $(estimatorsParamMaps)(idx).length
      case _ => 0
    })
  }
}

private[ml] object ValidatorParams {
  /**
   * Check that [[ValidatorParams.evaluator]] and [[ValidatorParams.estimators]] are Writable.
   * This does not check [[ValidatorParams.estimatorsParamMaps]].
   */
  def validateParams(instance: ValidatorParams): Unit = {
    def checkElement(elem: Params, name: String): Unit = elem match {
      case stage: MLWritable => // good
      case other =>
        throw new UnsupportedOperationException(instance.getClass.getName + " write will fail " +
          s" because it contains $name which does not implement Writable." +
          s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
    }
    checkElement(instance.getEvaluator, "evaluator")
    instance.getEstimators.foreach(checkElement(_, "estimator"))
    // Check to make sure all Params apply to this estimator.  Throw an error if any do not.
    // Extraneous Params would cause problems when loading the estimatorParamMaps.
    val uidToInstance: Map[String, Params] = MetaAlgorithmReadWrite.getUidMap(instance)
    instance.getEstimatorsParamMaps.foreach {
      _.foreach { case pMap: ParamMap =>
        pMap.toSeq.foreach { case ParamPair(p, v) =>
          require(uidToInstance.contains(p.parent), s"ValidatorParams save requires all Params in" +
            s" estimatorParamMaps to apply to this ValidatorParams, its Estimator, or its" +
            s" Evaluator. An extraneous Param was found: $p")
        }
      }
    }
  }

  /**
   * Generic implementation of save for [[ValidatorParams]] types.
   * This handles all [[ValidatorParams]] fields and saves [[Param]] values, but the implementing
   * class needs to handle model data.
   */
  def saveImpl(
      path: String,
      instance: ValidatorParams,
      sc: SparkContext,
      extraMetadata: Option[JObject] = None): Unit = {
    import org.json4s.JsonDSL._

    val estimatorsParamMapsJson =
      instance.getEstimatorsParamMaps.zipWithIndex.map { case (paramMaps, idx) =>
        val json = compact(render(
          paramMaps.map { case paramMap =>
            paramMap.toSeq.map { case ParamPair(p, v) =>
              Map("parent" -> p.parent, "name" -> p.name, "value" -> p.jsonEncode(v))
            }
          }.toSeq
        ))
        indexKeyName("estimatorParamMaps", idx) -> parse(json)
      }

    val validatorSpecificParams = instance match {
      case cv: CrossValidatorParams =>
        List("numFolds" -> parse(cv.numFolds.jsonEncode(cv.getNumFolds)))
      case tvs: TrainValidationSplitParams =>
        List("trainRatio" -> parse(tvs.trainRatio.jsonEncode(tvs.getTrainRatio)))
      case _ =>
        // This should not happen.
        throw new NotImplementedError("ValidatorParams.saveImpl does not handle type: " +
          instance.getClass.getCanonicalName)
    }

    val jsonParams = validatorSpecificParams ++ estimatorsParamMapsJson ++ List(
      "estimatorNumber" -> parse(instance.getEstimators.length.toString),
      "seed" -> parse(instance.seed.jsonEncode(instance.getSeed)))

    DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))

    val evaluatorPath = new Path(path, "evaluator").toString
    instance.getEvaluator.asInstanceOf[MLWritable].save(evaluatorPath)

    instance.getEstimators.zipWithIndex.foreach { case(est, idx) =>
      val estimatorPath = new Path(path, indexKeyName("estimator", idx)).toString
      est.asInstanceOf[MLWritable].save(estimatorPath)
    }
  }

  /**
   * Generic implementation of load for [[ValidatorParams]] types.
   * This handles all [[ValidatorParams]] fields, but the implementing
   * class needs to handle model data and special [[Param]] values.
   */
  def loadImpl[M <: Model[M]](
      path: String,
      sc: SparkContext,
      expectedClassName: String):
  (Metadata, Array[Estimator[_]], Evaluator, Array[Array[ParamMap]]) = {

    val metadata = DefaultParamsReader.loadMetadata(path, sc, expectedClassName)

    implicit val format = DefaultFormats
    val evaluatorPath = new Path(path, "evaluator").toString
    val evaluator = DefaultParamsReader.loadParamsInstance[Evaluator](evaluatorPath, sc)

    // backwards compatible if missing, assumes 1 estimator
    val estimatorNumber = (metadata.params \ "estimatorNumber").extractOrElse[String]("1").toInt
    val estimators: Array[Estimator[_]] = (0 until estimatorNumber).map { idx =>
      val estimatorPath = new Path(path, indexKeyName("estimator", idx)).toString
      DefaultParamsReader.loadParamsInstance[Estimator[M]](estimatorPath, sc)
    }.toArray

    val uidToParams = Map(evaluator.uid -> evaluator) ++
      estimators.flatMap(MetaAlgorithmReadWrite.getUidMap)

    val estimatorsParamsMaps: Array[Array[ParamMap]] = (0 until estimatorNumber).map { idx =>
      (metadata.params \ indexKeyName("estimatorParamMaps", idx))
        .extract[Seq[Seq[Map[String, String]]]].map { pMap =>
          val paramPairs = pMap.map { case pInfo: Map[String, String] =>
            val est = uidToParams(pInfo("parent"))
            val param = est.getParam(pInfo("name"))
            val value = param.jsonDecode(pInfo("value"))
            param -> value
          }
          ParamMap(paramPairs: _*)
      }.toArray
    }.toArray

    (metadata, estimators, evaluator, estimatorsParamsMaps)
  }

  private def indexKeyName(name: String, idx: Int): String = idx match {
    case idx if idx == 0 => name  // backwards compatible name for head
    case idx => name + idx.toString
  }
}

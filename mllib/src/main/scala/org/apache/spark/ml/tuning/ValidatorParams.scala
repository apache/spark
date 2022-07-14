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
import org.json4s._
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
  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")

  /** @group getParam */
  def getEstimator: Estimator[_] = $(estimator)

  /**
   * param for estimator param maps
   *
   * @group param
   */
  val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param(this, "estimatorParamMaps", "param maps for the estimator")

  /** @group getParam */
  def getEstimatorParamMaps: Array[ParamMap] = $(estimatorParamMaps)

  /**
   * param for the evaluator used to select hyper-parameters that maximize the validated metric
   *
   * @group param
   */
  val evaluator: Param[Evaluator] = new Param(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")

  /** @group getParam */
  def getEvaluator: Evaluator = $(evaluator)

  protected def transformSchemaImpl(schema: StructType): StructType = {
    require($(estimatorParamMaps).nonEmpty, s"Validator requires non-empty estimatorParamMaps")
    val firstEstimatorParamMap = $(estimatorParamMaps).head
    val est = $(estimator)
    for (paramMap <- $(estimatorParamMaps).tail) {
      est.copy(paramMap).transformSchema(schema)
    }
    est.copy(firstEstimatorParamMap).transformSchema(schema)
  }

  /**
   * Instrumentation logging for tuning params including the inner estimator and evaluator info.
   */
  protected def logTuningParams(instrumentation: Instrumentation): Unit = {
    instrumentation.logNamedValue("estimator", $(estimator).getClass.getCanonicalName)
    instrumentation.logNamedValue("evaluator", $(evaluator).getClass.getCanonicalName)
    instrumentation.logNamedValue("estimatorParamMapsLength", $(estimatorParamMaps).length)
  }
}

private[ml] object ValidatorParams {
  /**
   * Check that [[ValidatorParams.evaluator]] and [[ValidatorParams.estimator]] are Writable.
   * This does not check [[ValidatorParams.estimatorParamMaps]].
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
    checkElement(instance.getEstimator, "estimator")
    // Check to make sure all Params apply to this estimator.  Throw an error if any do not.
    // Extraneous Params would cause problems when loading the estimatorParamMaps.
    val uidToInstance: Map[String, Params] = MetaAlgorithmReadWrite.getUidMap(instance)
    instance.getEstimatorParamMaps.foreach { case pMap: ParamMap =>
      pMap.toSeq.foreach { case ParamPair(p, v) =>
        require(uidToInstance.contains(p.parent), s"ValidatorParams save requires all Params in" +
          s" estimatorParamMaps to apply to this ValidatorParams, its Estimator, or its" +
          s" Evaluator. An extraneous Param was found: $p")
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

    var numParamsNotJson = 0
    val estimatorParamMapsJson = compact(render(
      instance.getEstimatorParamMaps.map { case paramMap =>
        paramMap.toSeq.map { case ParamPair(p, v) =>
          v match {
            case writeableObj: DefaultParamsWritable =>
              val relativePath = "epm_" + p.name + numParamsNotJson
              val paramPath = new Path(path, relativePath).toString
              numParamsNotJson += 1
              writeableObj.save(paramPath)
              Map("parent" -> p.parent, "name" -> p.name,
                "value" -> compact(render(JString(relativePath))),
                "isJson" -> compact(render(JBool(false))))
            case _: MLWritable =>
              throw new UnsupportedOperationException("ValidatorParams.saveImpl does not handle" +
                " parameters of type: MLWritable that are not DefaultParamsWritable")
            case _ =>
              Map("parent" -> p.parent, "name" -> p.name, "value" -> p.jsonEncode(v),
                "isJson" -> compact(render(JBool(true))))
          }
        }
      }.toSeq
    ))

    val params = instance.extractParamMap().toSeq
    val skipParams = List("estimator", "evaluator", "estimatorParamMaps")
    val jsonParams = render(params
      .filter { case ParamPair(p, v) => !skipParams.contains(p.name)}
      .map { case ParamPair(p, v) =>
        p.name -> parse(p.jsonEncode(v))
      }.toList ++ List("estimatorParamMaps" -> parse(estimatorParamMapsJson))
    )

    DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))

    val evaluatorPath = new Path(path, "evaluator").toString
    instance.getEvaluator.asInstanceOf[MLWritable].save(evaluatorPath)
    val estimatorPath = new Path(path, "estimator").toString
    instance.getEstimator.asInstanceOf[MLWritable].save(estimatorPath)
  }

  /**
   * Generic implementation of load for [[ValidatorParams]] types.
   * This handles all [[ValidatorParams]] fields, but the implementing
   * class needs to handle model data and special [[Param]] values.
   */
  def loadImpl[M <: Model[M]](
      path: String,
      sc: SparkContext,
      expectedClassName: String): (Metadata, Estimator[M], Evaluator, Array[ParamMap]) = {

    val metadata = DefaultParamsReader.loadMetadata(path, sc, expectedClassName)

    implicit val format = DefaultFormats
    val evaluatorPath = new Path(path, "evaluator").toString
    val evaluator = DefaultParamsReader.loadParamsInstance[Evaluator](evaluatorPath, sc)
    val estimatorPath = new Path(path, "estimator").toString
    val estimator = DefaultParamsReader.loadParamsInstance[Estimator[M]](estimatorPath, sc)

    val uidToParams = Map(evaluator.uid -> evaluator) ++ MetaAlgorithmReadWrite.getUidMap(estimator)

    val estimatorParamMaps: Array[ParamMap] =
      (metadata.params \ "estimatorParamMaps").extract[Seq[Seq[Map[String, String]]]].map {
        pMap =>
          val paramPairs = pMap.map { case pInfo: Map[String, String] =>
            val est = uidToParams(pInfo("parent"))
            val param = est.getParam(pInfo("name"))
            // [Spark-21221] introduced the isJson field
            if (!pInfo.contains("isJson") ||
                (pInfo.contains("isJson") && pInfo("isJson").toBoolean.booleanValue())) {
              val value = param.jsonDecode(pInfo("value"))
              param -> value
            } else {
              val relativePath = param.jsonDecode(pInfo("value")).toString
              val value = DefaultParamsReader
                .loadParamsInstance[MLWritable](new Path(path, relativePath).toString, sc)
              param -> value
            }
          }
          ParamMap(paramPairs: _*)
      }.toArray

    (metadata, estimator, evaluator, estimatorParamMaps)
  }
}

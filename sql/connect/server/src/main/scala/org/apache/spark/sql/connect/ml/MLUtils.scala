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

package org.apache.spark.sql.connect.ml

import java.util.ServiceLoader

import scala.collection.immutable.HashSet
import scala.jdk.CollectionConverters.{IterableHasAsScala, MapHasAsScala}

import org.apache.commons.lang3.reflect.MethodUtils.{invokeMethod, invokeStaticMethod}

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.util.Utils

object MLUtils {

  private lazy val estimators: Map[String, Class[_]] = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[Estimator[_]], loader)
    val providers = serviceLoader.asScala.toList
    providers.map(est => est.getClass.getName -> est.getClass).toMap
  }

  private lazy val transformers: Map[String, Class[_]] = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[Transformer], loader)
    val providers = serviceLoader.asScala.toList
    providers.map(est => est.getClass.getName -> est.getClass).toMap
  }

  def deserializeVector(vector: proto.Vector): Vector = {
    if (vector.hasDense) {
      val values = vector.getDense.getValueList.asScala.map(_.toDouble).toArray
      Vectors.dense(values)
    } else {
      val size = vector.getSparse.getSize
      val indices = vector.getSparse.getIndexList.asScala.map(_.toInt).toArray
      val values = vector.getSparse.getValueList.asScala.map(_.toDouble).toArray
      Vectors.sparse(size, indices, values)
    }
  }

  def deserializeMatrix(matrix: proto.Matrix): Matrix = {
    if (matrix.hasDense) {
      val values = matrix.getDense.getValueList.asScala.map(_.toDouble).toArray
      Matrices.dense(matrix.getDense.getNumRows, matrix.getDense.getNumCols, values)
    } else {
      val sparse = matrix.getSparse
      val colPtrs = sparse.getColptrList.asScala.map(_.toInt).toArray
      val rowIndices = sparse.getRowIndexList.asScala.map(_.toInt).toArray
      val values = sparse.getValueList.asScala.map(_.toDouble).toArray
      Matrices.sparse(sparse.getNumRows, sparse.getNumCols, colPtrs, rowIndices, values)
    }
  }

  def setInstanceParams(instance: Params, params: proto.MlParams): Unit = {
    params.getParamsMap.asScala.foreach { case (name, paramProto) =>
      val p = instance.getParam(name)
      val value = if (paramProto.hasLiteral) {
        convertParamValue(
          p.paramValueClassTag.runtimeClass,
          LiteralValueProtoConverter.toCatalystValue(paramProto.getLiteral))
      } else if (paramProto.hasVector) {
        deserializeVector(paramProto.getVector)
      } else if (paramProto.hasMatrix) {
        deserializeMatrix(paramProto.getMatrix)
      } else {
        throw new RuntimeException("Unsupported parameter type")
      }
      instance.set(p, value)
    }
  }

  private def convertArray(paramType: Class[_], array: Array[_]): Array[_] = {
    if (paramType == classOf[Byte]) {
      array.map(_.asInstanceOf[Byte])
    } else if (paramType == classOf[Short]) {
      array.map(_.asInstanceOf[Short])
    } else if (paramType == classOf[Int]) {
      array.map(_.asInstanceOf[Int])
    } else if (paramType == classOf[Long]) {
      array.map(_.asInstanceOf[Long])
    } else if (paramType == classOf[Float]) {
      array.map(_.asInstanceOf[Float])
    } else if (paramType == classOf[Double]) {
      array.map(_.asInstanceOf[Double])
    } else if (paramType == classOf[String]) {
      array.map(_.asInstanceOf[String])
    } else {
      array
    }
  }

  private def convertParamValue(paramType: Class[_], value: Any): Any = {
    // Some cases the param type might be mismatched with the value type.
    // Because in python side we only have int / float type for numeric params.
    // e.g.:
    // param type is Int but client sends a Long type.
    // param type is Long but client sends a Int type.
    // param type is Float but client sends a Double type.
    // param type is Array[Int] but client sends a Array[Long] type.
    // param type is Array[Float] but client sends a Array[Double] type.
    // param type is Array[Array[Int]] but client sends a Array[Array[Long]] type.
    // param type is Array[Array[Float]] but client sends a Array[Array[Double]] type.
    if (paramType == classOf[Byte]) {
      value.asInstanceOf[java.lang.Number].byteValue()
    } else if (paramType == classOf[Short]) {
      value.asInstanceOf[java.lang.Number].shortValue()
    } else if (paramType == classOf[Int]) {
      value.asInstanceOf[java.lang.Number].intValue()
    } else if (paramType == classOf[Long]) {
      value.asInstanceOf[java.lang.Number].longValue()
    } else if (paramType == classOf[Float]) {
      value.asInstanceOf[java.lang.Number].floatValue()
    } else if (paramType == classOf[Double]) {
      value.asInstanceOf[java.lang.Number].doubleValue()
    } else if (paramType.isArray) {
      val compType = paramType.getComponentType
      val array = value.asInstanceOf[Array[_]].map { e =>
        convertParamValue(compType, e)
      }
      convertArray(compType, array)
    } else {
      value
    }
  }

  def parseRelationProto(relation: proto.Relation, sessionHolder: SessionHolder): DataFrame = {
    val planner = new SparkConnectPlanner(sessionHolder)
    val plan = planner.transformRelation(relation)
    Dataset.ofRows(sessionHolder.session, plan)
  }

  /**
   * Get the Estimator instance according to the fit command
   *
   * @param fit
   *   command
   * @return
   *   an Estimator
   */
  def getEstimator(fit: proto.MlCommand.Fit): Estimator[_] = {
    // TODO support plugin
    // Get the estimator according to the fit command
    val name = fit.getEstimator.getName
    if (estimators.isEmpty || !estimators.contains(name)) {
      throw new RuntimeException(s"Failed to find estimator: $name")
    }
    val uid = fit.getEstimator.getUid
    val estimator: Estimator[_] = estimators(name)
      .getConstructor(classOf[String])
      .newInstance(uid)
      .asInstanceOf[Estimator[_]]

    // Set parameters for the estimator
    val params = fit.getParams
    MLUtils.setInstanceParams(estimator, params)
    estimator
  }

  def loadModel(className: String, path: String): Model[_] = {
    // scalastyle:off classforname
    val clazz = Class.forName(className)
    // scalastyle:on classforname
    val model = invokeStaticMethod(clazz, "load", path)
    model.asInstanceOf[Model[_]]
  }

  /**
   * Get the transformer instance according to the transform proto
   *
   * @param transformProto
   *   transform proto
   * @return
   *   a Transformer
   */
  def getTransformer(transformProto: proto.MlRelation.Transform): Transformer = {
    // Get the transformer name
    val name = transformProto.getTransformer.getName
    if (transformers.isEmpty || !transformers.contains(name)) {
      throw new RuntimeException(s"Failed to find transformer: $name")
    }
    val uid = transformProto.getTransformer.getUid
    val transformer = transformers(name)
      .getConstructor(classOf[String])
      .newInstance(uid)
      .asInstanceOf[Transformer]

    val params = transformProto.getParams
    MLUtils.setInstanceParams(transformer, params)
    transformer
  }

  private lazy val ALLOWED_ATTRIBUTES = HashSet(
    "toString",
    "numFeatures",
    "predict", // PredictionModel
    "numClasses",
    "predictRaw", // ClassificationModel
    "predictProbability", // ProbabilisticClassificationModel
    "coefficients",
    "intercept",
    "coefficientMatrix",
    "interceptVector", // LogisticRegressionModel
    "summary",
    "hasSummary",
    "evaluate", // LogisticRegressionModel
    "predictions",
    "predictionCol",
    "labelCol",
    "weightCol",
    "labels", // _ClassificationSummary
    "truePositiveRateByLabel",
    "falsePositiveRateByLabel", // _ClassificationSummary
    "precisionByLabel",
    "recallByLabel",
    "fMeasureByLabel",
    "accuracy", // _ClassificationSummary
    "weightedTruePositiveRate",
    "weightedFalsePositiveRate", // _ClassificationSummary
    "weightedRecall",
    "weightedPrecision",
    "weightedFMeasure", // _ClassificationSummary
    "scoreCol",
    "roc",
    "areaUnderROC",
    "pr",
    "fMeasureByThreshold", // _BinaryClassificationSummary
    "precisionByThreshold",
    "recallByThreshold", // _BinaryClassificationSummary
    "probabilityCol",
    "featuresCol", // LogisticRegressionSummary
    "objectiveHistory",
    "totalIterations" // _TrainingSummary
  )

  def invokeMethodAllowed(obj: Object, methodName: String): Object = {
    require(
      ALLOWED_ATTRIBUTES.contains(methodName),
      s"$methodName is not allowed to be accessed.")
    invokeMethod(obj, methodName)
  }

  def invokeMethodAllowed(
      obj: Object,
      methodName: String,
      args: Array[Object],
      parameterTypes: Array[Class[_]]): Object = {
    require(
      ALLOWED_ATTRIBUTES.contains(methodName),
      s"$methodName is not allowed to be accessed.")
    invokeMethod(obj, methodName, args, parameterTypes)
  }

}

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
import scala.jdk.CollectionConverters._

import org.apache.commons.lang3.reflect.MethodUtils.invokeMethod

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.util.{SparkClassUtils, Utils}

private[ml] object MLUtils {

  /**
   * Load the registered ML operators via ServiceLoader
   *
   * @param mlCls
   *   the operator class
   * @return
   *   a Map with name and class
   */
  private def loadOperators(mlCls: Class[_]): Map[String, Class[_]] = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(mlCls, loader)
    val providers = serviceLoader.asScala.toList
    providers.map(est => est.getClass.getName -> est.getClass).toMap
  }

  private lazy val estimators = loadOperators(classOf[Estimator[_]])

  private lazy val transformers = loadOperators(classOf[Transformer])

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

  /**
   * Set the parameters to the ML instance
   *
   * @param instance
   *   an ML operator
   * @param params
   *   the parameters of the ML operator
   */
  def setInstanceParams(instance: Params, params: proto.MlParams): Unit = {
    params.getParamsMap.asScala.foreach { case (name, paramProto) =>
      val p = instance.getParam(name)
      val value = if (paramProto.hasLiteral) {
        reconcileParam(
          p.paramValueClassTag.runtimeClass,
          LiteralValueProtoConverter.toCatalystValue(paramProto.getLiteral))
      } else if (paramProto.hasVector) {
        deserializeVector(paramProto.getVector)
      } else if (paramProto.hasMatrix) {
        deserializeMatrix(paramProto.getMatrix)
      } else {
        throw MlUnsupportedException(s"Unsupported parameter type for ${name}")
      }
      instance.set(p, value)
    }
  }

  /**
   * Convert the array from Object[] to Array[_]
   * @param elementType
   *   the element type of the array
   * @param array
   *   to be reconciled
   * @return
   *   the reconciled array
   */
  private def reconcileArray(elementType: Class[_], array: Array[_]): Array[_] = {
    if (elementType == classOf[Byte]) {
      array.map(_.asInstanceOf[Byte])
    } else if (elementType == classOf[Short]) {
      array.map(_.asInstanceOf[Short])
    } else if (elementType == classOf[Int]) {
      array.map(_.asInstanceOf[Int])
    } else if (elementType == classOf[Long]) {
      array.map(_.asInstanceOf[Long])
    } else if (elementType == classOf[Float]) {
      array.map(_.asInstanceOf[Float])
    } else if (elementType == classOf[Double]) {
      array.map(_.asInstanceOf[Double])
    } else if (elementType == classOf[String]) {
      array.map(_.asInstanceOf[String])
    } else {
      throw MlUnsupportedException(
        s"array element type unsupported, " +
          s"found ${elementType.getName}")
    }
  }

  /**
   * Reconcile the parameter value given the provided parameter type. Currently, support
   * byte/short/int/long/float/double/string and array. Note that, array of array is not supported
   * yet.
   */
  private def reconcileParam(paramType: Class[_], value: Any): Any = {
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
    } else if (paramType == classOf[Boolean]) {
      value.asInstanceOf[Boolean]
    } else if (paramType == classOf[String]) {
      value.asInstanceOf[String]
    } else if (paramType.isArray) {
      val compType = paramType.getComponentType
      if (compType.isArray) {
        throw MlUnsupportedException(s"Array of array unsupported")
      } else {
        val array = value.asInstanceOf[Array[_]].map { e =>
          reconcileParam(compType, e)
        }
        reconcileArray(compType, array)
      }
    } else {
      throw MlUnsupportedException(s"Unsupported parameter type, found ${paramType.getName}")
    }
  }

  def parseRelationProto(relation: proto.Relation, sessionHolder: SessionHolder): DataFrame = {
    val planner = new SparkConnectPlanner(sessionHolder)
    val plan = planner.transformRelation(relation)
    Dataset.ofRows(sessionHolder.session, plan)
  }

  /**
   * Get the instance according to the provided proto information.
   *
   * @param name
   *   The name of the instance (either estimator or transformer).
   * @param uid
   *   The unique identifier for the instance.
   * @param instanceMap
   *   A map of instance names to constructors.
   * @param params
   *   Optional parameters for the instance.
   * @tparam T
   *   The type of the instance (Estimator or Transformer).
   * @return
   *   The instance of the requested type.
   * @throws MlUnsupportedException
   *   If the instance is not supported.
   */
  private def getInstance[T](
      name: String,
      uid: String,
      instanceMap: Map[String, Class[_]],
      params: Option[proto.MlParams]): T = {
    if (instanceMap.isEmpty || !instanceMap.contains(name)) {
      throw MlUnsupportedException(s"Unsupported ML operator, found $name")
    }

    val instance = instanceMap(name)
      .getConstructor(classOf[String])
      .newInstance(uid)
      .asInstanceOf[T]

    // Set parameters for the instance if they are provided
    params.foreach(p => MLUtils.setInstanceParams(instance.asInstanceOf[Params], p))
    instance
  }

  /**
   * Get the Estimator instance according to the proto information
   *
   * @param operator
   *   MlOperator information
   * @param params
   *   The optional parameters of the estimator
   * @return
   *   the estimator
   */
  def getEstimator(operator: proto.MlOperator, params: Option[proto.MlParams]): Estimator[_] = {
    val name = operator.getName
    val uid = operator.getUid
    getInstance[Estimator[_]](name, uid, estimators, params)
  }

  /**
   * Get the transformer instance according to the transform proto
   *
   * @param transformProto
   *   transform proto
   * @return
   *   a transformer
   */
  def getTransformer(transformProto: proto.MlRelation.Transform): Transformer = {
    val name = transformProto.getTransformer.getName
    val uid = transformProto.getTransformer.getUid
    val params = transformProto.getParams
    getInstance[Transformer](name, uid, transformers, Some(params))
  }

  /**
   * Call "load: function on the ML operator given the operator name
   *
   * @param className
   *   the ML operator name
   * @param path
   *   the path to be loaded
   * @return
   *   the ML instance
   */
  def load(className: String, path: String): Object = {
    val loadedMethod = SparkClassUtils.classForName(className).getMethod("load", classOf[String])
    loadedMethod.invoke(null, path)
  }

  // Since we're using reflection way to get the attribute, in order not to
  // leave a security hole, we define an allowed attribute list that can be accessed.
  // The attributes could be retrieved from the corresponding python class
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
    if (!ALLOWED_ATTRIBUTES.contains(methodName)) {
      throw MLAttributeNotAllowedException(methodName)
    }
    invokeMethod(obj, methodName)
  }

  def invokeMethodAllowed(
      obj: Object,
      methodName: String,
      args: Array[Object],
      parameterTypes: Array[Class[_]]): Object = {
    if (!ALLOWED_ATTRIBUTES.contains(methodName)) {
      throw MLAttributeNotAllowedException(methodName)
    }
    invokeMethod(obj, methodName, args, parameterTypes)
  }

  def write(instance: MLWritable, writeProto: proto.MlCommand.Write): Unit = {
    val writer = if (writeProto.getShouldOverwrite) {
      instance.write.overwrite()
    } else {
      instance.write
    }
    val path = writeProto.getPath
    val options = writeProto.getOptionsMap
    options.forEach((k, v) => writer.option(k, v))
    writer.save(path)
  }

}

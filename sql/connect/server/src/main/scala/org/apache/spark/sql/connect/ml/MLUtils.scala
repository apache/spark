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

import java.util.{Optional, ServiceLoader}

import scala.collection.immutable.HashSet
import scala.jdk.CollectionConverters._

import org.apache.commons.lang3.reflect.MethodUtils.invokeMethod

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, LiteralValueProtoConverter}
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.util.{SparkClassUtils, Utils}

private[ml] object MLUtils {

  /**
   * Load the registered ML operators via ServiceLoader
   *
   * @param mlCls
   *   the operator class that will be loaded
   * @return
   *   a Map with name and class
   */
  private def loadOperators(mlCls: Class[_]): Map[String, Class[_]] = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(mlCls, loader)
    val providers = serviceLoader.asScala.toList
    providers.map(est => est.getClass.getName -> est.getClass).toMap
  }

  private def parseInts(ints: proto.Ints): Array[Int] = {
    val size = ints.getValuesCount
    val values = Array.ofDim[Int](size)
    var i = 0
    while (i < size) {
      values(i) = ints.getValues(i)
      i += 1
    }
    values
  }

  private def parseDoubles(doubles: proto.Doubles): Array[Double] = {
    val size = doubles.getValuesCount
    val values = Array.ofDim[Double](size)
    var i = 0
    while (i < size) {
      values(i) = doubles.getValues(i)
      i += 1
    }
    values
  }

  def deserializeVector(s: proto.Expression.Literal.Struct): Vector = {
    assert(s.getElementsCount == 4)
    s.getElements(0).getByte match {
      case 0 =>
        val size = s.getElements(1).getInteger
        val indices = parseInts(s.getElements(2).getSpecializedArray.getInts)
        val values = parseDoubles(s.getElements(3).getSpecializedArray.getDoubles)
        Vectors.sparse(size, indices, values)

      case 1 =>
        val values = parseDoubles(s.getElements(3).getSpecializedArray.getDoubles)
        Vectors.dense(values)

      case o => throw MlUnsupportedException(s"Unknown Vector type $o")
    }
  }

  def deserializeMatrix(s: proto.Expression.Literal.Struct): Matrix = {
    assert(s.getElementsCount == 7)
    s.getElements(0).getByte match {
      case 0 =>
        val numRows = s.getElements(1).getInteger
        val numCols = s.getElements(2).getInteger
        val colPtrs = parseInts(s.getElements(3).getSpecializedArray.getInts)
        val rowIndices = parseInts(s.getElements(4).getSpecializedArray.getInts)
        val values = parseDoubles(s.getElements(5).getSpecializedArray.getDoubles)
        val isTransposed = s.getElements(6).getBoolean
        new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)

      case 1 =>
        val numRows = s.getElements(1).getInteger
        val numCols = s.getElements(2).getInteger
        val values = parseDoubles(s.getElements(5).getSpecializedArray.getDoubles)
        val isTransposed = s.getElements(6).getBoolean
        new DenseMatrix(numRows, numCols, values, isTransposed)

      case o => throw MlUnsupportedException(s"Unknown Matrix type $o")
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
    params.getParamsMap.asScala.foreach { case (name, literal) =>
      val p = instance.getParam(name)
      val value = literal.getLiteralTypeCase match {
        case proto.Expression.Literal.LiteralTypeCase.STRUCT =>
          val s = literal.getStruct
          val schema = DataTypeProtoConverter.toCatalystType(s.getStructType)
          if (schema == VectorUDT.sqlType) {
            deserializeVector(s)
          } else if (schema == MatrixUDT.sqlType) {
            deserializeMatrix(s)
          } else {
            throw MlUnsupportedException(s"Unsupported parameter struct ${schema} for ${name}")
          }

        case _ =>
          reconcileParam(
            p.paramValueClassTag.runtimeClass,
            LiteralValueProtoConverter.toCatalystValue(literal))
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
   * Replace the operator with the value provided by the backend.
   */
  private def replaceOperator(sessionHolder: SessionHolder, name: String): String = {
    SparkConnectPluginRegistry
      .mlBackendRegistry(sessionHolder.session.sessionState.conf)
      .view
      .map(p => p.transform(name))
      .find(_.isPresent) // First come, first served
      .getOrElse(Optional.of(name))
      .get()
  }

  /**
   * Get the Estimator instance according to the proto information
   *
   * @param sessionHolder
   *   session holder to hold the Spark Connect session state
   * @param operator
   *   MlOperator information
   * @param params
   *   The optional parameters of the estimator
   * @return
   *   the estimator
   */
  def getEstimator(
      sessionHolder: SessionHolder,
      operator: proto.MlOperator,
      params: Option[proto.MlParams]): Estimator[_] = {
    val name = replaceOperator(sessionHolder, operator.getName)
    val uid = operator.getUid

    // Load the estimator by ServiceLoader everytime
    val estimators = loadOperators(classOf[Estimator[_]])
    getInstance[Estimator[_]](name, uid, estimators, params)
  }

  /**
   * Get the transformer instance according to the transform proto
   *
   * @param sessionHolder
   *   session holder to hold the Spark Connect session state
   * @param transformProto
   *   transform proto
   * @return
   *   a transformer
   */
  def getTransformer(
      sessionHolder: SessionHolder,
      transformProto: proto.MlRelation.Transform): Transformer = {
    val name = replaceOperator(sessionHolder, transformProto.getTransformer.getName)
    val uid = transformProto.getTransformer.getUid
    val params = transformProto.getParams
    // Load the transformer by ServiceLoader everytime.
    val transformers = loadOperators(classOf[Transformer])
    getInstance[Transformer](name, uid, transformers, Some(params))
  }

  /**
   * Get the Evaluator instance according to the proto information
   *
   * @param sessionHolder
   *   session holder to hold the Spark Connect session state
   * @param operator
   *   MlOperator information
   * @param params
   *   The optional parameters of the evaluator
   * @return
   *   the evaluator
   */
  def getEvaluator(
      sessionHolder: SessionHolder,
      operator: proto.MlOperator,
      params: Option[proto.MlParams]): Evaluator = {
    val name = replaceOperator(sessionHolder, operator.getName)
    val uid = operator.getUid

    // Load the evaluators by ServiceLoader everytime
    val evaluators = loadOperators(classOf[Evaluator])
    getInstance[Evaluator](name, uid, evaluators, params)
  }

  /**
   * Call "load" function on the ML operator given the operator name
   *
   * @param className
   *   the ML operator name
   * @param path
   *   the path to be loaded
   * @return
   *   the ML instance
   */
  def load(sessionHolder: SessionHolder, className: String, path: String): Object = {
    val name = replaceOperator(sessionHolder, className)

    // It's the companion object of the corresponding spark operators to load.
    val objectCls = SparkClassUtils.classForName(name + "$")
    val mlReadableClass = classOf[MLReadable[_]]
    // Make sure it is implementing MLReadable
    if (!mlReadableClass.isAssignableFrom(objectCls)) {
      throw MlUnsupportedException(s"$name must implement MLReadable.")
    }

    val loadedMethod = SparkClassUtils.classForName(name).getMethod("load", classOf[String])
    loadedMethod.invoke(null, path)
  }

  // Since we're using reflection way to get the attribute, in order not to
  // leave a security hole, we define an allowed attribute list that can be accessed.
  // The attributes could be retrieved from the corresponding python class
  private lazy val ALLOWED_ATTRIBUTES = HashSet(
    "toString",
    "toDebugString",
    "numFeatures",
    "predict", // PredictionModel
    "predictLeaf", // Tree models
    "numClasses",
    "depth", // DecisionTreeClassificationModel
    "numNodes", // Tree models
    "totalNumNodes", // Tree models
    "javaTreeWeights", // Tree models
    "treeWeights", // Tree models
    "featureImportances", // Tree models
    "predictRaw", // ClassificationModel
    "predictProbability", // ProbabilisticClassificationModel
    "scale", // LinearRegressionModel
    "coefficients",
    "intercept",
    "coefficientMatrix",
    "interceptVector", // LogisticRegressionModel
    "summary",
    "hasSummary",
    "evaluate", // LogisticRegressionModel
    "evaluateEachIteration", // GBTClassificationModel
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
    "coefficientStandardErrors", // _TrainingSummary
    "degreesOfFreedom", // LinearRegressionSummary
    "devianceResiduals", // LinearRegressionSummary
    "explainedVariance", // LinearRegressionSummary
    "meanAbsoluteError", // LinearRegressionSummary
    "meanSquaredError", // LinearRegressionSummary
    "numInstances", // LinearRegressionSummary
    "pValues", // LinearRegressionSummary
    "r2", // LinearRegressionSummary
    "r2adj", // LinearRegressionSummary
    "residuals", // LinearRegressionSummary
    "rootMeanSquaredError", // LinearRegressionSummary
    "tValues", // LinearRegressionSummary
    "totalIterations", // LinearRegressionSummary
    "k", // KMeansSummary
    "numIter", // KMeansSummary
    "clusterSizes", // KMeansSummary
    "trainingCost", // KMeansSummary
    "cluster", // KMeansSummary
    "computeCost", // BisectingKMeansModel
    "rank", // ALSModel
    "itemFactors", // ALSModel
    "userFactors", // ALSModel
    "recommendForAllUsers", // ALSModel
    "recommendForAllItems", // ALSModel
    "recommendForUserSubset", // ALSModel
    "recommendForItemSubset" // ALSModel
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

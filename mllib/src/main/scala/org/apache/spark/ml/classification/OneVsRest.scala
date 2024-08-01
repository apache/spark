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

package org.apache.spark.ml.classification

import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.language.existentials

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.Since
import org.apache.spark.internal.{LogKeys, MDC}
import org.apache.spark.ml._
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap, ParamPair, Params}
import org.apache.spark.ml.param.shared.{HasParallelism, HasWeightCol}
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ThreadUtils

private[ml] trait ClassifierTypeTrait {
  // scalastyle:off structural.type
  type ClassifierType = Classifier[F, E, M] forSome {
    type F
    type M <: ClassificationModel[F, M]
    type E <: Classifier[F, E, M]
  }
  // scalastyle:on structural.type
}

/**
 * Params for [[OneVsRest]].
 */
private[ml] trait OneVsRestParams extends ClassifierParams
  with ClassifierTypeTrait with HasWeightCol {

  /**
   * param for the base binary classifier that we reduce multiclass classification into.
   * The base classifier input and output columns are ignored in favor of
   * the ones specified in [[OneVsRest]].
   * @group param
   */
  val classifier: Param[ClassifierType] = new Param(this, "classifier", "base binary classifier")

  /** @group getParam */
  def getClassifier: ClassifierType = $(classifier)
}

private[ml] object OneVsRestParams extends ClassifierTypeTrait {

  def validateParams(instance: OneVsRestParams): Unit = {
    def checkElement(elem: Params, name: String): Unit = elem match {
      case stage: MLWritable => // good
      case other =>
        throw new UnsupportedOperationException("OneVsRest write will fail " +
          s" because it contains $name which does not implement MLWritable." +
          s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
    }

    instance match {
      case ovrModel: OneVsRestModel => ovrModel.models.foreach(checkElement(_, "model"))
      case _ => // no need to check OneVsRest here
    }

    checkElement(instance.getClassifier, "classifier")
  }

  def saveImpl(
      path: String,
      instance: OneVsRestParams,
      spark: SparkSession,
      extraMetadata: Option[JObject] = None): Unit = {

    val params = instance.extractParamMap().toSeq
    val jsonParams = render(params
      .filter { case ParamPair(p, v) => p.name != "classifier" }
      .map { case ParamPair(p, v) => p.name -> parse(p.jsonEncode(v)) }
      .toList)

    DefaultParamsWriter.saveMetadata(instance, path, spark, extraMetadata, Some(jsonParams))

    val classifierPath = new Path(path, "classifier").toString
    instance.getClassifier.asInstanceOf[MLWritable].save(classifierPath)
  }

  def loadImpl(
      path: String,
      spark: SparkSession,
      expectedClassName: String): (DefaultParamsReader.Metadata, ClassifierType) = {

    val metadata = DefaultParamsReader.loadMetadata(path, spark, expectedClassName)
    val classifierPath = new Path(path, "classifier").toString
    val estimator = DefaultParamsReader.loadParamsInstance[ClassifierType](classifierPath, spark)
    (metadata, estimator)
  }
}

/**
 * Model produced by [[OneVsRest]].
 * This stores the models resulting from training k binary classifiers: one for each class.
 * Each example is scored against all k models, and the model with the highest score
 * is picked to label the example.
 *
 * @param labelMetadata Metadata of label column if it exists, or Nominal attribute
 *                      representing the number of classes in training dataset otherwise.
 * @param models The binary classification models for the reduction.
 *               The i-th model is produced by testing the i-th class (taking label 1) vs the rest
 *               (taking label 0).
 */
@Since("1.4.0")
final class OneVsRestModel private[ml] (
    @Since("1.4.0") override val uid: String,
    private[ml] val labelMetadata: Metadata,
    @Since("1.4.0") val models: Array[_ <: ClassificationModel[_, _]])
  extends Model[OneVsRestModel] with OneVsRestParams with MLWritable {

  require(models.nonEmpty, "OneVsRestModel requires at least one model for one class")

  @Since("2.4.0")
  val numClasses: Int = models.length

  @Since("2.4.0")
  val numFeatures: Int = models.head.numFeatures

  /** @group setParam */
  @Since("2.1.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.1.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.4.0")
  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema, fitting = false,
      getClassifier.featuresDataType)
    if ($(predictionCol).nonEmpty) {
      outputSchema = SchemaUtils.updateNumValues(outputSchema,
        $(predictionCol), numClasses)
    }
    if ($(rawPredictionCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(rawPredictionCol), numClasses)
    }
    outputSchema
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    // Check schema
    val outputSchema = transformSchema(dataset.schema, logging = true)

    if (getPredictionCol.isEmpty && getRawPredictionCol.isEmpty) {
      logWarning(log"${MDC(LogKeys.UUID, uid)}: OneVsRestModel.transform() does nothing " +
        log"because no output columns were set.")
      return dataset.toDF()
    }

    val isProbModel = models.head.isInstanceOf[ProbabilisticClassificationModel[_, _]]

    // use a temporary raw prediction column to avoid column conflict
    val tmpRawPredName = "mbc$raw" + UUID.randomUUID().toString

    // add an accumulator column to store predictions of all the models
    val accColName = "mbc$acc" + UUID.randomUUID().toString
    val newDataset = dataset.withColumn(accColName, lit(Array.emptyDoubleArray))
    val columns = newDataset.schema.fieldNames.map(col)
    val updateUDF = udf { (preds: Array[Double], pred: Vector) => preds :+ pred(1) }

    // persist if underlying dataset is not persistent.
    val handlePersistence = !dataset.isStreaming && dataset.storageLevel == StorageLevel.NONE
    if (handlePersistence) {
      newDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // update the accumulator column with the result of prediction of models
    val aggregatedDataset = models.foldLeft[DataFrame](newDataset) { case (df, model) =>
      // avoid calling directly setter of model
      val tmpModel = model.copy(ParamMap.empty).asInstanceOf[ClassificationModel[_, _]]
      tmpModel.setFeaturesCol($(featuresCol))
      tmpModel.setRawPredictionCol(tmpRawPredName)
      tmpModel.setPredictionCol("")
      if (isProbModel) {
        tmpModel.asInstanceOf[ProbabilisticClassificationModel[_, _]].setProbabilityCol("")
      }

      import org.apache.spark.util.ArrayImplicits._
      tmpModel.transform(df)
        .withColumn(accColName, updateUDF(col(accColName), col(tmpRawPredName)))
        .select(columns.toImmutableArraySeq: _*)
    }

    if (handlePersistence) {
      newDataset.unpersist()
    }

    var predictionColNames = Seq.empty[String]
    var predictionColumns = Seq.empty[Column]

    if (getRawPredictionCol.nonEmpty) {
      // output the RawPrediction as vector
      val rawPredictionUDF = udf { preds: Array[Double] => Vectors.dense(preds) }
      predictionColNames :+= getRawPredictionCol
      predictionColumns :+= rawPredictionUDF(col(accColName))
        .as($(rawPredictionCol), outputSchema($(rawPredictionCol)).metadata)
    }

    if (getPredictionCol.nonEmpty) {
      // output the index of the classifier with highest confidence as prediction
      val labelUDF = udf { (preds: Array[Double]) => preds.indices.maxBy(preds.apply).toDouble }
      predictionColNames :+= getPredictionCol
      predictionColumns :+= labelUDF(col(accColName))
        .as(getPredictionCol, labelMetadata)
    }

    aggregatedDataset
      .withColumns(predictionColNames, predictionColumns)
      .drop(accColName)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): OneVsRestModel = {
    val copied = new OneVsRestModel(
      uid, labelMetadata, models.map(_.copy(extra).asInstanceOf[ClassificationModel[_, _]]))
    copyValues(copied, extra).setParent(parent)
  }

  @Since("2.0.0")
  override def write: MLWriter = new OneVsRestModel.OneVsRestModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"OneVsRestModel: uid=$uid, classifier=${$(classifier)}, numClasses=$numClasses, " +
      s"numFeatures=$numFeatures"
  }
}

@Since("2.0.0")
object OneVsRestModel extends MLReadable[OneVsRestModel] {

  @Since("2.0.0")
  override def read: MLReader[OneVsRestModel] = new OneVsRestModelReader

  @Since("2.0.0")
  override def load(path: String): OneVsRestModel = super.load(path)

  /** [[MLWriter]] instance for [[OneVsRestModel]] */
  private[OneVsRestModel] class OneVsRestModelWriter(instance: OneVsRestModel) extends MLWriter {

    OneVsRestParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      val extraJson = ("labelMetadata" -> instance.labelMetadata.json) ~
        ("numClasses" -> instance.models.length)
      OneVsRestParams.saveImpl(path, instance, sparkSession, Some(extraJson))
      instance.models.map(_.asInstanceOf[MLWritable]).zipWithIndex.foreach { case (model, idx) =>
        val modelPath = new Path(path, s"model_$idx").toString
        model.save(modelPath)
      }
    }
  }

  private class OneVsRestModelReader extends MLReader[OneVsRestModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[OneVsRestModel].getName

    override def load(path: String): OneVsRestModel = {
      implicit val format = DefaultFormats
      val (metadata, classifier) = OneVsRestParams.loadImpl(path, sparkSession, className)
      val labelMetadata = Metadata.fromJson((metadata.metadata \ "labelMetadata").extract[String])
      val numClasses = (metadata.metadata \ "numClasses").extract[Int]
      val models = Range(0, numClasses).toArray.map { idx =>
        val modelPath = new Path(path, s"model_$idx").toString
        DefaultParamsReader.loadParamsInstance[ClassificationModel[_, _]](modelPath, sparkSession)
      }
      val ovrModel = new OneVsRestModel(metadata.uid, labelMetadata, models)
      metadata.getAndSetParams(ovrModel)
      ovrModel.set("classifier", classifier)
      ovrModel
    }
  }
}

/**
 * Reduction of Multiclass Classification to Binary Classification.
 * Performs reduction using one against all strategy.
 * For a multiclass classification with k classes, train k models (one per class).
 * Each example is scored against all k models and the model with highest score
 * is picked to label the example.
 */
@Since("1.4.0")
final class OneVsRest @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends Estimator[OneVsRestModel] with OneVsRestParams with HasParallelism with MLWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("oneVsRest"))

  /** @group setParam */
  @Since("1.4.0")
  def setClassifier(value: Classifier[_, _, _]): this.type = {
    set(classifier, value.asInstanceOf[ClassifierType])
  }

  /** @group setParam */
  @Since("1.5.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.4.0")
  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  /**
   * The implementation of parallel one vs. rest runs the classification for
   * each class in a separate threads.
   *
   * @group expertSetParam
   */
  @Since("2.3.0")
  def setParallelism(value: Int): this.type = {
    set(parallelism, value)
  }

  /**
   * Sets the value of param [[weightCol]].
   *
   * This is ignored if weight is not supported by [[classifier]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("2.3.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, getClassifier.featuresDataType)
  }

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): OneVsRestModel = instrumented { instr =>
    transformSchema(dataset.schema)

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, weightCol, featuresCol, predictionCol,
      rawPredictionCol, parallelism)
    instr.logNamedValue("classifier", $(classifier).getClass.getCanonicalName)

    // determine number of classes either from metadata if provided, or via computation.
    val labelSchema = dataset.schema($(labelCol))
    val computeNumClasses: () => Int = () => {
      val Row(maxLabelIndex: Double) = dataset.agg(max(col($(labelCol)).cast(DoubleType))).head()
      // classes are assumed to be numbered from 0,...,maxLabelIndex
      maxLabelIndex.toInt + 1
    }
    val numClasses = MetadataUtils.getNumClasses(labelSchema).fold(computeNumClasses())(identity)
    instr.logNumClasses(numClasses)

    val weightColIsUsed = isDefined(weightCol) && $(weightCol).nonEmpty && {
      getClassifier match {
        case _: HasWeightCol => true
        case c =>
          instr.logWarning(log"weightCol is ignored, as it is not supported by " +
            log"${MDC(LogKeys.CLASSIFIER, c)} now.")
          false
      }
    }

    val multiclassLabeled = if (weightColIsUsed) {
      dataset.select($(labelCol), $(featuresCol), $(weightCol))
    } else {
      dataset.select($(labelCol), $(featuresCol))
    }

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    if (handlePersistence) {
      multiclassLabeled.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val executionContext = getExecutionContext

    // create k columns, one for each binary classifier.
    val modelFutures = Range(0, numClasses).map { index =>
      // generate new label metadata for the binary problem.
      val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
      val labelColName = "mc2b$" + index
      val trainingDataset = multiclassLabeled.withColumn(
        labelColName, when(col($(labelCol)) === index.toDouble, 1.0).otherwise(0.0), newLabelMeta)
      val classifier = getClassifier
      val paramMap = new ParamMap()
      paramMap.put(classifier.labelCol -> labelColName)
      paramMap.put(classifier.featuresCol -> getFeaturesCol)
      paramMap.put(classifier.predictionCol -> getPredictionCol)
      Future {
        if (weightColIsUsed) {
          val classifier_ = classifier.asInstanceOf[ClassifierType with HasWeightCol]
          paramMap.put(classifier_.weightCol -> getWeightCol)
          classifier_.fit(trainingDataset, paramMap)
        } else {
          classifier.fit(trainingDataset, paramMap)
        }
      }(executionContext)
    }
    val models = modelFutures
      .map(ThreadUtils.awaitResult(_, Duration.Inf)).toArray[ClassificationModel[_, _]]
    instr.logNumFeatures(models.head.numFeatures)

    if (handlePersistence) {
      multiclassLabeled.unpersist()
    }

    // extract label metadata from label column if present, or create a nominal attribute
    // to output the number of labels
    val labelAttribute = Attribute.fromStructField(labelSchema) match {
      case _: NumericAttribute | UnresolvedAttribute =>
        NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
      case attr: Attribute => attr
    }
    val model = new OneVsRestModel(uid, labelAttribute.toMetadata(), models).setParent(this)
    copyValues(model)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): OneVsRest = {
    val copied = defaultCopy(extra).asInstanceOf[OneVsRest]
    if (isDefined(classifier)) {
      copied.setClassifier($(classifier).copy(extra))
    }
    copied
  }

  @Since("2.0.0")
  override def write: MLWriter = new OneVsRest.OneVsRestWriter(this)
}

@Since("2.0.0")
object OneVsRest extends MLReadable[OneVsRest] {

  @Since("2.0.0")
  override def read: MLReader[OneVsRest] = new OneVsRestReader

  @Since("2.0.0")
  override def load(path: String): OneVsRest = super.load(path)

  /** [[MLWriter]] instance for [[OneVsRest]] */
  private[OneVsRest] class OneVsRestWriter(instance: OneVsRest) extends MLWriter {

    OneVsRestParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      OneVsRestParams.saveImpl(path, instance, sparkSession)
    }
  }

  private class OneVsRestReader extends MLReader[OneVsRest] {

    /** Checked against metadata when loading model */
    private val className = classOf[OneVsRest].getName

    override def load(path: String): OneVsRest = {
      val (metadata, classifier) = OneVsRestParams.loadImpl(path, sparkSession, className)
      val ovr = new OneVsRest(metadata.uid)
      metadata.getAndSetParams(ovr)
      ovr.setClassifier(classifier)
    }
  }
}

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

import scala.language.existentials

import org.apache.hadoop.fs.Path
import org.json4s.{DefaultFormats, JObject}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


import org.apache.spark.SparkContext
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml._
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.param.{ParamPair, Param, ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

  trait MyClassifierType {
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
private[ml] trait OneVsRestParams extends PredictorParams with MyClassifierType {


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

/**
 * :: Experimental ::
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
@Experimental
final class OneVsRestModel private[ml] (
    override val uid: String,
    val labelMetadata: Metadata,
    val models: Array[_ <: ClassificationModel[_, _]])
  extends Model[OneVsRestModel] with OneVsRestParams with MLWritable {

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false, getClassifier.featuresDataType)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    // Check schema
    transformSchema(dataset.schema, logging = true)

    // determine the input columns: these need to be passed through
    val origCols = dataset.schema.map(f => col(f.name))

    // add an accumulator column to store predictions of all the models
    val accColName = "mbc$acc" + UUID.randomUUID().toString
    val initUDF = udf { () => Map[Int, Double]() }
    val newDataset = dataset.withColumn(accColName, initUDF())

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      newDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // update the accumulator column with the result of prediction of models
    val aggregatedDataset = models.zipWithIndex.foldLeft[DataFrame](newDataset) {
      case (df, (model, index)) =>
        val rawPredictionCol = model.getRawPredictionCol
        val columns = origCols ++ List(col(rawPredictionCol), col(accColName))

        // add temporary column to store intermediate scores and update
        val tmpColName = "mbc$tmp" + UUID.randomUUID().toString
        val updateUDF = udf { (predictions: Map[Int, Double], prediction: Vector) =>
          predictions + ((index, prediction(1)))
        }
        val transformedDataset = model.transform(df).select(columns : _*)
        val updatedDataset = transformedDataset
          .withColumn(tmpColName, updateUDF(col(accColName), col(rawPredictionCol)))
        val newColumns = origCols ++ List(col(tmpColName))

        // switch out the intermediate column with the accumulator column
        updatedDataset.select(newColumns : _*).withColumnRenamed(tmpColName, accColName)
    }

    if (handlePersistence) {
      newDataset.unpersist()
    }

    // output the index of the classifier with highest confidence as prediction
    val labelUDF = udf { (predictions: Map[Int, Double]) =>
      predictions.maxBy(_._2)._1.toDouble
    }

    // output label and label metadata as prediction
    aggregatedDataset
      .withColumn($(predictionCol), labelUDF(col(accColName)), labelMetadata)
      .drop(accColName)
  }

  override def copy(extra: ParamMap): OneVsRestModel = {
    val copied = new OneVsRestModel(
      uid, labelMetadata, models.map(_.copy(extra).asInstanceOf[ClassificationModel[_, _]]))
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new OneVsRestModel.OneVsRestModelWriter(this)
}

@Since("1.6.0")
object OneVsRestModel extends MLReadable[OneVsRestModel] {

  @Since("1.6.0")
  override def read: MLReader[OneVsRestModel] = new OneVsRestModelReader

  @Since("1.6.0")
  override def load(path: String): OneVsRestModel = super.load(path)

  /** [[MLWriter]] instance for [[OneVsRestModel]] */
  private[OneVsRestModel] class OneVsRestModelWriter(instance: OneVsRestModel) extends MLWriter {

    SharedReadWrite.validateParams(instance)

    private case class Data(labelMetadata: Map[String, Any])

    override protected def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      // Save model data: labelMetadata
      val dataPath = new Path(path, "data").toString
      sc.parallelize(Seq(instance.labelMetadata.json), 1).saveAsTextFile(dataPath)
      SharedReadWrite.saveImpl(path, instance, sc, Some("numClasses" -> instance.models.length))
    }
  }

  private class OneVsRestModelReader extends MLReader[OneVsRestModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[OneVsRestModel].getName

    override def load(path: String): OneVsRestModel = {
      implicit val format = DefaultFormats
      val (metadata, _) = SharedReadWrite.load(path, sc, className)
      val numClasses = (metadata.metadata \ "numClasses").extract[Int]
      val models = Array(0, numClasses).map { idx =>
        val modelPath = new Path(path, s"model_$idx").toString
        DefaultParamsReader.loadParamsInstance[ClassificationModel[_, _]](modelPath, sc)
      }
      val dataPath = new Path(path, "data").toString
      val dataStr = sc.textFile(dataPath, 1).first()
      val labelMetadata = Metadata.fromJson(dataStr)
      new OneVsRestModel(metadata.uid, labelMetadata, models)
    }
  }
}

/**
 * :: Experimental ::
 *
 * Reduction of Multiclass Classification to Binary Classification.
 * Performs reduction using one against all strategy.
 * For a multiclass classification with k classes, train k models (one per class).
 * Each example is scored against all k models and the model with highest score
 * is picked to label the example.
 */
@Experimental
final class OneVsRest(override val uid: String)
  extends Estimator[OneVsRestModel] with OneVsRestParams with MLWritable {

  def this() = this(Identifiable.randomUID("oneVsRest"))

  /** @group setParam */
  def setClassifier(value: Classifier[_, _, _]): this.type = {
    set(classifier, value.asInstanceOf[ClassifierType])
  }

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, getClassifier.featuresDataType)
  }

  override def fit(dataset: DataFrame): OneVsRestModel = {
    // determine number of classes either from metadata if provided, or via computation.
    val labelSchema = dataset.schema($(labelCol))
    val computeNumClasses: () => Int = () => {
      val Row(maxLabelIndex: Double) = dataset.agg(max($(labelCol))).head()
      // classes are assumed to be numbered from 0,...,maxLabelIndex
      maxLabelIndex.toInt + 1
    }
    val numClasses = MetadataUtils.getNumClasses(labelSchema).fold(computeNumClasses())(identity)

    val multiclassLabeled = dataset.select($(labelCol), $(featuresCol))

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      multiclassLabeled.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // create k columns, one for each binary classifier.
    val models = Range(0, numClasses).par.map { index =>
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
      classifier.fit(trainingDataset, paramMap)
    }.toArray[ClassificationModel[_, _]]

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

  override def copy(extra: ParamMap): OneVsRest = {
    val copied = defaultCopy(extra).asInstanceOf[OneVsRest]
    if (isDefined(classifier)) {
      copied.setClassifier($(classifier).copy(extra))
    }
    copied
  }

  @Since("1.6.0")
  override def write: MLWriter = new OneVsRest.OneVsRestWriter(this)
}

private[classification] object SharedReadWrite extends MyClassifierType {

  def validateParams(instance: OneVsRestParams): Unit = {
    def checkElement(elem: Params, name: String): Unit = elem match {
      case stage: MLWritable => // good
      case other =>
        throw new UnsupportedOperationException("CrossValidator write will fail " +
          s" because it contains $name which does not implement Writable." +
          s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
    }

    instance match {
      case ovrModel: OneVsRestModel => ovrModel.models.foreach(checkElement(_, "model"))
      case _ => // check nothing
    }

    checkElement(instance.getClassifier, "classifier")
  }

  private[classification] def saveImpl(
      path: String,
      instance: OneVsRestParams,
      sc: SparkContext,
      extraMetadata: Option[JObject] = None): Unit = {

    val params = instance.extractParamMap().toSeq.asInstanceOf[Seq[ParamPair[Any]]]
    val jsonParams = render(params
      .filter { case ParamPair(p, v) => p.name != "classifier" }
      .map { case ParamPair(p, v) => p.name -> parse(p.jsonEncode(v)) }
      .toList)

    DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))

    val classifierPath = new Path(path, "metadata_classifier").toString
    instance.getClassifier.asInstanceOf[MLWritable].save(classifierPath)

    instance match {
      case ovrModel: OneVsRestModel =>
        ovrModel.asInstanceOf[OneVsRestModel].models.zipWithIndex.foreach {
          case (model: MLWritable, idx) =>
            val modelPath = new Path(path, s"model_$idx").toString
            model.save(modelPath)
          case _ => // do nothing here since we have already checked the models
        }
      case _ =>
    }
  }

  private[classification] def load(
      path: String,
      sc: SparkContext,
      expectedClassName: String): (DefaultParamsReader.Metadata, ClassifierType) = {

    val metadata = DefaultParamsReader.loadMetadata(path, sc, expectedClassName)

    val classifierPath = new Path(path, "metadata_classifier").toString
    val estimator = DefaultParamsReader.loadParamsInstance[ClassifierType](classifierPath, sc)

    (metadata, estimator)
  }
}

@Since("1.6.0")
object OneVsRest extends MLReadable[OneVsRest] {

  @Since("1.6.0")
  override def read: MLReader[OneVsRest] = new OneVsRestReader

  @Since("1.6.0")
  override def load(path: String): OneVsRest = super.load(path)

  /** [[MLWriter]] instance for [[OneVsRest]] */
  private[OneVsRest] class OneVsRestWriter(instance: OneVsRest) extends MLWriter {

    SharedReadWrite.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      SharedReadWrite.saveImpl(path, instance, sc)
    }
  }
  private class OneVsRestReader extends MLReader[OneVsRest] {

    /** Checked against metadata when loading model */
    private val className = classOf[OneVsRest].getName

    override def load(path: String): OneVsRest = {
      val (metadata, classifier) = SharedReadWrite.load(path, sc, className)
      val ova = new OneVsRest(metadata.uid)
      DefaultParamsReader.getAndSetParams(ova, metadata)
      ova.setClassifier(classifier)
    }
  }
}

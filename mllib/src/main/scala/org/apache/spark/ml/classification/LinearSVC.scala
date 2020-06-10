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

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, OWLQN => BreezeOWLQN}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.optim.aggregator._
import org.apache.spark.ml.optim.loss.{L2Regularization, RDDLossFunction}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.storage.StorageLevel

/** Params for linear SVM Classifier. */
private[classification] trait LinearSVCParams extends ClassifierParams with HasRegParam
  with HasMaxIter with HasFitIntercept with HasTol with HasStandardization with HasWeightCol
  with HasAggregationDepth with HasThreshold with HasBlockSize {

  /**
   * Param for threshold in binary classification prediction.
   * For LinearSVC, this threshold is applied to the rawPrediction, rather than a probability.
   * This threshold can be any real number, where Inf will make all predictions 0.0
   * and -Inf will make all predictions 1.0.
   * Default: 0.0
   *
   * @group param
   */
  final override val threshold: DoubleParam = new DoubleParam(this, "threshold",
    "threshold in binary classification prediction applied to rawPrediction")
}

/**
 * <a href = "https://en.wikipedia.org/wiki/Support_vector_machine#Linear_SVM">
 *   Linear SVM Classifier</a>
 *
 * This binary classifier optimizes the Hinge Loss using the OWLQN optimizer.
 * Only supports L2 regularization currently.
 *
 */
@Since("2.2.0")
class LinearSVC @Since("2.2.0") (
    @Since("2.2.0") override val uid: String)
  extends Classifier[Vector, LinearSVC, LinearSVCModel]
  with LinearSVCParams with DefaultParamsWritable {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("linearsvc"))

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Whether to fit an intercept term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller values will lead to higher accuracy at the cost of more iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Whether to standardize the training features before fitting the model.
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setStandardization(value: Boolean): this.type = set(standardization, value)
  setDefault(standardization -> true)

  /**
   * Set the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Set threshold in binary classification.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setThreshold(value: Double): this.type = set(threshold, value)
  setDefault(threshold -> 0.0)

  /**
   * Suggested depth for treeAggregate (greater than or equal to 2).
   * If the dimensions of features or the number of partitions are large,
   * this param could be adjusted to a larger size.
   * Default is 2.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)
  setDefault(aggregationDepth -> 2)

  /**
   * Set block size for stacking input data in matrices.
   * If blockSize == 1, then stacking will be skipped, and each vector is treated individually;
   * If blockSize &gt; 1, then vectors will be stacked to blocks, and high-level BLAS routines
   * will be used if possible (for example, GEMV instead of DOT, GEMM instead of GEMV).
   * Recommended size is between 10 and 1000. An appropriate choice of the block size depends
   * on the sparsity and dim of input datasets, the underlying BLAS implementation (for example,
   * f2jBLAS, OpenBLAS, intel MKL) and its configuration (for example, number of threads).
   * Note that existing BLAS implementations are mainly optimized for dense matrices, if the
   * input dataset is sparse, stacking may bring no performance gain, the worse is possible
   * performance regression.
   * Default is 1.
   *
   * @group expertSetParam
   */
  @Since("3.1.0")
  def setBlockSize(value: Int): this.type = set(blockSize, value)
  setDefault(blockSize -> 1)

  @Since("2.2.0")
  override def copy(extra: ParamMap): LinearSVC = defaultCopy(extra)

  override protected def train(dataset: Dataset[_]): LinearSVCModel = instrumented { instr =>
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, weightCol, featuresCol, predictionCol, rawPredictionCol,
      regParam, maxIter, fitIntercept, tol, standardization, threshold, aggregationDepth, blockSize)

    val instances = extractInstances(dataset)
      .setName("training instances")

    if (dataset.storageLevel == StorageLevel.NONE && $(blockSize) == 1) {
      instances.persist(StorageLevel.MEMORY_AND_DISK)
    }

    var requestedMetrics = Seq("mean", "std", "count")
    if ($(blockSize) != 1) requestedMetrics +:= "numNonZeros"
    val (summarizer, labelSummarizer) = Summarizer
      .getClassificationSummarizers(instances, $(aggregationDepth), requestedMetrics)

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numFeatures = summarizer.mean.size

    instr.logNumExamples(summarizer.count)
    instr.logNamedValue("lowestLabelWeight", labelSummarizer.histogram.min.toString)
    instr.logNamedValue("highestLabelWeight", labelSummarizer.histogram.max.toString)
    instr.logSumOfWeights(summarizer.weightSum)
    if ($(blockSize) > 1) {
      val scale = 1.0 / summarizer.count / numFeatures
      val sparsity = 1 - summarizer.numNonzeros.toArray.map(_ * scale).sum
      instr.logNamedValue("sparsity", sparsity.toString)
      if (sparsity > 0.5) {
        instr.logWarning(s"sparsity of input dataset is $sparsity, " +
          s"which may hurt performance in high-level BLAS.")
      }
    }

    val numClasses = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) =>
        require(n >= histogram.length, s"Specified number of classes $n was " +
          s"less than the number of unique labels ${histogram.length}.")
        n
      case None => histogram.length
    }
    require(numClasses == 2, s"LinearSVC only supports binary classification." +
      s" $numClasses classes detected in $labelCol")
    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)

    if (numInvalid != 0) {
      val msg = s"Classification labels should be in [0 to ${numClasses - 1}]. " +
        s"Found $numInvalid invalid labels."
      instr.logError(msg)
      throw new SparkException(msg)
    }

    val featuresStd = summarizer.std.toArray
    val getFeaturesStd = (j: Int) => featuresStd(j)
    val regularization = if ($(regParam) != 0.0) {
      val shouldApply = (idx: Int) => idx >= 0 && idx < numFeatures
      Some(new L2Regularization($(regParam), shouldApply,
        if ($(standardization)) None else Some(getFeaturesStd)))
    } else None

    def regParamL1Fun = (index: Int) => 0.0
    val optimizer = new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))

    /*
       The coefficients are trained in the scaled space; we're converting them back to
       the original space.
       Note that the intercept in scaled space and original space is the same;
       as a result, no scaling is needed.
     */
    val (rawCoefficients, objectiveHistory) = if ($(blockSize) == 1) {
      trainOnRows(instances, featuresStd, regularization, optimizer)
    } else {
      trainOnBlocks(instances, featuresStd, regularization, optimizer)
    }
    if (instances.getStorageLevel != StorageLevel.NONE) instances.unpersist()

    if (rawCoefficients == null) {
      val msg = s"${optimizer.getClass.getName} failed."
      instr.logError(msg)
      throw new SparkException(msg)
    }

    val coefficientArray = Array.tabulate(numFeatures) { i =>
      if (featuresStd(i) != 0.0) rawCoefficients(i) / featuresStd(i) else 0.0
    }
    val intercept = if ($(fitIntercept)) rawCoefficients.last else 0.0
    copyValues(new LinearSVCModel(uid, Vectors.dense(coefficientArray), intercept))
  }

  private def trainOnRows(
      instances: RDD[Instance],
      featuresStd: Array[Double],
      regularization: Option[L2Regularization],
      optimizer: BreezeOWLQN[Int, BDV[Double]]): (Array[Double], Array[Double]) = {
    val numFeatures = featuresStd.length
    val numFeaturesPlusIntercept = if ($(fitIntercept)) numFeatures + 1 else numFeatures

    val bcFeaturesStd = instances.context.broadcast(featuresStd)
    val getAggregatorFunc = new HingeAggregator(bcFeaturesStd, $(fitIntercept))(_)
    val costFun = new RDDLossFunction(instances, getAggregatorFunc,
      regularization, $(aggregationDepth))

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      Vectors.zeros(numFeaturesPlusIntercept).asBreeze.toDenseVector)

    val arrayBuilder = mutable.ArrayBuilder.make[Double]
    var state: optimizer.State = null
    while (states.hasNext) {
      state = states.next()
      arrayBuilder += state.adjustedValue
    }
    bcFeaturesStd.destroy()

    (if (state != null) state.x.toArray else null, arrayBuilder.result)
  }

  private def trainOnBlocks(
      instances: RDD[Instance],
      featuresStd: Array[Double],
      regularization: Option[L2Regularization],
      optimizer: BreezeOWLQN[Int, BDV[Double]]): (Array[Double], Array[Double]) = {
    val numFeatures = featuresStd.length
    val numFeaturesPlusIntercept = if ($(fitIntercept)) numFeatures + 1 else numFeatures

    val bcFeaturesStd = instances.context.broadcast(featuresStd)

    val standardized = instances.mapPartitions { iter =>
      val inverseStd = bcFeaturesStd.value.map { std => if (std != 0) 1.0 / std else 0.0 }
      val func = StandardScalerModel.getTransformFunc(Array.empty, inverseStd, false, true)
      iter.map { case Instance(label, weight, vec) => Instance(label, weight, func(vec)) }
    }
    val blocks = InstanceBlock.blokify(standardized, $(blockSize))
      .persist(StorageLevel.MEMORY_AND_DISK)
      .setName(s"training blocks (blockSize=${$(blockSize)})")

    val getAggregatorFunc = new BlockHingeAggregator($(fitIntercept))(_)
    val costFun = new RDDLossFunction(blocks, getAggregatorFunc,
      regularization, $(aggregationDepth))

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      Vectors.zeros(numFeaturesPlusIntercept).asBreeze.toDenseVector)

    val arrayBuilder = mutable.ArrayBuilder.make[Double]
    var state: optimizer.State = null
    while (states.hasNext) {
      state = states.next()
      arrayBuilder += state.adjustedValue
    }
    blocks.unpersist()
    bcFeaturesStd.destroy()

    (if (state != null) state.x.toArray else null, arrayBuilder.result)
  }
}

@Since("2.2.0")
object LinearSVC extends DefaultParamsReadable[LinearSVC] {

  @Since("2.2.0")
  override def load(path: String): LinearSVC = super.load(path)
}

/**
 * Linear SVM Model trained by [[LinearSVC]]
 */
@Since("2.2.0")
class LinearSVCModel private[classification] (
    @Since("2.2.0") override val uid: String,
    @Since("2.2.0") val coefficients: Vector,
    @Since("2.2.0") val intercept: Double)
  extends ClassificationModel[Vector, LinearSVCModel]
  with LinearSVCParams with MLWritable {

  @Since("2.2.0")
  override val numClasses: Int = 2

  @Since("2.2.0")
  override val numFeatures: Int = coefficients.size

  @Since("2.2.0")
  def setThreshold(value: Double): this.type = set(threshold, value)
  setDefault(threshold, 0.0)

  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, coefficients) + intercept
  }

  override def predict(features: Vector): Double = {
    if (margin(features) > $(threshold)) 1.0 else 0.0
  }

  @Since("3.0.0")
  override def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }

  override protected def raw2prediction(rawPrediction: Vector): Double = {
    if (rawPrediction(1) > $(threshold)) 1.0 else 0.0
  }

  @Since("2.2.0")
  override def copy(extra: ParamMap): LinearSVCModel = {
    copyValues(new LinearSVCModel(uid, coefficients, intercept), extra).setParent(parent)
  }

  @Since("2.2.0")
  override def write: MLWriter = new LinearSVCModel.LinearSVCWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"LinearSVCModel: uid=$uid, numClasses=$numClasses, numFeatures=$numFeatures"
  }
}


@Since("2.2.0")
object LinearSVCModel extends MLReadable[LinearSVCModel] {

  @Since("2.2.0")
  override def read: MLReader[LinearSVCModel] = new LinearSVCReader

  @Since("2.2.0")
  override def load(path: String): LinearSVCModel = super.load(path)

  /** [[MLWriter]] instance for [[LinearSVCModel]] */
  private[LinearSVCModel]
  class LinearSVCWriter(instance: LinearSVCModel)
    extends MLWriter with Logging {

    private case class Data(coefficients: Vector, intercept: Double)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.coefficients, instance.intercept)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class LinearSVCReader extends MLReader[LinearSVCModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[LinearSVCModel].getName

    override def load(path: String): LinearSVCModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(coefficients: Vector, intercept: Double) =
        data.select("coefficients", "intercept").head()
      val model = new LinearSVCModel(metadata.uid, coefficients, intercept)
      metadata.getAndSetParams(model)
      model
    }
  }
}

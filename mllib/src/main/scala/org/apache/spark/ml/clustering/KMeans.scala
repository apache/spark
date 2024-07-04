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

package org.apache.spark.ml.clustering

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.internal.LogKeys.{COST, INIT_MODE, NUM_ITERATIONS, TOTAL_TIME}
import org.apache.spark.internal.MDC
import org.apache.spark.ml.{Estimator, Model, PipelineStage}
import org.apache.spark.ml.feature.{Instance, InstanceBlock}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DatasetUtils._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans, KMeansModel => MLlibKMeansModel}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.VersionUtils.majorVersion

/**
 * Common params for KMeans and KMeansModel
 */
private[clustering] trait KMeansParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasTol with HasDistanceMeasure with HasWeightCol
  with HasSolver with HasMaxBlockSizeInMB {
  import KMeans._

  /**
   * The number of clusters to create (k). Must be &gt; 1. Note that it is possible for fewer than
   * k clusters to be returned, for example, if there are fewer than k distinct points to cluster.
   * Default: 2.
   * @group param
   */
  @Since("1.5.0")
  final val k = new IntParam(this, "k", "The number of clusters to create. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("1.5.0")
  def getK: Int = $(k)

  /**
   * Param for the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   * @group expertParam
   */
  @Since("1.5.0")
  final val initMode = new Param[String](this, "initMode", "The initialization algorithm. " +
    "Supported options: 'random' and 'k-means||'.",
    ParamValidators.inArray[String](supportedInitModes))

  /** @group expertGetParam */
  @Since("1.5.0")
  def getInitMode: String = $(initMode)

  /**
   * Param for the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 2 is almost always enough. Must be &gt; 0. Default: 2.
   * @group expertParam
   */
  @Since("1.5.0")
  final val initSteps = new IntParam(this, "initSteps", "The number of steps for k-means|| " +
    "initialization mode. Must be > 0.", ParamValidators.gt(0))

  /** @group expertGetParam */
  @Since("1.5.0")
  def getInitSteps: Int = $(initSteps)

  /**
   * Param for the name of optimization method used in KMeans.
   * Supported options:
   *  - "auto": Automatically select the solver based on the input schema and sparsity:
   *            If input instances are arrays or input vectors are dense, set to "block".
   *            Else, set to "row".
   *  - "row": input instances are processed row by row, and triangle-inequality is applied to
   *           accelerate the training.
   *  - "block": input instances are stacked to blocks, and GEMM is applied to compute the
   *             distances.
   * Default is "auto".
   *
   * @group expertParam
   */
  @Since("3.4.0")
  final override val solver: Param[String] = new Param[String](this, "solver",
    "The solver algorithm for optimization. Supported options: " +
      s"${supportedSolvers.mkString(", ")}. (Default auto)",
    ParamValidators.inArray[String](supportedSolvers))

  setDefault(k -> 2, maxIter -> 20, initMode -> K_MEANS_PARALLEL, initSteps -> 2,
    tol -> 1e-4, distanceMeasure -> EUCLIDEAN, solver -> AUTO, maxBlockSizeInMB -> 0.0)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.validateVectorCompatibleColumn(schema, getFeaturesCol)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
  }
}

/**
 * Model fitted by KMeans.
 *
 * @param parentModel a model trained by spark.mllib.clustering.KMeans.
 */
@Since("1.5.0")
class KMeansModel private[ml] (
    @Since("1.5.0") override val uid: String,
    private[clustering] val parentModel: MLlibKMeansModel)
  extends Model[KMeansModel] with KMeansParams with GeneralMLWritable
    with HasTrainingSummary[KMeansSummary] {

  @Since("3.0.0")
  lazy val numFeatures: Int = parentModel.clusterCenters.head.size

  @Since("1.5.0")
  override def copy(extra: ParamMap): KMeansModel = {
    val copied = copyValues(new KMeansModel(uid, parentModel), extra)
    copied.setSummary(trainingSummary).setParent(this.parent)
  }

  /** @group setParam */
  @Since("2.0.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val predictUDF = udf((vector: Vector) => predict(vector))

    dataset.withColumn($(predictionCol),
      predictUDF(columnToVector(dataset, getFeaturesCol)),
      outputSchema($(predictionCol)).metadata)
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(predictionCol).nonEmpty) {
      outputSchema = SchemaUtils.updateNumValues(outputSchema,
        $(predictionCol), parentModel.k)
    }
    outputSchema
  }

  @Since("3.0.0")
  def predict(features: Vector): Int = parentModel.predict(OldVectors.fromML(features))

  @Since("2.0.0")
  def clusterCenters: Array[Vector] = parentModel.clusterCenters.map(_.asML)

  /**
   * Returns a [[org.apache.spark.ml.util.GeneralMLWriter]] instance for this ML instance.
   *
   * For [[KMeansModel]], this does NOT currently save the training [[summary]].
   * An option to save [[summary]] may be added in the future.
   *
   */
  @Since("1.6.0")
  override def write: GeneralMLWriter = new GeneralMLWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"KMeansModel: uid=$uid, k=${parentModel.k}, distanceMeasure=${$(distanceMeasure)}, " +
      s"numFeatures=$numFeatures"
  }

  /**
   * Gets summary of model on training set. An exception is
   * thrown if `hasSummary` is false.
   */
  @Since("2.0.0")
  override def summary: KMeansSummary = super.summary
}

/** Helper class for storing model data */
private case class ClusterData(clusterIdx: Int, clusterCenter: Vector)


/** A writer for KMeans that handles the "internal" (or default) format */
private class InternalKMeansModelWriter extends MLWriterFormat with MLFormatRegister {

  override def format(): String = "internal"
  override def stageName(): String = "org.apache.spark.ml.clustering.KMeansModel"

  override def write(path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage): Unit = {
    val instance = stage.asInstanceOf[KMeansModel]
    val sc = sparkSession.sparkContext
    // Save metadata and Params
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    // Save model data: cluster centers
    val data: Array[ClusterData] = instance.clusterCenters.zipWithIndex.map {
      case (center, idx) =>
        ClusterData(idx, center)
    }
    val dataPath = new Path(path, "data").toString
    sparkSession.createDataFrame(data.toImmutableArraySeq).repartition(1).write.parquet(dataPath)
  }
}

/** A writer for KMeans that handles the "pmml" format */
private class PMMLKMeansModelWriter extends MLWriterFormat with MLFormatRegister {

  override def format(): String = "pmml"
  override def stageName(): String = "org.apache.spark.ml.clustering.KMeansModel"

  override def write(path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage): Unit = {
    val instance = stage.asInstanceOf[KMeansModel]
    val sc = sparkSession.sparkContext
    instance.parentModel.toPMML(sc, path)
  }
}


@Since("1.6.0")
object KMeansModel extends MLReadable[KMeansModel] {

  @Since("1.6.0")
  override def read: MLReader[KMeansModel] = new KMeansModelReader

  @Since("1.6.0")
  override def load(path: String): KMeansModel = super.load(path)

  /**
   * We store all cluster centers in a single row and use this class to store model data by
   * Spark 1.6 and earlier. A model can be loaded from such older data for backward compatibility.
   */
  private case class OldData(clusterCenters: Array[OldVector])

  private class KMeansModelReader extends MLReader[KMeansModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[KMeansModel].getName

    override def load(path: String): KMeansModel = {
      // Import implicits for Dataset Encoder
      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString

      val clusterCenters = if (majorVersion(metadata.sparkVersion) >= 2) {
        val data: Dataset[ClusterData] = sparkSession.read.parquet(dataPath).as[ClusterData]
        data.collect().sortBy(_.clusterIdx).map(_.clusterCenter).map(OldVectors.fromML)
      } else {
        // Loads KMeansModel stored with the old format used by Spark 1.6 and earlier.
        sparkSession.read.parquet(dataPath).as[OldData].head().clusterCenters
      }
      val model = new KMeansModel(metadata.uid, new MLlibKMeansModel(clusterCenters))
      metadata.getAndSetParams(model)
      model
    }
  }
}

/**
 * K-means clustering with support for k-means|| initialization proposed by Bahmani et al.
 *
 * @see <a href="https://doi.org/10.14778/2180912.2180915">Bahmani et al., Scalable k-means++.</a>
 */
@Since("1.5.0")
class KMeans @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends Estimator[KMeansModel] with KMeansParams with DefaultParamsWritable {
  import KMeans._

  @Since("1.5.0")
  override def copy(extra: ParamMap): KMeans = defaultCopy(extra)

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("kmeans"))

  /** @group setParam */
  @Since("1.5.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  @Since("1.5.0")
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group expertSetParam */
  @Since("2.4.0")
  def setDistanceMeasure(value: String): this.type = set(distanceMeasure, value)

  /** @group expertSetParam */
  @Since("1.5.0")
  def setInitSteps(value: Int): this.type = set(initSteps, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.5.0")
  def setTol(value: Double): this.type = set(tol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Sets the value of param [[solver]].
   * Default is "auto".
   *
   * @group expertSetParam
   */
  @Since("3.4.0")
  def setSolver(value: String): this.type = set(solver, value)

  /**
   * Sets the value of param [[maxBlockSizeInMB]].
   * Default is 0.0, then 1.0 MB will be chosen.
   *
   * @group expertSetParam
   */
  @Since("3.4.0")
  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): KMeansModel = instrumented { instr =>
    transformSchema(dataset.schema, logging = true)

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, featuresCol, predictionCol, k, initMode, initSteps, distanceMeasure,
      maxIter, seed, tol, weightCol, solver, maxBlockSizeInMB)

    val oldModel = if (preferBlockSolver(dataset)) {
      trainWithBlock(dataset, instr)
    } else {
      trainWithRow(dataset, instr)
    }

    val model = copyValues(new KMeansModel(uid, oldModel).setParent(this))
    val summary = new KMeansSummary(
      model.transform(dataset),
      $(predictionCol),
      $(featuresCol),
      $(k),
      oldModel.numIter,
      oldModel.trainingCost)

    model.setSummary(Some(summary))
    instr.logNamedValue("clusterSizes", summary.clusterSizes)
    model
  }

  private def preferBlockSolver(dataset: Dataset[_]): Boolean = {
    $(solver) match {
      case ROW => false
      case BLOCK => true
      case AUTO =>
        dataset.schema($(featuresCol)).dataType match {
          case _: VectorUDT =>

            val Row(count: Long, numNonzeros: Vector) = dataset
              .select(Summarizer.metrics("count", "numNonZeros")
                .summary(checkNonNanVectors(col($(featuresCol)))).as("summary"))
              .select("summary.count", "summary.numNonZeros")
              .first()
            val numFeatures = numNonzeros.size
            val nnz = numNonzeros.activeIterator.map(_._1).foldLeft(BigDecimal(0))(_ + _)
            nnz >= BigDecimal(count) * numFeatures * 0.5

          case fdt: ArrayType =>
            // when input schema is array, the dataset should be dense
            fdt.elementType match {
              case _: FloatType => true
              case _: DoubleType => true
              case _ => false
            }

          case _ => false
        }
    }
  }

  private def trainWithRow(dataset: Dataset[_], instr: Instrumentation) = {
    val algo = new MLlibKMeans()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setInitializationSteps($(initSteps))
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setEpsilon($(tol))
      .setDistanceMeasure($(distanceMeasure))

    val instances = dataset.select(
      checkNonNanVectors(columnToVector(dataset, $(featuresCol))),
      checkNonNegativeWeights(get(weightCol))
    ).rdd.map { case Row(f: Vector, w: Double) => (OldVectors.fromML(f), w)
    }.setName("training instances")

    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    algo.runWithWeight(instances, handlePersistence, Some(instr))
  }

  private def trainWithBlock(dataset: Dataset[_], instr: Instrumentation) = {
    if (dataset.storageLevel != StorageLevel.NONE) {
      instr.logWarning("Input vectors will be blockified to blocks, and " +
        "then cached during training. Be careful of double caching!")
    }

    val initStartTime = System.currentTimeMillis
    val centers = initialize(dataset)
    val initTimeMs = System.currentTimeMillis - initStartTime
    instr.logInfo(log"Initialization with ${MDC(INIT_MODE, $(initMode))} took " +
      log"${MDC(TOTAL_TIME, initTimeMs)} ms.")

    val numFeatures = centers.head.size
    instr.logNumFeatures(numFeatures)

    val instances = $(distanceMeasure) match {
      case EUCLIDEAN =>
        dataset.select(
          checkNonNanVectors(columnToVector(dataset, $(featuresCol))),
          checkNonNegativeWeights(get(weightCol))
        ).rdd.map { case Row(features: Vector, weight: Double) =>
          Instance(BLAS.dot(features, features), weight, features)
        }

      case COSINE =>
        dataset.select(
          checkNonNanVectors(columnToVector(dataset, $(featuresCol))),
          checkNonNegativeWeights(get(weightCol))
        ).rdd.map { case Row(features: Vector, weight: Double) =>
          Instance(1.0, weight, Vectors.normalize(features, 2))
        }
    }

    var actualBlockSizeInMB = $(maxBlockSizeInMB)
    if (actualBlockSizeInMB == 0) {
      actualBlockSizeInMB = InstanceBlock.DefaultBlockSizeInMB
      require(actualBlockSizeInMB > 0, "inferred actual BlockSizeInMB must > 0")
      instr.logNamedValue("actualBlockSizeInMB", actualBlockSizeInMB.toString)
    }
    val maxMemUsage = (actualBlockSizeInMB * 1024L * 1024L).ceil.toLong
    val blocks = InstanceBlock.blokifyWithMaxMemUsage(instances, maxMemUsage)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .setName(s"$uid: training blocks (blockSizeInMB=$actualBlockSizeInMB)")

    val distanceFunction = getDistanceFunction
    val sc = dataset.sparkSession.sparkContext
    val iterationStartTime = System.currentTimeMillis
    var converged = false
    var cost = 0.0
    var iteration = 0

    // Execute iterations of Lloyd's algorithm until converged
    while (iteration < $(maxIter) && !converged) {
      // Find the new centers
      val bcCenters = sc.broadcast(DenseMatrix.fromVectors(centers.toImmutableArraySeq))
      val countSumAccum = if (iteration == 0) sc.longAccumulator else null
      val weightSumAccum = if (iteration == 0) sc.doubleAccumulator else null
      val costSumAccum = sc.doubleAccumulator

      val newCenters = blocks.mapPartitions { iter =>
        if (iter.nonEmpty) {
          val agg = new KMeansAggregator(bcCenters.value, $(k), numFeatures, $(distanceMeasure))
          iter.foreach(agg.add)
          if (iteration == 0) {
            countSumAccum.add(agg.count)
            weightSumAccum.add(agg.weightSum)
          }
          costSumAccum.add(agg.costSum)
          agg.weightSumVec.iterator.zip(agg.sumMat.rowIter)
            .flatMap { case ((i, weightSum), vectorSum) =>
              if (weightSum > 0) Some((i, (weightSum, vectorSum.toDense))) else None
            }
        } else Iterator.empty
      }.reduceByKey { (sum1, sum2) =>
        BLAS.axpy(1.0, sum2._2, sum1._2)
        (sum1._1 + sum2._1, sum1._2)
      }.mapValues { case (weightSum, vectorSum) =>
        BLAS.scal(1.0 / weightSum, vectorSum)
        $(distanceMeasure) match {
          case COSINE => Vectors.normalize(vectorSum, 2)
          case _ => vectorSum
        }
      }.collectAsMap()
      bcCenters.destroy()

      if (iteration == 0) {
        instr.logNumExamples(countSumAccum.value)
        instr.logSumOfWeights(weightSumAccum.value)
      }

      // Update the cluster centers and costs
      converged = true
      newCenters.foreach { case (i, newCenter) =>
        if (converged && distanceFunction(centers(i), newCenter) > $(tol)) {
          converged = false
        }
        centers(i) = newCenter
      }
      cost = costSumAccum.value
      iteration += 1
    }
    blocks.unpersist()

    val iterationTimeMs = System.currentTimeMillis - iterationStartTime
    instr.logInfo(log"Iterations took ${MDC(TOTAL_TIME, iterationTimeMs)} ms.")

    if (iteration == $(maxIter)) {
      instr.logInfo(log"KMeans reached the max number of iterations: " +
        log"${MDC(NUM_ITERATIONS, $(maxIter))}.")
    } else {
      instr.logInfo(log"KMeans converged in ${MDC(NUM_ITERATIONS, iteration)} iterations.")
    }
    instr.logInfo(log"The cost is ${MDC(COST, cost)}.")
    new MLlibKMeansModel(centers.map(OldVectors.fromML), $(distanceMeasure), cost, iteration)
  }

  private def getDistanceFunction = $(distanceMeasure) match {
    case EUCLIDEAN =>
      (v1: Vector, v2: Vector) =>
        math.sqrt(Vectors.sqdist(v1, v2))
    case COSINE =>
      (v1: Vector, v2: Vector) =>
        val norm1 = Vectors.norm(v1, 2)
        val norm2 = Vectors.norm(v2, 2)
        require(norm1 > 0 && norm2 > 0,
          "Cosine distance is not defined for zero-length vectors.")
        1 - BLAS.dot(v1, v2) / norm1 / norm2
  }

  private def initialize(dataset: Dataset[_]): Array[Vector] = {
    val algo = new MLlibKMeans()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setInitializationSteps($(initSteps))
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setEpsilon($(tol))
      .setDistanceMeasure($(distanceMeasure))

    val vectors = dataset.select(DatasetUtils.columnToVector(dataset, getFeaturesCol))
      .rdd
      .map { case Row(features: Vector) => OldVectors.fromML(features) }

    val centers = algo.initialize(vectors).map(_.asML)
    $(distanceMeasure) match {
      case EUCLIDEAN => centers
      case COSINE => centers.map(Vectors.normalize(_, 2))
    }
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

@Since("1.6.0")
object KMeans extends DefaultParamsReadable[KMeans] {

  @Since("1.6.0")
  override def load(path: String): KMeans = super.load(path)

  /** String name for random mode type. */
  private[clustering] val RANDOM = "random"

  /** String name for k-means|| mode type. */
  private[clustering] val K_MEANS_PARALLEL = "k-means||"

  private[clustering] val supportedInitModes = Array(RANDOM, K_MEANS_PARALLEL)

  /** String name for euclidean distance. */
  private[clustering] val EUCLIDEAN = "euclidean"

  /** String name for cosine distance. */
  private[clustering] val COSINE = "cosine"

  /** String name for optimizer based on triangle-inequality. */
  private[clustering] val ROW = "row"

  /** String name for optimizer based on blockifying and gemm. */
  private[clustering] val BLOCK = "block"

  /** String name for optimizer automatically chosen based on data schema and sparsity. */
  private[clustering] val AUTO = "auto"

  private[clustering] val supportedSolvers = Array(ROW, BLOCK, AUTO)
}

/**
 * Summary of KMeans.
 *
 * @param predictions  `DataFrame` produced by `KMeansModel.transform()`.
 * @param predictionCol  Name for column of predicted clusters in `predictions`.
 * @param featuresCol  Name for column of features in `predictions`.
 * @param k  Number of clusters.
 * @param numIter  Number of iterations.
 * @param trainingCost K-means cost (sum of squared distances to the nearest centroid for all
 *                     points in the training dataset). This is equivalent to sklearn's inertia.
 */
@Since("2.0.0")
class KMeansSummary private[clustering] (
    predictions: DataFrame,
    predictionCol: String,
    featuresCol: String,
    k: Int,
    numIter: Int,
    @Since("2.4.0") val trainingCost: Double)
  extends ClusteringSummary(predictions, predictionCol, featuresCol, k, numIter)

/**
 * KMeansAggregator computes the distances and updates the centers for blocks
 * in sparse or dense matrix in an online fashion.
 * @param centerMatrix The matrix containing center vectors.
 * @param k The number of clusters.
 * @param numFeatures The number of features.
 * @param distanceMeasure The distance measure.
 *                        When 'euclidean' is chosen, the instance blocks should contains
 *                        the squared norms in the labels field;
 *                        When 'cosine' is chosen, the vectors should be already normalized.
 */
private class KMeansAggregator (
    val centerMatrix: DenseMatrix,
    val k: Int,
    val numFeatures: Int,
    val distanceMeasure: String) extends Serializable {
  import KMeans.{EUCLIDEAN, COSINE}

  def weightSum: Double = weightSumVec.values.sum

  var costSum = 0.0
  var count = 0L
  val weightSumVec = new DenseVector(Array.ofDim[Double](k))
  val sumMat = new DenseMatrix(k, numFeatures, Array.ofDim[Double](k * numFeatures))

  @transient private lazy val centerSquaredNorms = {
    distanceMeasure match {
      case EUCLIDEAN =>
        centerMatrix.rowIter.map(center => center.dot(center)).toArray
      case COSINE => null
    }
  }

  // avoid reallocating a dense matrix (size x k) for each instance block
  @transient private var buffer: Array[Double] = _

  def add(block: InstanceBlock): this.type = {
    val size = block.size
    require(block.matrix.isTransposed)
    require(numFeatures == block.numFeatures, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${block.numFeatures}.")
    require(block.weightIter.forall(_ >= 0),
      s"instance weights ${block.weightIter.mkString("[", ",", "]")} has to be >= 0.0")
    if (block.weightIter.forall(_ == 0)) return this

    if (buffer == null || buffer.length < size * k) {
      buffer = Array.ofDim[Double](size * k)
    }

    distanceMeasure match {
      case EUCLIDEAN => euclideanUpdateInPlace(block)
      case COSINE => cosineUpdateInPlace(block)
    }
    count += size

    this
  }

  private def euclideanUpdateInPlace(block: InstanceBlock): Unit = {
    val localBuffer = buffer
    BLAS.gemm(-2.0, block.matrix, centerMatrix.transpose, 0.0, localBuffer)

    val size = block.size
    val localCenterSquaredNorms = centerSquaredNorms
    val localWeightSumArr = weightSumVec.values
    val localSumArr = sumMat.values
    var i = 0
    var j = 0
    while (i < size) {
      val weight = block.getWeight(i)
      if (weight > 0) {
        val instanceSquaredNorm = block.getLabel(i)
        var bestIndex = 0
        var bestSquaredDistance = Double.PositiveInfinity
        j = 0
        while (j < k) {
          val squaredDistance = localBuffer(i + j * size) +
            instanceSquaredNorm + localCenterSquaredNorms(j)
          if (squaredDistance < bestSquaredDistance) {
            bestIndex = j
            bestSquaredDistance = squaredDistance
          }
          j += 1
        }

        costSum += weight * bestSquaredDistance
        localWeightSumArr(bestIndex) += weight
        block.getNonZeroIter(i)
          .foreach { case (j, v) => localSumArr(bestIndex + j * k) += v * weight }
      }

      i += 1
    }
  }

  private def cosineUpdateInPlace(block: InstanceBlock): Unit = {
    val localBuffer = buffer
    BLAS.gemm(-1.0, block.matrix, centerMatrix.transpose, 0.0, localBuffer)

    val size = block.size
    val localWeightSumArr = weightSumVec.values
    val localSumArr = sumMat.values
    var i = 0
    var j = 0
    while (i < size) {
      val weight = block.getWeight(i)
      if (weight > 0) {
        var bestIndex = 0
        var bestDistance = Double.PositiveInfinity
        j = 0
        while (j < k) {
          val cosineDistance = 1 + localBuffer(i + j * size)
          if (cosineDistance < bestDistance) {
            bestIndex = j
            bestDistance = cosineDistance
          }
          j += 1
        }

        costSum += weight * bestDistance
        localWeightSumArr(bestIndex) += weight
        block.getNonZeroIter(i)
          .foreach { case (j, v) => localSumArr(bestIndex + j * k) += v * weight }
      }

      i += 1
    }
  }
}

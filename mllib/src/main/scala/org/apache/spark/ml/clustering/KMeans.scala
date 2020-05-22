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
import org.apache.spark.ml.{Estimator, Model, PipelineStage}
import org.apache.spark.ml.feature.{Instance, InstanceBlock}
import org.apache.spark.ml.linalg.{BLAS, DenseMatrix, Matrices, Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.clustering.{DistanceMeasure, KMeans => MLlibKMeans, KMeansModel => MLlibKMeansModel, VectorWithNorm}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.VersionUtils.majorVersion

/**
 * Common params for KMeans and KMeansModel
 */
private[clustering] trait KMeansParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasTol with HasDistanceMeasure with HasWeightCol
  with HasBlockSize {

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
    (value: String) => MLlibKMeans.validateInitMode(value))

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
      predictUDF(DatasetUtils.columnToVector(dataset, getFeaturesCol)),
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
  def predict(features: Vector): Int = parentModel.predict(features)

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
    sparkSession.createDataFrame(data).repartition(1).write.parquet(dataPath)
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

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> MLlibKMeans.K_MEANS_PARALLEL,
    initSteps -> 2,
    tol -> 1e-4,
    distanceMeasure -> DistanceMeasure.EUCLIDEAN,
    blockSize -> 1
  )

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

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): KMeansModel = instrumented { instr =>
    transformSchema(dataset.schema, logging = true)

    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    val w = if (isDefined(weightCol) && $(weightCol).nonEmpty) {
      col($(weightCol)).cast(DoubleType)
    } else {
      lit(1.0)
    }

    val instances: RDD[(Vector, Double)] = dataset
      .select(DatasetUtils.columnToVector(dataset, getFeaturesCol), w).rdd.map {
      case Row(point: Vector, weight: Double) => (point, weight)
    }

    if (handlePersistence) {
      instances.persist(StorageLevel.MEMORY_AND_DISK)
    }

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, featuresCol, predictionCol, k, initMode, initSteps, distanceMeasure,
      maxIter, seed, tol, weightCol, blockSize)

    val model = if ($(blockSize) == 1) {
      trainOnRows(instances)
    } else {
      trainOnBlocks(instances)
    }

    val summary = new KMeansSummary(
      model.transform(dataset),
      $(predictionCol),
      $(featuresCol),
      $(k),
      model.parentModel.numIter,
      model.parentModel.trainingCost)
    model.setSummary(Some(summary))

    instr.logNamedValue("clusterSizes", model.summary.clusterSizes)
    if (handlePersistence) {
      instances.unpersist()
    }
    model
  }

  private def trainOnRows(instances: RDD[(Vector, Double)]): KMeansModel =
    instrumented { instr =>
      val oldVectorInstances = instances.map {
        case (point: Vector, weight: Double) => (OldVectors.fromML(point), weight)
      }
      val algo = new MLlibKMeans()
        .setK($(k))
        .setInitializationMode($(initMode))
        .setInitializationSteps($(initSteps))
        .setMaxIterations($(maxIter))
        .setSeed($(seed))
        .setEpsilon($(tol))
        .setDistanceMeasure($(distanceMeasure))
      val parentModel = algo.runWithWeight(oldVectorInstances, Option(instr))
      val model = copyValues(new KMeansModel(uid, parentModel).setParent(this))
      model
  }

  private def trainOnBlocks(instances: RDD[(Vector, Double)]): KMeansModel =
    instrumented { instr =>
      val instanceRDD: RDD[Instance] = instances.map {
        case (point: Vector, weight: Double) => Instance(0.0, weight, point)
      }

      val blocks = InstanceBlock.blokify(instanceRDD, $(blockSize))
        .persist(StorageLevel.MEMORY_AND_DISK)
        .setName(s"training dataset (blockSize=${$(blockSize)})")

      val sc = instances.sparkContext

      val initStartTime = System.nanoTime()

      val distanceMeasureInstance = DistanceMeasure.decodeFromString($(distanceMeasure))

      val mllibKMeans = new MLlibKMeans()

      val centers = if (initMode == "random") {
        mllibKMeans.initBlocksRandom(blocks)
      } else {
        mllibKMeans.initBlocksKMeansParallel(blocks, distanceMeasureInstance)
      }

      val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
      logInfo(f"Initialization with $initMode took $initTimeInSeconds%.3f seconds.")

      var converged = false
      var cost = 0.0
      var iteration = 0

      val iterationStartTime = System.nanoTime()

      instr.logNumFeatures(centers.head.size)

      // Execute iterations of Lloyd's algorithm until converged
      while (iteration < $(maxIter) && !converged) {
        // Convert center vectors to dense matrix
        val centers_matrix = Matrices.fromVectors(centers).toDense

        val costAccum = sc.doubleAccumulator
        val bcCenters = sc.broadcast(centers_matrix)

        val centers_num = centers_matrix.numRows
        val centers_dim = centers_matrix.numCols

        // Compute squared sums for points
        val data_square_sums: RDD[DenseMatrix] = blocks.mapPartitions { p =>
          p.map { block =>
            computePointsSquareSum(block.matrix.toDense, centers_num) }
        }

        // Find the new centers
        val collected = blocks.zip(data_square_sums).flatMap {
          case (block, points_square_sums) =>
            val centers_matrix = bcCenters.value
            val points_num = block.matrix.numRows

            val sums = Array.fill(centers_num)(Vectors.zeros(centers_dim))
            val counts = Array.fill(centers_num)(0L)

            // Compute squared sums for centers
            val centers_square_sums = computeCentersSquareSum(centers_matrix, points_num)

            // Compute squared distances
            val distances = computeSquaredDistances(
              block.matrix.toDense, points_square_sums,
              centers_matrix, centers_square_sums)

            val (bestCenters, weightedCosts) = findClosest(
              distances, block.weights)

            for (cost <- weightedCosts)
              costAccum.add(cost)

            // sums points around best center
            for ((row, index) <- block.matrix.rowIter.zipWithIndex) {
              val bestCenter = bestCenters(index)
              if (block.weights.nonEmpty) {
                BLAS.axpy(block.weights(index), row, sums(bestCenter))
              } else {
                BLAS.axpy(1, row, sums(bestCenter))
              }
              counts(bestCenter) += 1
            }

            counts.indices.filter(counts(_) > 0).map(j => (j, (sums(j), counts(j)))).iterator
        }.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
          BLAS.axpy(1.0, sum2, sum1)
          (sum1, count1 + count2)
        }.collectAsMap()

        if (iteration == 0) {
          instr.logNumExamples(collected.values.map(_._2).sum)
        }

        val newCenters = collected.mapValues { case (sum, count) =>
          distanceMeasureInstance.centroid(sum, count)
        }

        bcCenters.destroy()

        // Update the cluster centers and costs
        converged = true
        newCenters.foreach { case (j, newCenter) =>
          if (converged &&
            !distanceMeasureInstance.isCenterConverged(
              new VectorWithNorm(centers(j)), newCenter, ${tol})) {
            converged = false
          }
          centers(j) = newCenter.vector
        }

        cost = costAccum.value
        iteration += 1
      }

      val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
      logInfo(f"Iterations took $iterationTimeInSeconds%.3f seconds.")

      if (iteration == ${maxIter}) {
        logInfo(s"KMeans reached the max number of iterations: $maxIter.")
      } else {
        logInfo(s"KMeans converged in $iteration iterations.")
      }

      logInfo(s"The cost is $cost.")

      val parentModel = new MLlibKMeansModel(centers.map(OldVectors.fromML(_)),
        distanceMeasure, cost, iteration)

      val model = copyValues(new KMeansModel(uid, parentModel).setParent(this))
      model
  }

  private def computeSquaredDistances(points_matrix: DenseMatrix,
                              points_square_sums: DenseMatrix,
                              centers_matrix: DenseMatrix,
                              centers_square_sums: DenseMatrix): DenseMatrix = {
    // (x - y)^2 = x^2 + y^2 - 2 * x * y

    // Add up squared sums of points and centers (x^2 + y^2)
    val ret: DenseMatrix = computeMatrixSum(points_square_sums, centers_square_sums)

    // use GEMM to compute squared distances, (2*x*y) can be decomposed to matrix multiply
    val alpha = -2.0
    val beta = 1.0
    BLAS.gemm(alpha, points_matrix, centers_matrix.transpose, beta, ret)

    ret
  }

  private def computePointsSquareSum(points_matrix: DenseMatrix,
                             centers_num: Int): DenseMatrix = {
    val points_num = points_matrix.numRows
    val ret = DenseMatrix.zeros(points_num, centers_num)
    for ((row, index) <- points_matrix.rowIter.zipWithIndex) {
      val square = BLAS.dot(row, row)
      for (i <- 0 until centers_num)
        ret(index, i) = square
    }
    ret
  }

  private def computeCentersSquareSum(centers_matrix: DenseMatrix,
                              points_num: Int): DenseMatrix = {
    val centers_num = centers_matrix.numRows
    val ret = DenseMatrix.zeros(points_num, centers_num)
    for ((row, index) <- centers_matrix.rowIter.zipWithIndex) {
      val square = BLAS.dot(row, row)
      for (i <- 0 until points_num)
        ret(i, index) = square
    }
    ret
  }

  // use GEMM to compute matrix sum
  private def computeMatrixSum(matrix1: DenseMatrix,
                       matrix2: DenseMatrix): DenseMatrix = {
    val column_num = matrix1.numCols
    val eye = DenseMatrix.eye(column_num)
    val alpha = 1.0
    val beta = 1.0
    BLAS.gemm(alpha, matrix1, eye, beta, matrix2)
    matrix2
  }

  private def findClosest(distances: DenseMatrix,
                  weights: Array[Double]): (Array[Int], Array[Double]) = {
    val points_num = distances.numRows
    val ret_closest = new Array[Int](points_num)
    val ret_cost = new Array[Double](points_num)

    for ((row, index) <- distances.rowIter.zipWithIndex) {
      var closest = 0
      var cost = row(0)
      for (i <- 1 until row.size) {
        if (row(i) < cost) {
          closest = i
          cost = row(i)
        }
      }
      ret_closest(index) = closest

      // use weighted squared distance as cost
      if (weights.nonEmpty) {
        ret_cost(index) = cost * weights(index)
      } else {
        ret_cost(index) = cost
      }
    }

    (ret_closest, ret_cost)

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

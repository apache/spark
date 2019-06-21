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

package org.apache.spark.mllib.api.python

import java.io.OutputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import net.razorvine.pickle._

import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel, PrefixSpan}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.random.{RandomRDDs => RG}
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.stat.{KernelDensity, MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.stat.correlation.CorrelationNames
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.stat.test.{ChiSqTestResult, KolmogorovSmirnovTestResult}
import org.apache.spark.mllib.tree.{DecisionTree, GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.{Algo, BoostingStrategy, Strategy}
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.loss.Losses
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, GradientBoostedTreesModel,
  RandomForestModel}
import org.apache.spark.mllib.util.{LinearDataGenerator, MLUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * The Java stubs necessary for the Python mllib bindings. It is called by Py4J on the Python side.
 */
private[python] class PythonMLLibAPI extends Serializable {

  /**
   * Loads and serializes labeled points saved with `RDD#saveAsTextFile`.
   * @param jsc Java SparkContext
   * @param path file or directory path in any Hadoop-supported file system URI
   * @param minPartitions min number of partitions
   * @return serialized labeled points stored in a JavaRDD of byte array
   */
  def loadLabeledPoints(
      jsc: JavaSparkContext,
      path: String,
      minPartitions: Int): JavaRDD[LabeledPoint] =
    MLUtils.loadLabeledPoints(jsc.sc, path, minPartitions)

  /**
   * Loads and serializes vectors saved with `RDD#saveAsTextFile`.
   * @param jsc Java SparkContext
   * @param path file or directory path in any Hadoop-supported file system URI
   * @return serialized vectors in a RDD
   */
  def loadVectors(jsc: JavaSparkContext, path: String): RDD[Vector] =
    MLUtils.loadVectors(jsc.sc, path)

  private def trainRegressionModel(
      learner: GeneralizedLinearAlgorithm[_ <: GeneralizedLinearModel],
      data: JavaRDD[LabeledPoint],
      initialWeights: Vector): JList[Object] = {
    try {
      val model = learner.run(data.rdd.persist(StorageLevel.MEMORY_AND_DISK), initialWeights)
      if (model.isInstanceOf[LogisticRegressionModel]) {
        val lrModel = model.asInstanceOf[LogisticRegressionModel]
        List(lrModel.weights, lrModel.intercept, lrModel.numFeatures, lrModel.numClasses)
          .map(_.asInstanceOf[Object]).asJava
      } else {
        List(model.weights, model.intercept).map(_.asInstanceOf[Object]).asJava
      }
    } finally {
      data.rdd.unpersist()
    }
  }

  /**
   * Return the Updater from string
   */
  def getUpdaterFromString(regType: String): Updater = {
    if (regType == "l2") {
      new SquaredL2Updater
    } else if (regType == "l1") {
      new L1Updater
    } else if (regType == null || regType == "none") {
      new SimpleUpdater
    } else {
      throw new IllegalArgumentException("Invalid value for 'regType' parameter."
        + " Can only be initialized using the following string values: ['l1', 'l2', None].")
    }
  }

  /**
   * Java stub for Python mllib BisectingKMeans.run()
   */
  def trainBisectingKMeans(
      data: JavaRDD[Vector],
      k: Int,
      maxIterations: Int,
      minDivisibleClusterSize: Double,
      seed: java.lang.Long): BisectingKMeansModel = {
    val kmeans = new BisectingKMeans()
      .setK(k)
      .setMaxIterations(maxIterations)
      .setMinDivisibleClusterSize(minDivisibleClusterSize)
    if (seed != null) kmeans.setSeed(seed)
    kmeans.run(data)
  }

  /**
   * Java stub for Python mllib LinearRegressionWithSGD.train()
   */
  def trainLinearRegressionModelWithSGD(
      data: JavaRDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      regParam: Double,
      regType: String,
      intercept: Boolean,
      validateData: Boolean,
      convergenceTol: Double): JList[Object] = {
    val lrAlg = new LinearRegressionWithSGD(1.0, 100, 0.0, 1.0)
    lrAlg.setIntercept(intercept)
      .setValidateData(validateData)
    lrAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
      .setConvergenceTol(convergenceTol)
    lrAlg.optimizer.setUpdater(getUpdaterFromString(regType))
    trainRegressionModel(
      lrAlg,
      data,
      initialWeights)
  }

  /**
   * Java stub for Python mllib LassoWithSGD.train()
   */
  def trainLassoModelWithSGD(
      data: JavaRDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      intercept: Boolean,
      validateData: Boolean,
      convergenceTol: Double): JList[Object] = {
    val lassoAlg = new LassoWithSGD(1.0, 100, 0.01, 1.0)
    lassoAlg.setIntercept(intercept)
      .setValidateData(validateData)
    lassoAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
      .setConvergenceTol(convergenceTol)
    trainRegressionModel(
      lassoAlg,
      data,
      initialWeights)
  }

  /**
   * Java stub for Python mllib RidgeRegressionWithSGD.train()
   */
  def trainRidgeModelWithSGD(
      data: JavaRDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      intercept: Boolean,
      validateData: Boolean,
      convergenceTol: Double): JList[Object] = {
    val ridgeAlg = new RidgeRegressionWithSGD(1.0, 100, 0.01, 1.0)
    ridgeAlg.setIntercept(intercept)
      .setValidateData(validateData)
    ridgeAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
      .setConvergenceTol(convergenceTol)
    trainRegressionModel(
      ridgeAlg,
      data,
      initialWeights)
  }

  /**
   * Java stub for Python mllib SVMWithSGD.train()
   */
  def trainSVMModelWithSGD(
      data: JavaRDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      regType: String,
      intercept: Boolean,
      validateData: Boolean,
      convergenceTol: Double): JList[Object] = {
    val SVMAlg = new SVMWithSGD()
    SVMAlg.setIntercept(intercept)
      .setValidateData(validateData)
    SVMAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
      .setConvergenceTol(convergenceTol)
    SVMAlg.optimizer.setUpdater(getUpdaterFromString(regType))
    trainRegressionModel(
      SVMAlg,
      data,
      initialWeights)
  }

  /**
   * Java stub for Python mllib LogisticRegressionWithSGD.train()
   */
  def trainLogisticRegressionModelWithSGD(
      data: JavaRDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      regParam: Double,
      regType: String,
      intercept: Boolean,
      validateData: Boolean,
      convergenceTol: Double): JList[Object] = {
    val LogRegAlg = new LogisticRegressionWithSGD(1.0, 100, 0.01, 1.0)
    LogRegAlg.setIntercept(intercept)
      .setValidateData(validateData)
    LogRegAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
      .setConvergenceTol(convergenceTol)
    LogRegAlg.optimizer.setUpdater(getUpdaterFromString(regType))
    trainRegressionModel(
      LogRegAlg,
      data,
      initialWeights)
  }

  /**
   * Java stub for Python mllib LogisticRegressionWithLBFGS.train()
   */
  def trainLogisticRegressionModelWithLBFGS(
      data: JavaRDD[LabeledPoint],
      numIterations: Int,
      initialWeights: Vector,
      regParam: Double,
      regType: String,
      intercept: Boolean,
      corrections: Int,
      tolerance: Double,
      validateData: Boolean,
      numClasses: Int): JList[Object] = {
    val LogRegAlg = new LogisticRegressionWithLBFGS()
    LogRegAlg.setIntercept(intercept)
      .setValidateData(validateData)
      .setNumClasses(numClasses)
    LogRegAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setNumCorrections(corrections)
      .setConvergenceTol(tolerance)
    LogRegAlg.optimizer.setUpdater(getUpdaterFromString(regType))
    trainRegressionModel(
      LogRegAlg,
      data,
      initialWeights)
  }

  /**
   * Java stub for NaiveBayes.train()
   */
  def trainNaiveBayesModel(
      data: JavaRDD[LabeledPoint],
      lambda: Double): JList[Object] = {
    val model = NaiveBayes.train(data.rdd, lambda)
    List(Vectors.dense(model.labels), Vectors.dense(model.pi), model.theta.map(Vectors.dense)).
      map(_.asInstanceOf[Object]).asJava
  }

  /**
   * Java stub for Python mllib IsotonicRegression.run()
   */
  def trainIsotonicRegressionModel(
      data: JavaRDD[Vector],
      isotonic: Boolean): JList[Object] = {
    val isotonicRegressionAlg = new IsotonicRegression().setIsotonic(isotonic)
    val input = data.rdd.map { x =>
      (x(0), x(1), x(2))
    }.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val model = isotonicRegressionAlg.run(input)
      List[AnyRef](model.boundaryVector, model.predictionVector).asJava
    } finally {
      data.rdd.unpersist()
    }
  }

  /**
   * Java stub for Python mllib KMeans.run()
   */
  def trainKMeansModel(
      data: JavaRDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String,
      seed: java.lang.Long,
      initializationSteps: Int,
      epsilon: Double,
      initialModel: java.util.ArrayList[Vector]): KMeansModel = {
    val kMeansAlg = new KMeans()
      .setK(k)
      .setMaxIterations(maxIterations)
      .setInitializationMode(initializationMode)
      .setInitializationSteps(initializationSteps)
      .setEpsilon(epsilon)

    if (seed != null) kMeansAlg.setSeed(seed)
    if (!initialModel.isEmpty()) kMeansAlg.setInitialModel(new KMeansModel(initialModel))

    try {
      kMeansAlg.run(data.rdd.persist(StorageLevel.MEMORY_AND_DISK))
    } finally {
      data.rdd.unpersist()
    }
  }

  /**
   * Java stub for Python mllib KMeansModel.computeCost()
   */
  def computeCostKmeansModel(
      data: JavaRDD[Vector],
      centers: java.util.ArrayList[Vector]): Double = {
    new KMeansModel(centers).computeCost(data)
  }

  /**
   * Java stub for Python mllib GaussianMixture.run()
   * Returns a list containing weights, mean and covariance of each mixture component.
   */
  def trainGaussianMixtureModel(
      data: JavaRDD[Vector],
      k: Int,
      convergenceTol: Double,
      maxIterations: Int,
      seed: java.lang.Long,
      initialModelWeights: java.util.ArrayList[Double],
      initialModelMu: java.util.ArrayList[Vector],
      initialModelSigma: java.util.ArrayList[Matrix]): GaussianMixtureModelWrapper = {
    val gmmAlg = new GaussianMixture()
      .setK(k)
      .setConvergenceTol(convergenceTol)
      .setMaxIterations(maxIterations)

    if (initialModelWeights != null && initialModelMu != null && initialModelSigma != null) {
      val gaussians = initialModelMu.asScala.toSeq.zip(initialModelSigma.asScala.toSeq).map {
        case (x, y) => new MultivariateGaussian(x.asInstanceOf[Vector], y.asInstanceOf[Matrix])
      }
      val initialModel = new GaussianMixtureModel(
        initialModelWeights.asScala.toArray, gaussians.toArray)
      gmmAlg.setInitialModel(initialModel)
    }

    if (seed != null) gmmAlg.setSeed(seed)

    try {
      new GaussianMixtureModelWrapper(gmmAlg.run(data.rdd.persist(StorageLevel.MEMORY_AND_DISK)))
    } finally {
      data.rdd.unpersist()
    }
  }

  /**
   * Java stub for Python mllib GaussianMixtureModel.predictSoft()
   */
  def predictSoftGMM(
      data: JavaRDD[Vector],
      wt: Vector,
      mu: Array[Object],
      si: Array[Object]): RDD[Vector] = {

      val weight = wt.toArray
      val mean = mu.map(_.asInstanceOf[DenseVector])
      val sigma = si.map(_.asInstanceOf[DenseMatrix])
      val gaussians = Array.tabulate(weight.length) {
        i => new MultivariateGaussian(mean(i), sigma(i))
      }
      val model = new GaussianMixtureModel(weight, gaussians)
      model.predictSoft(data).map(Vectors.dense)
  }

  /**
   * Java stub for Python mllib PowerIterationClustering.run(). This stub returns a
   * handle to the Java object instead of the content of the Java object.  Extra care
   * needs to be taken in the Python code to ensure it gets freed on exit; see the
   * Py4J documentation.
   * @param data an RDD of (i, j, s,,ij,,) tuples representing the affinity matrix.
   * @param k number of clusters.
   * @param maxIterations maximum number of iterations of the power iteration loop.
   * @param initMode the initialization mode. This can be either "random" to use
   *                 a random vector as vertex properties, or "degree" to use
   *                 normalized sum similarities. Default: random.
   */
  def trainPowerIterationClusteringModel(
      data: JavaRDD[Vector],
      k: Int,
      maxIterations: Int,
      initMode: String): PowerIterationClusteringModel = {

    val pic = new PowerIterationClustering()
      .setK(k)
      .setMaxIterations(maxIterations)
      .setInitializationMode(initMode)

    val model = pic.run(data.rdd.map(v => (v(0).toLong, v(1).toLong, v(2))))
    new PowerIterationClusteringModelWrapper(model)
  }

  /**
   * Java stub for Python mllib ALS.train().  This stub returns a handle
   * to the Java object instead of the content of the Java object.  Extra care
   * needs to be taken in the Python code to ensure it gets freed on exit; see
   * the Py4J documentation.
   */
  def trainALSModel(
      ratingsJRDD: JavaRDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      nonnegative: Boolean,
      seed: java.lang.Long): MatrixFactorizationModel = {

    val als = new ALS()
      .setRank(rank)
      .setIterations(iterations)
      .setLambda(lambda)
      .setBlocks(blocks)
      .setNonnegative(nonnegative)

    if (seed != null) als.setSeed(seed)

    val model = als.run(ratingsJRDD.rdd)
    new MatrixFactorizationModelWrapper(model)
  }

  /**
   * Java stub for Python mllib ALS.trainImplicit().  This stub returns a
   * handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on
   * exit; see the Py4J documentation.
   */
  def trainImplicitALSModel(
      ratingsJRDD: JavaRDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      alpha: Double,
      nonnegative: Boolean,
      seed: java.lang.Long): MatrixFactorizationModel = {

    val als = new ALS()
      .setImplicitPrefs(true)
      .setRank(rank)
      .setIterations(iterations)
      .setLambda(lambda)
      .setBlocks(blocks)
      .setAlpha(alpha)
      .setNonnegative(nonnegative)

    if (seed != null) als.setSeed(seed)

    val model = als.run(ratingsJRDD.rdd)
    new MatrixFactorizationModelWrapper(model)
  }

  /**
   * Java stub for Python mllib LDA.run()
   */
  def trainLDAModel(
      data: JavaRDD[java.util.List[Any]],
      k: Int,
      maxIterations: Int,
      docConcentration: Double,
      topicConcentration: Double,
      seed: java.lang.Long,
      checkpointInterval: Int,
      optimizer: String): LDAModelWrapper = {
    val algo = new LDA()
      .setK(k)
      .setMaxIterations(maxIterations)
      .setDocConcentration(docConcentration)
      .setTopicConcentration(topicConcentration)
      .setCheckpointInterval(checkpointInterval)
      .setOptimizer(optimizer)

    if (seed != null) algo.setSeed(seed)

    val documents = data.rdd.map(_.asScala.toArray).map { r =>
      r(0) match {
        case i: java.lang.Integer => (i.toLong, r(1).asInstanceOf[Vector])
        case i: java.lang.Long => (i.toLong, r(1).asInstanceOf[Vector])
        case _ => throw new IllegalArgumentException("input values contains invalid type value.")
      }
    }
    val model = algo.run(documents)
    new LDAModelWrapper(model)
  }

  /**
   * Load a LDA model
   */
  def loadLDAModel(jsc: JavaSparkContext, path: String): LDAModelWrapper = {
    val model = DistributedLDAModel.load(jsc.sc, path)
    new LDAModelWrapper(model)
  }


  /**
   * Java stub for Python mllib FPGrowth.train().  This stub returns a handle
   * to the Java object instead of the content of the Java object.  Extra care
   * needs to be taken in the Python code to ensure it gets freed on exit; see
   * the Py4J documentation.
   */
  def trainFPGrowthModel(
      data: JavaRDD[java.lang.Iterable[Any]],
      minSupport: Double,
      numPartitions: Int): FPGrowthModel[Any] = {
    val fpg = new FPGrowth(minSupport, numPartitions)
    val model = fpg.run(data.rdd.map(_.asScala.toArray))
    new FPGrowthModelWrapper(model)
  }

  /**
   * Java stub for Python mllib PrefixSpan.train().  This stub returns a handle
   * to the Java object instead of the content of the Java object.  Extra care
   * needs to be taken in the Python code to ensure it gets freed on exit; see
   * the Py4J documentation.
   */
  def trainPrefixSpanModel(
      data: JavaRDD[java.util.ArrayList[java.util.ArrayList[Any]]],
      minSupport: Double,
      maxPatternLength: Int,
      localProjDBSize: Int ): PrefixSpanModelWrapper = {
    val prefixSpan = new PrefixSpan()
      .setMinSupport(minSupport)
      .setMaxPatternLength(maxPatternLength)
      .setMaxLocalProjDBSize(localProjDBSize)

    val trainData = data.rdd.map(_.asScala.toArray.map(_.asScala.toArray))
    val model = prefixSpan.run(trainData)
    new PrefixSpanModelWrapper(model)
  }

  /**
   * Java stub for Normalizer.transform()
   */
  def normalizeVector(p: Double, vector: Vector): Vector = {
    new Normalizer(p).transform(vector)
  }

  /**
   * Java stub for Normalizer.transform()
   */
  def normalizeVector(p: Double, rdd: JavaRDD[Vector]): JavaRDD[Vector] = {
    new Normalizer(p).transform(rdd)
  }

  /**
   * Java stub for StandardScaler.fit(). This stub returns a
   * handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on
   * exit; see the Py4J documentation.
   */
  def fitStandardScaler(
      withMean: Boolean,
      withStd: Boolean,
      data: JavaRDD[Vector]): StandardScalerModel = {
    new StandardScaler(withMean, withStd).fit(data.rdd)
  }

  /**
   * Java stub for ChiSqSelector.fit(). This stub returns a
   * handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on
   * exit; see the Py4J documentation.
   */
  def fitChiSqSelector(
      selectorType: String,
      numTopFeatures: Int,
      percentile: Double,
      fpr: Double,
      fdr: Double,
      fwe: Double,
      data: JavaRDD[LabeledPoint]): ChiSqSelectorModel = {
    new ChiSqSelector()
      .setSelectorType(selectorType)
      .setNumTopFeatures(numTopFeatures)
      .setPercentile(percentile)
      .setFpr(fpr)
      .setFdr(fdr)
      .setFwe(fwe)
      .fit(data.rdd)
  }

  /**
   * Java stub for PCA.fit(). This stub returns a
   * handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on
   * exit; see the Py4J documentation.
   */
  def fitPCA(k: Int, data: JavaRDD[Vector]): PCAModel = {
    new PCA(k).fit(data.rdd)
  }

  /**
   * Java stub for IDF.fit(). This stub returns a
   * handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on
   * exit; see the Py4J documentation.
   */
  def fitIDF(minDocFreq: Int, dataset: JavaRDD[Vector]): IDFModel = {
    new IDF(minDocFreq).fit(dataset)
  }

  /**
   * Java stub for Python mllib Word2Vec fit(). This stub returns a
   * handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on
   * exit; see the Py4J documentation.
   * @param dataJRDD input JavaRDD
   * @param vectorSize size of vector
   * @param learningRate initial learning rate
   * @param numPartitions number of partitions
   * @param numIterations number of iterations
   * @param seed initial seed for random generator
   * @param windowSize size of window
   * @return A handle to java Word2VecModelWrapper instance at python side
   */
  def trainWord2VecModel(
      dataJRDD: JavaRDD[java.util.ArrayList[String]],
      vectorSize: Int,
      learningRate: Double,
      numPartitions: Int,
      numIterations: Int,
      seed: java.lang.Long,
      minCount: Int,
      windowSize: Int): Word2VecModelWrapper = {
    val word2vec = new Word2Vec()
      .setVectorSize(vectorSize)
      .setLearningRate(learningRate)
      .setNumPartitions(numPartitions)
      .setNumIterations(numIterations)
      .setMinCount(minCount)
      .setWindowSize(windowSize)
    if (seed != null) word2vec.setSeed(seed)
    try {
      val model = word2vec.fit(dataJRDD.rdd.persist(StorageLevel.MEMORY_AND_DISK_SER))
      new Word2VecModelWrapper(model)
    } finally {
      dataJRDD.rdd.unpersist()
    }
  }

  /**
   * Java stub for Python mllib DecisionTree.train().
   * This stub returns a handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on exit;
   * see the Py4J documentation.
   * @param data  Training data
   * @param categoricalFeaturesInfo  Categorical features info, as Java map
   */
  def trainDecisionTreeModel(
      data: JavaRDD[LabeledPoint],
      algoStr: String,
      numClasses: Int,
      categoricalFeaturesInfo: JMap[Int, Int],
      impurityStr: String,
      maxDepth: Int,
      maxBins: Int,
      minInstancesPerNode: Int,
      minInfoGain: Double): DecisionTreeModel = {

    val algo = Algo.fromString(algoStr)
    val impurity = Impurities.fromString(impurityStr)

    val strategy = new Strategy(
      algo = algo,
      impurity = impurity,
      maxDepth = maxDepth,
      numClasses = numClasses,
      maxBins = maxBins,
      categoricalFeaturesInfo = categoricalFeaturesInfo.asScala.toMap,
      minInstancesPerNode = minInstancesPerNode,
      minInfoGain = minInfoGain)
    try {
      DecisionTree.train(data.rdd.persist(StorageLevel.MEMORY_AND_DISK), strategy)
    } finally {
      data.rdd.unpersist()
    }
  }

  /**
   * Java stub for Python mllib RandomForest.train().
   * This stub returns a handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on exit;
   * see the Py4J documentation.
   */
  def trainRandomForestModel(
      data: JavaRDD[LabeledPoint],
      algoStr: String,
      numClasses: Int,
      categoricalFeaturesInfo: JMap[Int, Int],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurityStr: String,
      maxDepth: Int,
      maxBins: Int,
      seed: java.lang.Long): RandomForestModel = {

    val algo = Algo.fromString(algoStr)
    val impurity = Impurities.fromString(impurityStr)
    val strategy = new Strategy(
      algo = algo,
      impurity = impurity,
      maxDepth = maxDepth,
      numClasses = numClasses,
      maxBins = maxBins,
      categoricalFeaturesInfo = categoricalFeaturesInfo.asScala.toMap)
    val cached = data.rdd.persist(StorageLevel.MEMORY_AND_DISK)
    // Only done because methods below want an int, not an optional Long
    val intSeed = getSeedOrDefault(seed).toInt
    try {
      if (algo == Algo.Classification) {
        RandomForest.trainClassifier(cached, strategy, numTrees, featureSubsetStrategy, intSeed)
      } else {
        RandomForest.trainRegressor(cached, strategy, numTrees, featureSubsetStrategy, intSeed)
      }
    } finally {
      cached.unpersist()
    }
  }

  /**
   * Java stub for Python mllib GradientBoostedTrees.train().
   * This stub returns a handle to the Java object instead of the content of the Java object.
   * Extra care needs to be taken in the Python code to ensure it gets freed on exit;
   * see the Py4J documentation.
   */
  def trainGradientBoostedTreesModel(
      data: JavaRDD[LabeledPoint],
      algoStr: String,
      categoricalFeaturesInfo: JMap[Int, Int],
      lossStr: String,
      numIterations: Int,
      learningRate: Double,
      maxDepth: Int,
      maxBins: Int): GradientBoostedTreesModel = {
    val boostingStrategy = BoostingStrategy.defaultParams(algoStr)
    boostingStrategy.setLoss(Losses.fromString(lossStr))
    boostingStrategy.setNumIterations(numIterations)
    boostingStrategy.setLearningRate(learningRate)
    boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
    boostingStrategy.treeStrategy.setMaxBins(maxBins)
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = categoricalFeaturesInfo.asScala.toMap

    val cached = data.rdd.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      GradientBoostedTrees.train(cached, boostingStrategy)
    } finally {
      cached.unpersist()
    }
  }

  def elementwiseProductVector(scalingVector: Vector, vector: Vector): Vector = {
    new ElementwiseProduct(scalingVector).transform(vector)
  }

  def elementwiseProductVector(scalingVector: Vector, vector: JavaRDD[Vector]): JavaRDD[Vector] = {
    new ElementwiseProduct(scalingVector).transform(vector)
  }

  /**
   * Java stub for mllib Statistics.colStats(X: RDD[Vector]).
   * TODO figure out return type.
   */
  def colStats(rdd: JavaRDD[Vector]): MultivariateStatisticalSummary = {
    Statistics.colStats(rdd.rdd)
  }

  /**
   * Java stub for mllib Statistics.corr(X: RDD[Vector], method: String).
   * Returns the correlation matrix serialized into a byte array understood by deserializers in
   * pyspark.
   */
  def corr(x: JavaRDD[Vector], method: String): Matrix = {
    Statistics.corr(x.rdd, getCorrNameOrDefault(method))
  }

  /**
   * Java stub for mllib Statistics.corr(x: RDD[Double], y: RDD[Double], method: String).
   */
  def corr(x: JavaRDD[Double], y: JavaRDD[Double], method: String): Double = {
    Statistics.corr(x.rdd, y.rdd, getCorrNameOrDefault(method))
  }

  /**
   * Java stub for mllib Statistics.chiSqTest()
   */
  def chiSqTest(observed: Vector, expected: Vector): ChiSqTestResult = {
    if (expected == null) {
      Statistics.chiSqTest(observed)
    } else {
      Statistics.chiSqTest(observed, expected)
    }
  }

  /**
   * Java stub for mllib Statistics.chiSqTest(observed: Matrix)
   */
  def chiSqTest(observed: Matrix): ChiSqTestResult = {
    Statistics.chiSqTest(observed)
  }

  /**
   * Java stub for mllib Statistics.chiSqTest(RDD[LabelPoint])
   */
  def chiSqTest(data: JavaRDD[LabeledPoint]): Array[ChiSqTestResult] = {
    Statistics.chiSqTest(data.rdd)
  }

  // used by the corr methods to retrieve the name of the correlation method passed in via pyspark
  private def getCorrNameOrDefault(method: String) = {
    if (method == null) CorrelationNames.defaultCorrName else method
  }

  // Used by the *RDD methods to get default seed if not passed in from pyspark
  private def getSeedOrDefault(seed: java.lang.Long): Long = {
    if (seed == null) Utils.random.nextLong else seed
  }

  // Used by *RDD methods to get default numPartitions if not passed in from pyspark
  private def getNumPartitionsOrDefault(numPartitions: java.lang.Integer,
      jsc: JavaSparkContext): Int = {
    if (numPartitions == null) {
      jsc.sc.defaultParallelism
    } else {
      numPartitions
    }
  }

  // Note: for the following methods, numPartitions and seed are boxed to allow nulls to be passed
  // in for either argument from pyspark

  /**
   * Java stub for Python mllib RandomRDDGenerators.uniformRDD()
   */
  def uniformRDD(jsc: JavaSparkContext,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Double] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.uniformRDD(jsc.sc, size, parts, s)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.normalRDD()
   */
  def normalRDD(jsc: JavaSparkContext,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Double] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.normalRDD(jsc.sc, size, parts, s)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.logNormalRDD()
   */
  def logNormalRDD(jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Double] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.logNormalRDD(jsc.sc, mean, std, size, parts, s)
  }


  /**
   * Java stub for Python mllib RandomRDDGenerators.poissonRDD()
   */
  def poissonRDD(jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Double] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.poissonRDD(jsc.sc, mean, size, parts, s)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.exponentialRDD()
   */
  def exponentialRDD(jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Double] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.exponentialRDD(jsc.sc, mean, size, parts, s)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.gammaRDD()
   */
  def gammaRDD(jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Double] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.gammaRDD(jsc.sc, shape, scale, size, parts, s)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.uniformVectorRDD()
   */
  def uniformVectorRDD(jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Vector] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.uniformVectorRDD(jsc.sc, numRows, numCols, parts, s)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.normalVectorRDD()
   */
  def normalVectorRDD(jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Vector] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.normalVectorRDD(jsc.sc, numRows, numCols, parts, s)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.logNormalVectorRDD()
   */
  def logNormalVectorRDD(jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Vector] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.logNormalVectorRDD(jsc.sc, mean, std, numRows, numCols, parts, s)
  }


  /**
   * Java stub for Python mllib RandomRDDGenerators.poissonVectorRDD()
   */
  def poissonVectorRDD(jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Vector] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.poissonVectorRDD(jsc.sc, mean, numRows, numCols, parts, s)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.exponentialVectorRDD()
   */
  def exponentialVectorRDD(jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Vector] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.exponentialVectorRDD(jsc.sc, mean, numRows, numCols, parts, s)
  }

  /**
   * Java stub for Python mllib RandomRDDGenerators.gammaVectorRDD()
   */
  def gammaVectorRDD(jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long): JavaRDD[Vector] = {
    val parts = getNumPartitionsOrDefault(numPartitions, jsc)
    val s = getSeedOrDefault(seed)
    RG.gammaVectorRDD(jsc.sc, shape, scale, numRows, numCols, parts, s)
  }

  /**
   * Java stub for the constructor of Python mllib RankingMetrics
   */
  def newRankingMetrics(predictionAndLabels: DataFrame): RankingMetrics[Any] = {
    new RankingMetrics(predictionAndLabels.rdd.map(
      r => (r.getSeq(0).toArray[Any], r.getSeq(1).toArray[Any])))
  }

  /**
   * Java stub for the estimate method of KernelDensity
   */
  def estimateKernelDensity(
      sample: JavaRDD[Double],
      bandwidth: Double, points: java.util.ArrayList[Double]): Array[Double] = {
    new KernelDensity().setSample(sample).setBandwidth(bandwidth).estimate(
      points.asScala.toArray)
  }

  /**
   * Java stub for the update method of StreamingKMeansModel.
   */
  def updateStreamingKMeansModel(
      clusterCenters: JList[Vector],
      clusterWeights: JList[Double],
      data: JavaRDD[Vector],
      decayFactor: Double,
      timeUnit: String): JList[Object] = {
    val model = new StreamingKMeansModel(
      clusterCenters.asScala.toArray, clusterWeights.asScala.toArray)
        .update(data, decayFactor, timeUnit)
      List[AnyRef](model.clusterCenters, Vectors.dense(model.clusterWeights)).asJava
  }

  /**
   * Wrapper around the generateLinearInput method of LinearDataGenerator.
   */
  def generateLinearInputWrapper(
      intercept: Double,
      weights: JList[Double],
      xMean: JList[Double],
      xVariance: JList[Double],
      nPoints: Int,
      seed: Int,
      eps: Double): Array[LabeledPoint] = {
    LinearDataGenerator.generateLinearInput(
      intercept, weights.asScala.toArray, xMean.asScala.toArray,
      xVariance.asScala.toArray, nPoints, seed, eps).toArray
  }

  /**
   * Wrapper around the generateLinearRDD method of LinearDataGenerator.
   */
  def generateLinearRDDWrapper(
      sc: JavaSparkContext,
      nexamples: Int,
      nfeatures: Int,
      eps: Double,
      nparts: Int,
      intercept: Double): JavaRDD[LabeledPoint] = {
    LinearDataGenerator.generateLinearRDD(
      sc, nexamples, nfeatures, eps, nparts, intercept)
  }

  /**
   * Java stub for Statistics.kolmogorovSmirnovTest()
   */
  def kolmogorovSmirnovTest(
      data: JavaRDD[Double],
      distName: String,
      params: JList[Double]): KolmogorovSmirnovTestResult = {
    val paramsSeq = params.asScala.toSeq
    Statistics.kolmogorovSmirnovTest(data, distName, paramsSeq: _*)
  }

  /**
   * Wrapper around RowMatrix constructor.
   */
  def createRowMatrix(rows: JavaRDD[Vector], numRows: Long, numCols: Int): RowMatrix = {
    new RowMatrix(rows.rdd, numRows, numCols)
  }

  /**
   * Wrapper around IndexedRowMatrix constructor.
   */
  def createIndexedRowMatrix(rows: DataFrame, numRows: Long, numCols: Int): IndexedRowMatrix = {
    // We use DataFrames for serialization of IndexedRows from Python,
    // so map each Row in the DataFrame back to an IndexedRow.
    val indexedRows = rows.rdd.map {
      case Row(index: Long, vector: Vector) => IndexedRow(index, vector)
    }
    new IndexedRowMatrix(indexedRows, numRows, numCols)
  }

  /**
   * Wrapper around CoordinateMatrix constructor.
   */
  def createCoordinateMatrix(rows: DataFrame, numRows: Long, numCols: Long): CoordinateMatrix = {
    // We use DataFrames for serialization of MatrixEntry entries from
    // Python, so map each Row in the DataFrame back to a MatrixEntry.
    val entries = rows.rdd.map {
      case Row(i: Long, j: Long, value: Double) => MatrixEntry(i, j, value)
    }
    new CoordinateMatrix(entries, numRows, numCols)
  }

  /**
   * Wrapper around BlockMatrix constructor.
   */
  def createBlockMatrix(blocks: DataFrame, rowsPerBlock: Int, colsPerBlock: Int,
                        numRows: Long, numCols: Long): BlockMatrix = {
    // We use DataFrames for serialization of sub-matrix blocks from
    // Python, so map each Row in the DataFrame back to a
    // ((blockRowIndex, blockColIndex), sub-matrix) tuple.
    val blockTuples = blocks.rdd.map {
      case Row(Row(blockRowIndex: Long, blockColIndex: Long), subMatrix: Matrix) =>
        ((blockRowIndex.toInt, blockColIndex.toInt), subMatrix)
    }
    new BlockMatrix(blockTuples, rowsPerBlock, colsPerBlock, numRows, numCols)
  }

  /**
   * Return the rows of an IndexedRowMatrix.
   */
  def getIndexedRows(indexedRowMatrix: IndexedRowMatrix): DataFrame = {
    // We use DataFrames for serialization of IndexedRows to Python,
    // so return a DataFrame.
    val sc = indexedRowMatrix.rows.sparkContext
    val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
    spark.createDataFrame(indexedRowMatrix.rows)
  }

  /**
   * Return the entries of a CoordinateMatrix.
   */
  def getMatrixEntries(coordinateMatrix: CoordinateMatrix): DataFrame = {
    // We use DataFrames for serialization of MatrixEntry entries to
    // Python, so return a DataFrame.
    val sc = coordinateMatrix.entries.sparkContext
    val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
    spark.createDataFrame(coordinateMatrix.entries)
  }

  /**
   * Return the sub-matrix blocks of a BlockMatrix.
   */
  def getMatrixBlocks(blockMatrix: BlockMatrix): DataFrame = {
    // We use DataFrames for serialization of sub-matrix blocks to
    // Python, so return a DataFrame.
    val sc = blockMatrix.blocks.sparkContext
    val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
    spark.createDataFrame(blockMatrix.blocks)
  }

  /**
   * Python-friendly version of [[MLUtils.convertVectorColumnsToML()]].
   */
  def convertVectorColumnsToML(dataset: DataFrame, cols: JArrayList[String]): DataFrame = {
    MLUtils.convertVectorColumnsToML(dataset, cols.asScala: _*)
  }

  /**
   * Python-friendly version of [[MLUtils.convertVectorColumnsFromML()]]
   */
  def convertVectorColumnsFromML(dataset: DataFrame, cols: JArrayList[String]): DataFrame = {
    MLUtils.convertVectorColumnsFromML(dataset, cols.asScala: _*)
  }

  /**
   * Python-friendly version of [[MLUtils.convertMatrixColumnsToML()]].
   */
  def convertMatrixColumnsToML(dataset: DataFrame, cols: JArrayList[String]): DataFrame = {
    MLUtils.convertMatrixColumnsToML(dataset, cols.asScala: _*)
  }

  /**
   * Python-friendly version of [[MLUtils.convertMatrixColumnsFromML()]]
   */
  def convertMatrixColumnsFromML(dataset: DataFrame, cols: JArrayList[String]): DataFrame = {
    MLUtils.convertMatrixColumnsFromML(dataset, cols.asScala: _*)
  }
}

/**
 * Basic SerDe utility class.
 */
private[spark] abstract class SerDeBase {

  val PYSPARK_PACKAGE: String
  def initialize(): Unit

  /**
   * Base class used for pickle
   */
  private[spark] abstract class BasePickler[T: ClassTag]
    extends IObjectPickler with IObjectConstructor {

    private val cls = implicitly[ClassTag[T]].runtimeClass
    private val module = PYSPARK_PACKAGE + "." + cls.getName.split('.')(4)
    private val name = cls.getSimpleName

    // register this to Pickler and Unpickler
    def register(): Unit = {
      Pickler.registerCustomPickler(this.getClass, this)
      Pickler.registerCustomPickler(cls, this)
      Unpickler.registerConstructor(module, name, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write((module + "\n" + name + "\n").getBytes(StandardCharsets.UTF_8))
      } else {
        pickler.save(this)  // it will be memorized by Pickler
        saveState(obj, out, pickler)
        out.write(Opcodes.REDUCE)
      }
    }

    private[python] def saveObjects(out: OutputStream, pickler: Pickler, objects: Any*) = {
      if (objects.length == 0 || objects.length > 3) {
        out.write(Opcodes.MARK)
      }
      objects.foreach(pickler.save)
      val code = objects.length match {
        case 1 => Opcodes.TUPLE1
        case 2 => Opcodes.TUPLE2
        case 3 => Opcodes.TUPLE3
        case _ => Opcodes.TUPLE
      }
      out.write(code)
    }

    protected def getBytes(obj: Object): Array[Byte] = {
      if (obj.getClass.isArray) {
        obj.asInstanceOf[Array[Byte]]
      } else {
        // This must be ISO 8859-1 / Latin 1, not UTF-8, to interoperate correctly
        obj.asInstanceOf[String].getBytes(StandardCharsets.ISO_8859_1)
      }
    }

    private[python] def saveState(obj: Object, out: OutputStream, pickler: Pickler)
  }

  def dumps(obj: AnyRef): Array[Byte] = {
    obj match {
      // Pickler in Python side cannot deserialize Scala Array normally. See SPARK-12834.
      case array: Array[_] => new Pickler().dumps(array.toSeq.asJava)
      case _ => new Pickler().dumps(obj)
    }
  }

  def loads(bytes: Array[Byte]): AnyRef = {
    new Unpickler().loads(bytes)
  }

  /* convert object into Tuple */
  def asTupleRDD(rdd: RDD[Array[Any]]): RDD[(Int, Int)] = {
    rdd.map(x => (x(0).asInstanceOf[Int], x(1).asInstanceOf[Int]))
  }

  /* convert RDD[Tuple2[,]] to RDD[Array[Any]] */
  def fromTuple2RDD(rdd: RDD[(Any, Any)]): RDD[Array[Any]] = {
    rdd.map(x => Array(x._1, x._2))
  }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Python objects, that is usable by
   * PySpark.
   */
  def javaToPython(jRDD: JavaRDD[Any]): JavaRDD[Array[Byte]] = {
    jRDD.rdd.mapPartitions { iter =>
      initialize()  // let it called in executor
      new SerDeUtil.AutoBatchedPickler(iter)
    }
  }

  /**
   * Convert an RDD of serialized Python objects to RDD of objects, that is usable by PySpark.
   */
  def pythonToJava(pyRDD: JavaRDD[Array[Byte]], batched: Boolean): JavaRDD[Any] = {
    pyRDD.rdd.mapPartitions { iter =>
      initialize()  // let it called in executor
      val unpickle = new Unpickler
      iter.flatMap { row =>
        val obj = unpickle.loads(row)
        // `Opcodes.MEMOIZE` of Protocol 4 (Python 3.4+) will store objects in internal map
        // of `Unpickler`. This map is cleared when calling `Unpickler.close()`. Pyrolite
        // doesn't clear it up, so we manually clear it.
        unpickle.close()
        if (batched) {
          obj match {
            case list: JArrayList[_] => list.asScala
            case arr: Array[_] => arr
          }
        } else {
          Seq(obj)
        }
      }
    }.toJavaRDD()
  }
}

/**
 * SerDe utility functions for PythonMLLibAPI.
 */
private[spark] object SerDe extends SerDeBase with Serializable {

  override val PYSPARK_PACKAGE = "pyspark.mllib"

  // Pickler for DenseVector
  private[python] class DenseVectorPickler extends BasePickler[DenseVector] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val vector: DenseVector = obj.asInstanceOf[DenseVector]
      val bytes = new Array[Byte](8 * vector.size)
      val bb = ByteBuffer.wrap(bytes)
      bb.order(ByteOrder.nativeOrder())
      val db = bb.asDoubleBuffer()
      db.put(vector.values)

      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(bytes.length))
      out.write(bytes)
      out.write(Opcodes.TUPLE1)
    }

    def construct(args: Array[Object]): Object = {
      require(args.length == 1)
      if (args.length != 1) {
        throw new PickleException("should be 1")
      }
      val bytes = getBytes(args(0))
      val bb = ByteBuffer.wrap(bytes, 0, bytes.length)
      bb.order(ByteOrder.nativeOrder())
      val db = bb.asDoubleBuffer()
      val ans = new Array[Double](bytes.length / 8)
      db.get(ans)
      Vectors.dense(ans)
    }
  }

  // Pickler for DenseMatrix
  private[python] class DenseMatrixPickler extends BasePickler[DenseMatrix] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val m: DenseMatrix = obj.asInstanceOf[DenseMatrix]
      val bytes = new Array[Byte](8 * m.values.length)
      val order = ByteOrder.nativeOrder()
      val isTransposed = if (m.isTransposed) 1 else 0
      ByteBuffer.wrap(bytes).order(order).asDoubleBuffer().put(m.values)

      out.write(Opcodes.MARK)
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(m.numRows))
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(m.numCols))
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(bytes.length))
      out.write(bytes)
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(isTransposed))
      out.write(Opcodes.TUPLE)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 4) {
        throw new PickleException("should be 4")
      }
      val bytes = getBytes(args(2))
      val n = bytes.length / 8
      val values = new Array[Double](n)
      val order = ByteOrder.nativeOrder()
      ByteBuffer.wrap(bytes).order(order).asDoubleBuffer().get(values)
      val isTransposed = args(3).asInstanceOf[Int] == 1
      new DenseMatrix(args(0).asInstanceOf[Int], args(1).asInstanceOf[Int], values, isTransposed)
    }
  }

  // Pickler for SparseMatrix
  private[python] class SparseMatrixPickler extends BasePickler[SparseMatrix] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val s = obj.asInstanceOf[SparseMatrix]
      val order = ByteOrder.nativeOrder()

      val colPtrsBytes = new Array[Byte](4 * s.colPtrs.length)
      val indicesBytes = new Array[Byte](4 * s.rowIndices.length)
      val valuesBytes = new Array[Byte](8 * s.values.length)
      val isTransposed = if (s.isTransposed) 1 else 0
      ByteBuffer.wrap(colPtrsBytes).order(order).asIntBuffer().put(s.colPtrs)
      ByteBuffer.wrap(indicesBytes).order(order).asIntBuffer().put(s.rowIndices)
      ByteBuffer.wrap(valuesBytes).order(order).asDoubleBuffer().put(s.values)

      out.write(Opcodes.MARK)
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(s.numRows))
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(s.numCols))
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(colPtrsBytes.length))
      out.write(colPtrsBytes)
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(indicesBytes.length))
      out.write(indicesBytes)
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(valuesBytes.length))
      out.write(valuesBytes)
      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(isTransposed))
      out.write(Opcodes.TUPLE)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 6) {
        throw new PickleException("should be 6")
      }
      val order = ByteOrder.nativeOrder()
      val colPtrsBytes = getBytes(args(2))
      val indicesBytes = getBytes(args(3))
      val valuesBytes = getBytes(args(4))
      val colPtrs = new Array[Int](colPtrsBytes.length / 4)
      val rowIndices = new Array[Int](indicesBytes.length / 4)
      val values = new Array[Double](valuesBytes.length / 8)
      ByteBuffer.wrap(colPtrsBytes).order(order).asIntBuffer().get(colPtrs)
      ByteBuffer.wrap(indicesBytes).order(order).asIntBuffer().get(rowIndices)
      ByteBuffer.wrap(valuesBytes).order(order).asDoubleBuffer().get(values)
      val isTransposed = args(5).asInstanceOf[Int] == 1
      new SparseMatrix(
        args(0).asInstanceOf[Int], args(1).asInstanceOf[Int], colPtrs, rowIndices, values,
        isTransposed)
    }
  }

  // Pickler for SparseVector
  private[python] class SparseVectorPickler extends BasePickler[SparseVector] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val v: SparseVector = obj.asInstanceOf[SparseVector]
      val n = v.indices.length
      val indiceBytes = new Array[Byte](4 * n)
      val order = ByteOrder.nativeOrder()
      ByteBuffer.wrap(indiceBytes).order(order).asIntBuffer().put(v.indices)
      val valueBytes = new Array[Byte](8 * n)
      ByteBuffer.wrap(valueBytes).order(order).asDoubleBuffer().put(v.values)

      out.write(Opcodes.BININT)
      out.write(PickleUtils.integer_to_bytes(v.size))
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(indiceBytes.length))
      out.write(indiceBytes)
      out.write(Opcodes.BINSTRING)
      out.write(PickleUtils.integer_to_bytes(valueBytes.length))
      out.write(valueBytes)
      out.write(Opcodes.TUPLE3)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 3) {
        throw new PickleException("should be 3")
      }
      val size = args(0).asInstanceOf[Int]
      val indiceBytes = getBytes(args(1))
      val valueBytes = getBytes(args(2))
      val n = indiceBytes.length / 4
      val indices = new Array[Int](n)
      val values = new Array[Double](n)
      if (n > 0) {
        val order = ByteOrder.nativeOrder()
        ByteBuffer.wrap(indiceBytes).order(order).asIntBuffer().get(indices)
        ByteBuffer.wrap(valueBytes).order(order).asDoubleBuffer().get(values)
      }
      new SparseVector(size, indices, values)
    }
  }

  // Pickler for MLlib LabeledPoint
  private[python] class LabeledPointPickler extends BasePickler[LabeledPoint] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val point: LabeledPoint = obj.asInstanceOf[LabeledPoint]
      saveObjects(out, pickler, point.label, point.features)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 2) {
        throw new PickleException("should be 2")
      }
      new LabeledPoint(args(0).asInstanceOf[Double], args(1).asInstanceOf[Vector])
    }
  }

  // Pickler for Rating
  private[python] class RatingPickler extends BasePickler[Rating] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      val rating: Rating = obj.asInstanceOf[Rating]
      saveObjects(out, pickler, rating.user, rating.product, rating.rating)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 3) {
        throw new PickleException("should be 3")
      }
      new Rating(ratingsIdCheckLong(args(0)), ratingsIdCheckLong(args(1)),
        args(2).asInstanceOf[Double])
    }

    private def ratingsIdCheckLong(obj: Object): Int = {
      try {
        obj.asInstanceOf[Int]
      } catch {
        case ex: ClassCastException =>
          throw new PickleException(s"Ratings id ${obj.toString} exceeds " +
            s"max integer value of ${Int.MaxValue}", ex)
      }
    }
  }

  var initialized = false
  // This should be called before trying to serialize any above classes
  // In cluster mode, this should be put in the closure
  override def initialize(): Unit = {
    SerDeUtil.initialize()
    synchronized {
      if (!initialized) {
        new DenseVectorPickler().register()
        new DenseMatrixPickler().register()
        new SparseMatrixPickler().register()
        new SparseVectorPickler().register()
        new LabeledPointPickler().register()
        new RatingPickler().register()
        initialized = true
      }
    }
  }
  // will not called in Executor automatically
  initialize()
}

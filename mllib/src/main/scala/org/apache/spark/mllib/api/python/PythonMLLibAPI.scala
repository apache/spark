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
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.reflect.ClassTag

import net.razorvine.pickle._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.random.{RandomRDDs => RG}
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.stat.correlation.CorrelationNames
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.mllib.tree.{RandomForest, DecisionTree}
import org.apache.spark.mllib.tree.configuration.{Algo, Strategy}
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.model.{RandomForestModel, DecisionTreeModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * The Java stubs necessary for the Python mllib bindings.
 */
@DeveloperApi
class PythonMLLibAPI extends Serializable {


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

  private def trainRegressionModel(
      learner: GeneralizedLinearAlgorithm[_ <: GeneralizedLinearModel],
      data: JavaRDD[LabeledPoint],
      initialWeights: Vector): JList[Object] = {
    try {
      val model = learner.run(data.rdd.persist(StorageLevel.MEMORY_AND_DISK), initialWeights)
      List(model.weights, model.intercept).map(_.asInstanceOf[Object]).asJava
    } finally {
      data.rdd.unpersist(blocking = false)
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
      intercept: Boolean): JList[Object] = {
    val lrAlg = new LinearRegressionWithSGD()
    lrAlg.setIntercept(intercept)
    lrAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
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
      initialWeights: Vector): JList[Object] = {
    val lassoAlg = new LassoWithSGD()
    lassoAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
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
      initialWeights: Vector): JList[Object] = {
    val ridgeAlg = new RidgeRegressionWithSGD()
    ridgeAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
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
      intercept: Boolean): JList[Object] = {
    val SVMAlg = new SVMWithSGD()
    SVMAlg.setIntercept(intercept)
    SVMAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
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
      intercept: Boolean): JList[Object] = {
    val LogRegAlg = new LogisticRegressionWithSGD()
    LogRegAlg.setIntercept(intercept)
    LogRegAlg.optimizer
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFraction)
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
      tolerance: Double): JList[Object] = {
    val LogRegAlg = new LogisticRegressionWithLBFGS()
    LogRegAlg.setIntercept(intercept)
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
  def trainNaiveBayes(
      data: JavaRDD[LabeledPoint],
      lambda: Double): JList[Object] = {
    val model = NaiveBayes.train(data.rdd, lambda)
    List(Vectors.dense(model.labels), Vectors.dense(model.pi), model.theta).
      map(_.asInstanceOf[Object]).asJava
  }

  /**
   * Java stub for Python mllib KMeans.train()
   */
  def trainKMeansModel(
      data: JavaRDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String): KMeansModel = {
    val kMeansAlg = new KMeans()
      .setK(k)
      .setMaxIterations(maxIterations)
      .setRuns(runs)
      .setInitializationMode(initializationMode)
    try {
      kMeansAlg.run(data.rdd.persist(StorageLevel.MEMORY_AND_DISK))
    } finally {
      data.rdd.unpersist(blocking = false)
    }
  }

  /**
   * A Wrapper of MatrixFactorizationModel to provide helpfer method for Python
   */
  private[python] class MatrixFactorizationModelWrapper(model: MatrixFactorizationModel)
    extends MatrixFactorizationModel(model.rank, model.userFeatures, model.productFeatures) {

    def predict(userAndProducts: JavaRDD[Array[Any]]): RDD[Rating] =
      predict(SerDe.asTupleRDD(userAndProducts.rdd))

    def getUserFeatures = SerDe.fromTuple2RDD(userFeatures.asInstanceOf[RDD[(Any, Any)]])

    def getProductFeatures = SerDe.fromTuple2RDD(productFeatures.asInstanceOf[RDD[(Any, Any)]])

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

    val model =  als.run(ratingsJRDD.rdd)
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

    val model =  als.run(ratingsJRDD.rdd)
    new MatrixFactorizationModelWrapper(model)
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
   * Java stub for IDF.fit(). This stub returns a
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
   * @return A handle to java Word2VecModelWrapper instance at python side
   */
  def trainWord2Vec(
      dataJRDD: JavaRDD[java.util.ArrayList[String]],
      vectorSize: Int,
      learningRate: Double,
      numPartitions: Int,
      numIterations: Int,
      seed: Long): Word2VecModelWrapper = {
    val word2vec = new Word2Vec()
      .setVectorSize(vectorSize)
      .setLearningRate(learningRate)
      .setNumPartitions(numPartitions)
      .setNumIterations(numIterations)
      .setSeed(seed)
    try {
      val model = word2vec.fit(dataJRDD.rdd.persist(StorageLevel.MEMORY_AND_DISK_SER))
      new Word2VecModelWrapper(model)
    } finally {
      dataJRDD.rdd.unpersist(blocking = false)
    }
  }

  private[python] class Word2VecModelWrapper(model: Word2VecModel) {
    def transform(word: String): Vector = {
      model.transform(word)
    }

    /**
     * Transforms an RDD of words to its vector representation
     * @param rdd an RDD of words
     * @return an RDD of vector representations of words
     */
    def transform(rdd: JavaRDD[String]): JavaRDD[Vector] = {
      rdd.rdd.map(model.transform)
    }

    def findSynonyms(word: String, num: Int): JList[Object] = {
      val vec = transform(word)
      findSynonyms(vec, num)
    }

    def findSynonyms(vector: Vector, num: Int): JList[Object] = {
      val result = model.findSynonyms(vector, num)
      val similarity = Vectors.dense(result.map(_._2))
      val words = result.map(_._1)
      List(words, similarity).map(_.asInstanceOf[Object]).asJava
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
      numClassesForClassification = numClasses,
      maxBins = maxBins,
      categoricalFeaturesInfo = categoricalFeaturesInfo.asScala.toMap,
      minInstancesPerNode = minInstancesPerNode,
      minInfoGain = minInfoGain)
    try {
      DecisionTree.train(data.rdd.persist(StorageLevel.MEMORY_AND_DISK), strategy)
    } finally {
      data.rdd.unpersist(blocking = false)
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
      seed: Int): RandomForestModel = {

    val algo = Algo.fromString(algoStr)
    val impurity = Impurities.fromString(impurityStr)
    val strategy = new Strategy(
      algo = algo,
      impurity = impurity,
      maxDepth = maxDepth,
      numClassesForClassification = numClasses,
      maxBins = maxBins,
      categoricalFeaturesInfo = categoricalFeaturesInfo.asScala.toMap)
    val cached = data.rdd.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      if (algo == Algo.Classification) {
        RandomForest.trainClassifier(cached, strategy, numTrees, featureSubsetStrategy, seed)
      } else {
        RandomForest.trainRegressor(cached, strategy, numTrees, featureSubsetStrategy, seed)
      }
    } finally {
      cached.unpersist(blocking = false)
    }
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

}

/**
 * SerDe utility functions for PythonMLLibAPI.
 */
private[spark] object SerDe extends Serializable {

  val PYSPARK_PACKAGE = "pyspark.mllib"

  /**
   * Base class used for pickle
   */
  private[python] abstract class BasePickler[T: ClassTag]
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
        out.write((module + "\n" + name + "\n").getBytes)
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

    private[python] def saveState(obj: Object, out: OutputStream, pickler: Pickler)
  }

  // Pickler for DenseVector
  private[python] class DenseVectorPickler extends BasePickler[DenseVector] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler) = {
      val vector: DenseVector = obj.asInstanceOf[DenseVector]
      saveObjects(out, pickler, vector.toArray)
    }

    def construct(args: Array[Object]): Object = {
      require(args.length == 1)
      if (args.length != 1) {
        throw new PickleException("should be 1")
      }
      new DenseVector(args(0).asInstanceOf[Array[Double]])
    }
  }

  // Pickler for DenseMatrix
  private[python] class DenseMatrixPickler extends BasePickler[DenseMatrix] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler) = {
      val m: DenseMatrix = obj.asInstanceOf[DenseMatrix]
      saveObjects(out, pickler, m.numRows, m.numCols, m.values)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 3) {
        throw new PickleException("should be 3")
      }
      new DenseMatrix(args(0).asInstanceOf[Int], args(1).asInstanceOf[Int],
        args(2).asInstanceOf[Array[Double]])
    }
  }

  // Pickler for SparseVector
  private[python] class SparseVectorPickler extends BasePickler[SparseVector] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler) = {
      val v: SparseVector = obj.asInstanceOf[SparseVector]
      saveObjects(out, pickler, v.size, v.indices, v.values)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 3) {
        throw new PickleException("should be 3")
      }
      new SparseVector(args(0).asInstanceOf[Int], args(1).asInstanceOf[Array[Int]],
        args(2).asInstanceOf[Array[Double]])
    }
  }

  // Pickler for LabeledPoint
  private[python] class LabeledPointPickler extends BasePickler[LabeledPoint] {

    def saveState(obj: Object, out: OutputStream, pickler: Pickler) = {
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

    def saveState(obj: Object, out: OutputStream, pickler: Pickler) = {
      val rating: Rating = obj.asInstanceOf[Rating]
      saveObjects(out, pickler, rating.user, rating.product, rating.rating)
    }

    def construct(args: Array[Object]): Object = {
      if (args.length != 3) {
        throw new PickleException("should be 3")
      }
      new Rating(args(0).asInstanceOf[Int], args(1).asInstanceOf[Int],
        args(2).asInstanceOf[Double])
    }
  }

  var initialized = false
  // This should be called before trying to serialize any above classes
  // In cluster mode, this should be put in the closure
  def initialize(): Unit = {
    SerDeUtil.initialize()
    synchronized {
      if (!initialized) {
        new DenseVectorPickler().register()
        new DenseMatrixPickler().register()
        new SparseVectorPickler().register()
        new LabeledPointPickler().register()
        new RatingPickler().register()
        initialized = true
      }
    }
  }
  // will not called in Executor automatically
  initialize()

  def dumps(obj: AnyRef): Array[Byte] = {
    new Pickler().dumps(obj)
  }

  def loads(bytes: Array[Byte]): AnyRef = {
    new Unpickler().loads(bytes)
  }

  /* convert object into Tuple */
  def asTupleRDD(rdd: RDD[Array[Any]]): RDD[(Int, Int)] = {
    rdd.map(x => (x(0).asInstanceOf[Int], x(1).asInstanceOf[Int]))
  }

  /* convert RDD[Tuple2[,]] to RDD[Array[Any]] */
  def fromTuple2RDD(rdd: RDD[(Any, Any)]): RDD[Array[Any]]  = {
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
        if (batched) {
          obj.asInstanceOf[JArrayList[_]].asScala
        } else {
          Seq(obj)
        }
      }
    }.toJavaRDD()
  }
}

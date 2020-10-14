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

package org.apache.spark.mllib.tree.model

import scala.collection.mutable

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.EnsembleCombiningStrategy._
import org.apache.spark.mllib.tree.loss.Loss
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

/**
 * Represents a random forest model.
 *
 * @param algo algorithm for the ensemble model, either Classification or Regression
 * @param trees tree ensembles
 */
@Since("1.2.0")
class RandomForestModel @Since("1.2.0") (
    @Since("1.2.0") override val algo: Algo,
    @Since("1.2.0") override val trees: Array[DecisionTreeModel])
  extends TreeEnsembleModel(algo, trees, Array.fill(trees.length)(1.0),
    combiningStrategy = if (algo == Classification) Vote else Average)
  with Saveable {

  require(trees.forall(_.algo == algo))

  /**
   *
   * @param sc  Spark context used to save model data.
   * @param path  Path specifying the directory in which to save this model.
   *              If the directory already exists, this method throws an exception.
   */
  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {
    TreeEnsembleModel.SaveLoadV1_0.save(sc, path, this,
      RandomForestModel.SaveLoadV1_0.thisClassName)
  }
}

@Since("1.3.0")
object RandomForestModel extends Loader[RandomForestModel] {

  /**
   *
   * @param sc  Spark context used for loading model files.
   * @param path  Path specifying the directory to which the model was saved.
   * @return  Model instance
   */
  @Since("1.3.0")
  override def load(sc: SparkContext, path: String): RandomForestModel = {
    val (loadedClassName, version, jsonMetadata) = Loader.loadMetadata(sc, path)
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val metadata = TreeEnsembleModel.SaveLoadV1_0.readMetadata(jsonMetadata)
        assert(metadata.treeWeights.forall(_ == 1.0))
        val trees =
          TreeEnsembleModel.SaveLoadV1_0.loadTrees(sc, path, metadata.treeAlgo)
        new RandomForestModel(Algo.fromString(metadata.algo), trees)
      case _ => throw new Exception(s"RandomForestModel.load did not recognize model" +
        s" with (className, format version): ($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }

  private object SaveLoadV1_0 {
    // Hard-code class name string in case it changes in the future
    def thisClassName: String = "org.apache.spark.mllib.tree.model.RandomForestModel"
  }

}

/**
 * Represents a gradient boosted trees model.
 *
 * @param algo algorithm for the ensemble model, either Classification or Regression
 * @param trees tree ensembles
 * @param treeWeights tree ensemble weights
 */
@Since("1.2.0")
class GradientBoostedTreesModel @Since("1.2.0") (
    @Since("1.2.0") override val algo: Algo,
    @Since("1.2.0") override val trees: Array[DecisionTreeModel],
    @Since("1.2.0") override val treeWeights: Array[Double])
  extends TreeEnsembleModel(algo, trees, treeWeights, combiningStrategy = Sum)
  with Saveable {

  require(trees.length == treeWeights.length)

  /**
   * @param sc  Spark context used to save model data.
   * @param path  Path specifying the directory in which to save this model.
   *              If the directory already exists, this method throws an exception.
   */
  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {
    TreeEnsembleModel.SaveLoadV1_0.save(sc, path, this,
      GradientBoostedTreesModel.SaveLoadV1_0.thisClassName)
  }

  /**
   * Method to compute error or loss for every iteration of gradient boosting.
   * @param data RDD of [[org.apache.spark.mllib.regression.LabeledPoint]]
   * @param loss evaluation metric.
   * @return an array with index i having the losses or errors for the ensemble
   *         containing the first i+1 trees
   */
  @Since("1.4.0")
  def evaluateEachIteration(
      data: RDD[LabeledPoint],
      loss: Loss): Array[Double] = {

    val sc = data.sparkContext
    val remappedData = algo match {
      case Classification => data.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
      case _ => data
    }

    val broadcastTrees = sc.broadcast(trees)
    val localTreeWeights = treeWeights
    val treesIndices = trees.indices

    val dataCount = remappedData.count()
    val evaluation = remappedData.map { point =>
      treesIndices
        .map(idx => broadcastTrees.value(idx).predict(point.features) * localTreeWeights(idx))
        .scanLeft(0.0)(_ + _).drop(1)
        .map(prediction => loss.computeError(prediction, point.label))
    }
    .aggregate(treesIndices.map(_ => 0.0))(
      (aggregated, row) => treesIndices.map(idx => aggregated(idx) + row(idx)),
      (a, b) => treesIndices.map(idx => a(idx) + b(idx)))
    .map(_ / dataCount)

    broadcastTrees.destroy()
    evaluation.toArray
  }
}

/**
 */
@Since("1.3.0")
object GradientBoostedTreesModel extends Loader[GradientBoostedTreesModel] {

  /**
   * Compute the initial predictions and errors for a dataset for the first
   * iteration of gradient boosting.
   * @param data: training data.
   * @param initTreeWeight: learning rate assigned to the first tree.
   * @param initTree: first DecisionTreeModel.
   * @param loss: evaluation metric.
   * @return an RDD with each element being a zip of the prediction and error
   *         corresponding to every sample.
   */
  @Since("1.4.0")
  def computeInitialPredictionAndError(
      data: RDD[LabeledPoint],
      initTreeWeight: Double,
      initTree: DecisionTreeModel,
      loss: Loss): RDD[(Double, Double)] = {
    data.map { lp =>
      val pred = initTreeWeight * initTree.predict(lp.features)
      val error = loss.computeError(pred, lp.label)
      (pred, error)
    }
  }

  /**
   * Update a zipped predictionError RDD
   * (as obtained with computeInitialPredictionAndError)
   * @param data: training data.
   * @param predictionAndError: predictionError RDD
   * @param treeWeight: Learning rate.
   * @param tree: Tree using which the prediction and error should be updated.
   * @param loss: evaluation metric.
   * @return an RDD with each element being a zip of the prediction and error
   *         corresponding to each sample.
   */
  @Since("1.4.0")
  def updatePredictionError(
    data: RDD[LabeledPoint],
    predictionAndError: RDD[(Double, Double)],
    treeWeight: Double,
    tree: DecisionTreeModel,
    loss: Loss): RDD[(Double, Double)] = {

    val newPredError = data.zip(predictionAndError).mapPartitions { iter =>
      iter.map { case (lp, (pred, error)) =>
        val newPred = pred + tree.predict(lp.features) * treeWeight
        val newError = loss.computeError(newPred, lp.label)
        (newPred, newError)
      }
    }
    newPredError
  }

  /**
   * @param sc  Spark context used for loading model files.
   * @param path  Path specifying the directory to which the model was saved.
   * @return  Model instance
   */
  @Since("1.3.0")
  override def load(sc: SparkContext, path: String): GradientBoostedTreesModel = {
    val (loadedClassName, version, jsonMetadata) = Loader.loadMetadata(sc, path)
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val metadata = TreeEnsembleModel.SaveLoadV1_0.readMetadata(jsonMetadata)
        assert(metadata.combiningStrategy == Sum.toString)
        val trees =
          TreeEnsembleModel.SaveLoadV1_0.loadTrees(sc, path, metadata.treeAlgo)
        new GradientBoostedTreesModel(Algo.fromString(metadata.algo), trees, metadata.treeWeights)
      case _ => throw new Exception(s"GradientBoostedTreesModel.load did not recognize model" +
        s" with (className, format version): ($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }

  private object SaveLoadV1_0 {
    // Hard-code class name string in case it changes in the future
    def thisClassName: String = "org.apache.spark.mllib.tree.model.GradientBoostedTreesModel"
  }

}

/**
 * Represents a tree ensemble model.
 *
 * @param algo algorithm for the ensemble model, either Classification or Regression
 * @param trees tree ensembles
 * @param treeWeights tree ensemble weights
 * @param combiningStrategy strategy for combining the predictions, not used for regression.
 */
private[tree] sealed class TreeEnsembleModel(
    protected val algo: Algo,
    protected val trees: Array[DecisionTreeModel],
    protected val treeWeights: Array[Double],
    protected val combiningStrategy: EnsembleCombiningStrategy) extends Serializable {

  require(numTrees > 0, "TreeEnsembleModel cannot be created without trees.")

  private val sumWeights = math.max(treeWeights.sum, 1e-15)

  /**
   * Predicts for a single data point using the weighted sum of ensemble predictions.
   *
   * @param features array representing a single data point
   * @return predicted category from the trained model
   */
  private def predictBySumming(features: Vector): Double = {
    val treePredictions = trees.map(_.predict(features))
    blas.ddot(numTrees, treePredictions, 1, treeWeights, 1)
  }

  /**
   * Classifies a single data point based on (weighted) majority votes.
   */
  private def predictByVoting(features: Vector): Double = {
    val votes = mutable.Map.empty[Int, Double]
    trees.iterator.zip(treeWeights.iterator).foreach { case (tree, weight) =>
      val prediction = tree.predict(features).toInt
      votes(prediction) = votes.getOrElse(prediction, 0.0) + weight
    }
    votes.maxBy(_._2)._1
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param features array representing a single data point
   * @return predicted category from the trained model
   */
  def predict(features: Vector): Double = {
    (algo, combiningStrategy) match {
      case (Regression, Sum) =>
        predictBySumming(features)
      case (Regression, Average) =>
        predictBySumming(features) / sumWeights
      case (Classification, Sum) => // binary classification
        val prediction = predictBySumming(features)
        // TODO: predicted labels are +1 or -1 for GBT. Need a better way to store this info.
        if (prediction > 0.0) 1.0 else 0.0
      case (Classification, Vote) =>
        predictByVoting(features)
      case _ =>
        throw new IllegalArgumentException(
          "TreeEnsembleModel given unsupported (algo, combiningStrategy) combination: " +
            s"($algo, $combiningStrategy).")
    }
  }

  /**
   * Predict values for the given data set.
   *
   * @param features RDD representing data points to be predicted
   * @return RDD[Double] where each entry contains the corresponding prediction
   */
  def predict(features: RDD[Vector]): RDD[Double] = features.map(x => predict(x))

  /**
   * Java-friendly version of `org.apache.spark.mllib.tree.model.TreeEnsembleModel.predict`.
   */
  def predict(features: JavaRDD[Vector]): JavaRDD[java.lang.Double] = {
    predict(features.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Double]]
  }

  /**
   * Print a summary of the model.
   */
  override def toString: String = {
    algo match {
      case Classification =>
        s"TreeEnsembleModel classifier with $numTrees trees\n"
      case Regression =>
        s"TreeEnsembleModel regressor with $numTrees trees\n"
      case _ => throw new IllegalArgumentException(
        s"TreeEnsembleModel given unknown algo parameter: $algo.")
    }
  }

  /**
   * Print the full model to a string.
   */
  def toDebugString: String = {
    val header = toString + "\n"
    header + trees.zipWithIndex.map { case (tree, treeIndex) =>
      s"  Tree $treeIndex:\n" + tree.topNode.subtreeToString(4)
    }.fold("")(_ + _)
  }

  /**
   * Get number of trees in ensemble.
   */
  def numTrees: Int = trees.length

  /**
   * Get total number of nodes, summed over all trees in the ensemble.
   */
  def totalNumNodes: Int = trees.map(_.numNodes).sum
}

private[tree] object TreeEnsembleModel extends Logging {

  object SaveLoadV1_0 {

    import org.apache.spark.mllib.tree.model.DecisionTreeModel.SaveLoadV1_0.{NodeData, constructTrees}

    def thisFormatVersion: String = "1.0"

    case class Metadata(
        algo: String,
        treeAlgo: String,
        combiningStrategy: String,
        treeWeights: Array[Double])

    /**
     * Model data for model import/export.
     * We have to duplicate NodeData here since Spark SQL does not yet support extracting subfields
     * of nested fields; once that is possible, we can use something like:
     *  case class EnsembleNodeData(treeId: Int, node: NodeData),
     *  where NodeData is from DecisionTreeModel.
     */
    case class EnsembleNodeData(treeId: Int, node: NodeData)

    def save(sc: SparkContext, path: String, model: TreeEnsembleModel, className: String): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      // SPARK-6120: We do a hacky check here so users understand why save() is failing
      //             when they run the ML guide example.
      // TODO: Fix this issue for real.
      val memThreshold = 768
      if (sc.isLocal) {
        val driverMemory = sc.getConf.getOption("spark.driver.memory")
          .orElse(Option(System.getenv("SPARK_DRIVER_MEMORY")))
          .map(Utils.memoryStringToMb)
          .getOrElse(Utils.DEFAULT_DRIVER_MEM_MB)
        if (driverMemory <= memThreshold) {
          logWarning(s"$className.save() was called, but it may fail because of too little" +
            s" driver memory (${driverMemory}m)." +
            s"  If failure occurs, try setting driver-memory ${memThreshold}m (or larger).")
        }
      } else {
        if (sc.executorMemory <= memThreshold) {
          logWarning(s"$className.save() was called, but it may fail because of too little" +
            s" executor memory (${sc.executorMemory}m)." +
            s"  If failure occurs try setting executor-memory ${memThreshold}m (or larger).")
        }
      }

      // Create JSON metadata.
      implicit val format = DefaultFormats
      val ensembleMetadata = Metadata(model.algo.toString, model.trees(0).algo.toString,
        model.combiningStrategy.toString, model.treeWeights)
      val metadata = compact(render(
        ("class" -> className) ~ ("version" -> thisFormatVersion) ~
          ("metadata" -> Extraction.decompose(ensembleMetadata))))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val dataRDD = sc.parallelize(model.trees.zipWithIndex).flatMap { case (tree, treeId) =>
        tree.topNode.subtreeIterator.toSeq.map(node => NodeData(treeId, node))
      }
      spark.createDataFrame(dataRDD).write.parquet(Loader.dataPath(path))
    }

    /**
     * Read metadata from the loaded JSON metadata.
     */
    def readMetadata(metadata: JValue): Metadata = {
      implicit val formats = DefaultFormats
      (metadata \ "metadata").extract[Metadata]
    }

    /**
     * Load trees for an ensemble, and return them in order.
     * @param path path to load the model from
     * @param treeAlgo Algorithm for individual trees (which may differ from the ensemble's
     *                 algorithm).
     */
    def loadTrees(
        sc: SparkContext,
        path: String,
        treeAlgo: String): Array[DecisionTreeModel] = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      import spark.implicits._
      val nodes = spark.read.parquet(Loader.dataPath(path)).map(NodeData.apply)
      val trees = constructTrees(nodes.rdd)
      trees.map(new DecisionTreeModel(_, Algo.fromString(treeAlgo)))
    }
  }

}

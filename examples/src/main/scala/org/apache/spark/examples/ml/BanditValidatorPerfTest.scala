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

package org.apache.spark.examples.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.bandit._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object BanditValidatorPerfTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[8]").setAppName("BanditPerfTest")

    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._

    //val data = MLUtils.loadLibSVMFile(sc, "/Users/panda/data/small_datasets/adult.tst").map {
    val data = MLUtils.loadLibSVMFile(sc, "/Users/panda/data/small_datasets/australian", -1, 2).map {
      case LabeledPoint(label: Double, features: Vector) =>
        LabeledPoint(if (label < 0) 0 else label, features)
    }

    val dataset = data.toDF()

    val splits = dataset.randomSplit(Array(0.7, 0.3))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val singleClassifier = new LogisticRegression().setMaxIter(3)

    val params = new ParamGridBuilder()
      .addGrid(singleClassifier.elasticNetParam, Array(1.0, 0.1, 0.01))
      .addGrid(singleClassifier.regParam, Array(0.1, 0.01))
      .addGrid(singleClassifier.fitIntercept, Array(true, false))
      .build()

    val eval = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

    val banditVal = new BanditValidator[LogisticRegressionModel]()
      .setEstimator(singleClassifier)
      .setEstimatorParamMaps(params)
      .setNumFolds(3)
      .setEvaluator(eval)

    val pathPrefix = "/Users/panda/data/small_datasets/australian-results-fixed"

    Array(
      new StaticSearch,
      new NaiveSearch,
      //new SuccessiveEliminationSearch,
      new ExponentialWeightsSearch
      //new LILUCBSearch,
      //new LUCBSearch,
      //new SuccessiveHalvingSearch,
      //new SuccessiveRejectSearch
    ).foreach { search =>
      banditVal.setSearchStrategy(search)
      val model = banditVal.fit(training)
      val auc = eval.evaluate(model.transform(test))

      val part1 = s"${search.getClass.getSimpleName} -> ${auc}"
      val part2 = banditVal.readableSummary()
      val part3 = banditVal.paintableSummary()

      sc.parallelize(Array(part1) ++ part2).repartition(1)
        .saveAsTextFile(s"${pathPrefix}/result-${search.getClass.getSimpleName}")

      part3.foreach { case (fold, result) =>
        val strResult = result.map { case (iter, arm, training, validation) =>
          s"$iter,$arm,$training,$validation"
        }
        sc.parallelize(strResult).repartition(1)
          .saveAsTextFile(s"${pathPrefix}/result-${search.getClass.getSimpleName}/fold-${fold}")
      }
    }
    sc.stop()
  }
}

object Utils {
  def loadLibSVMFile(
      sc: SparkContext,
      path: String,
      numFeatures: Int,
      minPartitions: Int): RDD[LabeledPoint] = {
    val parsed = sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map { line =>
        val items = line.split(' ')
        val label = items.head.toDouble
        val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
          val indexAndValue = item.split(':')
          val index = indexAndValue(0).toInt
          val value = indexAndValue(1).toDouble
          (index, value)
        }.unzip

        // check if indices are one-based and in ascending order
        var previous = -1
        var i = 0
        val indicesLength = indices.length
        while (i < indicesLength) {
          val current = indices(i)
          require(current > previous, "indices should be one-based and in ascending order" )
          previous = current
          i += 1
        }

        (label, indices.toArray, values.toArray)
      }

    // Determine number of features.
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      parsed.persist(StorageLevel.MEMORY_ONLY)
      parsed.map { case (label, indices, values) =>
        indices.lastOption.getOrElse(0)
      }.reduce(math.max) + 1
    }

    parsed.map { case (label, indices, values) =>
      LabeledPoint(label, Vectors.sparse(d, indices, values))
    }
  }
}

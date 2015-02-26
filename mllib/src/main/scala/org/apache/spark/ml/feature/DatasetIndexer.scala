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

package org.apache.spark.ml.feature

import org.apache.spark.ml.{UnaryTransformer, Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.mllib.feature.{DatasetIndexer => OldDatasetIndexer}
import org.apache.spark.sql.SchemaRDD


private[ml] trait DatasetIndexerParams extends Params with HasInputCol with HasOutputCol {

  val maxCategories = new IntParam(this, "maxCategories",
    "Threshold for the number of values a categorical feature can take." +
      " If a feature is found to have > maxCategories values, then it is declared continuous.",
    Some(20))

  def getMaxCategories: Int = get(maxCategories)
}

/**
 * :: Experimental ::
 * Class for indexing columns in a dataset.
 *
 * This helps process a dataset of unknown vectors into a dataset with some continuous features
 * and some categorical features. The choice between continuous and categorical is based upon
 * a maxCategories parameter.
 *
 * This can also map categorical feature values to 0-based indices.
 *
 * Usage:
 *   val myData1: RDD[Vector] = ...
 *   val myData2: RDD[Vector] = ...
 *   val datasetIndexer = new DatasetIndexer(maxCategories)
 *   datasetIndexer.fit(myData1)
 *   val indexedData1: RDD[Vector] = datasetIndexer.transform(myData1)
 *   datasetIndexer.fit(myData2)
 *   val indexedData2: RDD[Vector] = datasetIndexer.transform(myData2)
 *   val categoricalFeaturesInfo: Map[Int, Int] = datasetIndexer.getCategoricalFeaturesInfo()
 *
 * TODO: Add warning if a categorical feature has only 1 category.
 *
 * TODO: Add option for allowing unknown categories:
 *       Parameter allowUnknownCategories:
 *        If true, then handle unknown categories during `transform`
 *        by assigning them to an extra category index.
 *        That unknown category index should be index 1; this will allow maintaining sparsity
 *        (reserving index 0 for value 0.0), and it will allow category indices to remain fixed
 *        even if more categories are added later.
 */
class DatasetIndexer extends Estimator[DatasetIndexerModel] with DatasetIndexerParams {

  def setMaxCategories(value: Int): this.type = set(maxCategories, value)

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): DatasetIndexerModel = {
    val map = this.paramMap ++ paramMap
    val indexer = new OldDatasetIndexer(maxCategories = map(maxCategories))

  }
}

class DatasetIndexerModel extends Model[DatasetIndexerModel] with DatasetIndexerParams {

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {

  }
}

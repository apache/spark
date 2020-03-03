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

package org.apache.spark.ml.stat

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.stat.{Statistics => OldStatistics}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * API for correlation functions in MLlib, compatible with DataFrames and Datasets.
 *
 * The functions in this package generalize the functions in [[org.apache.spark.sql.Dataset#stat]]
 * to spark.ml's Vector types.
 */
@Since("2.2.0")
object Correlation {

  /**
   * Compute the correlation matrix for the input Dataset of Vectors using the specified method.
   * Methods currently supported: `pearson` (default), `spearman`.
   *
   * @param dataset A dataset or a dataframe
   * @param column The name of the column of vectors for which the correlation coefficient needs
   *               to be computed. This must be a column of the dataset, and it must contain
   *               Vector objects.
   * @param method String specifying the method to use for computing correlation.
   *               Supported: `pearson` (default), `spearman`
   * @return A dataframe that contains the correlation matrix of the column of vectors. This
   *         dataframe contains a single row and a single column of name
   *         `$METHODNAME($COLUMN)`.
   * @throws IllegalArgumentException if the column is not a valid column in the dataset, or if
   *                                  the content of this column is not of type Vector.
   *
   *  Here is how to access the correlation coefficient:
   *  {{{
   *    val data: Dataset[Vector] = ...
   *    val Row(coeff: Matrix) = Correlation.corr(data, "value").head
   *    // coeff now contains the Pearson correlation matrix.
   *  }}}
   *
   * @note For Spearman, a rank correlation, we need to create an RDD[Double] for each column
   * and sort it in order to retrieve the ranks and then join the columns back into an RDD[Vector],
   * which is fairly costly. Cache the input Dataset before calling corr with `method = "spearman"`
   * to avoid recomputing the common lineage.
   */
  @Since("2.2.0")
  def corr(dataset: Dataset[_], column: String, method: String): DataFrame = {
    val rdd = dataset.select(column).rdd.map {
      case Row(v: Vector) => OldVectors.fromML(v)
    }
    val oldM = OldStatistics.corr(rdd, method)
    val name = s"$method($column)"
    val schema = StructType(Array(StructField(name, SQLDataTypes.MatrixType, nullable = false)))
    dataset.sparkSession.createDataFrame(Seq(Row(oldM.asML)).asJava, schema)
  }

  /**
   * Compute the Pearson correlation matrix for the input Dataset of Vectors.
   */
  @Since("2.2.0")
  def corr(dataset: Dataset[_], column: String): DataFrame = {
    corr(dataset, column, "pearson")
  }
}

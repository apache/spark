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

package org.apache.spark.sql

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.stat.{ContingencyTable, FrequentItems}

/**
 * :: Experimental ::
 * Statistic functions for [[DataFrame]]s.
 */
@Experimental
final class DataFrameStatFunctions private[sql](df: DataFrame) {

  /**
   * Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
   * The number of distinct values for each column should be less than Int.MaxValue. The first
   * column of each row will be the distinct values of `col1` and the column names will be the
   * distinct values of `col2` sorted in lexicographical order. Counts will be returned as `Long`s.
   *
   * @param col1 The name of the first column.
   * @param col2 The name of the second column.
   * @return A Local DataFrame containing the table
   */
  def crosstab(col1: String, col2: String): DataFrame = {
    ContingencyTable.crossTabulate(df, col1, col2)
  }

  /**
   * Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * [[http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou]].
   * The `support` should be greater than 1e-4.
   *
   * @param cols the names of the columns to search frequent items in.
   * @param support The minimum frequency for an item to be considered `frequent`. Should be greater
   *                than 1e-4.
   * @return A Local DataFrame with the Array of frequent items for each column.
   */
  def freqItems(cols: Array[String], support: Double): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, support)
  }

  /**
   * Runs `freqItems` with a default `support` of 1%.
   *
   * @param cols the names of the columns to search frequent items in.
   * @return A Local DataFrame with the Array of frequent items for each column.
   */
  def freqItems(cols: Array[String]): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, 0.01)
  }

  /**
   * Python friendly implementation for `freqItems`
   */
  def freqItems(cols: List[String], support: Double): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, support)
  }

  /**
   * Python friendly implementation for `freqItems` with a default `support` of 1%.
   */
  def freqItems(cols: List[String]): DataFrame = {
    FrequentItems.singlePassFreqItems(df, cols, 0.01)
  }
}

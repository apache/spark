#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# stats.R - Statistic functions for DataFrames.

setOldClass("jobj")

#' crosstab
#'
#' Computes a pair-wise frequency table of the given columns. Also known as a contingency
#' table. The number of distinct values for each column should be less than 1e4. At most 1e6
#' non-zero pair frequencies will be returned.
#'
#' @param col1 name of the first column. Distinct items will make the first item of each row.
#' @param col2 name of the second column. Distinct items will make the column names of the output.
#' @return a local R data.frame representing the contingency table. The first column of each row
#'         will be the distinct values of `col1` and the column names will be the distinct values
#'         of `col2`. The name of the first column will be `$col1_$col2`. Pairs that have no
#'         occurrences will have zero as their counts.
#'
#' @rdname statfunctions
#' @name crosstab
#' @export
#' @examples
#' \dontrun{
#' df <- jsonFile(sqlContext, "/path/to/file.json")
#' ct <- crosstab(df, "title", "gender")
#' }
setMethod("crosstab",
          signature(x = "DataFrame", col1 = "character", col2 = "character"),
          function(x, col1, col2) {
            statFunctions <- callJMethod(x@sdf, "stat")
            sct <- callJMethod(statFunctions, "crosstab", col1, col2)
            collect(dataFrame(sct))
          })

#' cov
#'
#' Calculate the sample covariance of two numerical columns of a DataFrame.
#'
#' @param x A SparkSQL DataFrame
#' @param col1 the name of the first column
#' @param col2 the name of the second column
#' @return the covariance of the two columns.
#'
#' @rdname statfunctions
#' @name cov
#' @export
#' @examples
#'\dontrun{
#' df <- jsonFile(sqlContext, "/path/to/file.json")
#' cov <- cov(df, "title", "gender")
#' }
setMethod("cov",
          signature(x = "DataFrame", col1 = "character", col2 = "character"),
          function(x, col1, col2) {
            statFunctions <- callJMethod(x@sdf, "stat")
            callJMethod(statFunctions, "cov", col1, col2)
          })

#' corr
#'
#' Calculates the correlation of two columns of a DataFrame.
#' Currently only supports the Pearson Correlation Coefficient.
#' For Spearman Correlation, consider using RDD methods found in MLlib's Statistics.
#'
#' @param x A SparkSQL DataFrame
#' @param col1 the name of the first column
#' @param col2 the name of the second column
#' @param method Optional. A character specifying the method for calculating the correlation.
#'               only "pearson" is allowed now.
#' @return The Pearson Correlation Coefficient as a Double.
#'
#' @rdname statfunctions
#' @name corr
#' @export
#' @examples
#'\dontrun{
#' df <- jsonFile(sqlContext, "/path/to/file.json")
#' corr <- corr(df, "title", "gender")
#' corr <- corr(df, "title", "gender", method = "pearson")
#' }
setMethod("corr",
          signature(x = "DataFrame"),
          function(x, col1, col2, method = "pearson") {
            stopifnot(class(col1) == "character" && class(col2) == "character")
            statFunctions <- callJMethod(x@sdf, "stat")
            callJMethod(statFunctions, "corr", col1, col2, method)
          })

#' freqItems
#'
#' Finding frequent items for columns, possibly with false positives.
#' Using the frequent element count algorithm described in
#' \url{http://dx.doi.org/10.1145/762471.762473}, proposed by Karp, Schenker, and Papadimitriou.
#'
#' @param x A SparkSQL DataFrame.
#' @param cols A vector column names to search frequent items in.
#' @param support (Optional) The minimum frequency for an item to be considered `frequent`.
#'                Should be greater than 1e-4. Default support = 0.01.
#' @return a local R data.frame with the frequent items in each column
#'
#' @rdname statfunctions
#' @name freqItems
#' @export
#' @examples
#' \dontrun{
#' df <- jsonFile(sqlContext, "/path/to/file.json")
#' fi = freqItems(df, c("title", "gender"))
#' }
setMethod("freqItems", signature(x = "DataFrame", cols = "character"),
          function(x, cols, support = 0.01) {
            statFunctions <- callJMethod(x@sdf, "stat")
            sct <- callJMethod(statFunctions, "freqItems", as.list(cols), support)
            collect(dataFrame(sct))
          })

#' sampleBy
#'
#' Returns a stratified sample without replacement based on the fraction given on each stratum.
#'
#' @param x A SparkSQL DataFrame
#' @param col column that defines strata
#' @param fractions A named list giving sampling fraction for each stratum. If a stratum is
#'                  not specified, we treat its fraction as zero.
#' @param seed random seed
#' @return A new DataFrame that represents the stratified sample
#'
#' @rdname statfunctions
#' @name sampleBy
#' @export
#' @examples
#'\dontrun{
#' df <- jsonFile(sqlContext, "/path/to/file.json")
#' sample <- sampleBy(df, "key", fractions, 36)
#' }
setMethod("sampleBy",
          signature(x = "DataFrame", col = "character",
                    fractions = "list", seed = "numeric"),
          function(x, col, fractions, seed) {
            fractionsEnv <- convertNamedListToEnv(fractions)

            statFunctions <- callJMethod(x@sdf, "stat")
            # Seed is expected to be Long on Scala side, here convert it to an integer
            # due to SerDe limitation now.
            sdf <- callJMethod(statFunctions, "sampleBy", col, fractionsEnv, as.integer(seed))
            dataFrame(sdf)
          })

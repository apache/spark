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

# stats.R - Statistic functions for SparkDataFrames.

setOldClass("jobj")

#' Computes a pair-wise frequency table of the given columns
#'
#' Computes a pair-wise frequency table of the given columns. Also known as a contingency
#' table. The number of distinct values for each column should be less than 1e4. At most 1e6
#' non-zero pair frequencies will be returned.
#'
#' @param x a SparkDataFrame
#' @param col1 name of the first column. Distinct items will make the first item of each row.
#' @param col2 name of the second column. Distinct items will make the column names of the output.
#' @return a local R data.frame representing the contingency table. The first column of each row
#'         will be the distinct values of \code{col1} and the column names will be the distinct values
#'         of \code{col2}. The name of the first column will be "\code{col1}_\code{col2}". Pairs
#'         that have no occurrences will have zero as their counts.
#'
#' @rdname crosstab
#' @name crosstab
#' @aliases crosstab,SparkDataFrame,character,character-method
#' @family stat functions
#' @export
#' @examples
#' \dontrun{
#' df <- read.json("/path/to/file.json")
#' ct <- crosstab(df, "title", "gender")
#' }
#' @note crosstab since 1.5.0
setMethod("crosstab",
          signature(x = "SparkDataFrame", col1 = "character", col2 = "character"),
          function(x, col1, col2) {
            statFunctions <- callJMethod(x@sdf, "stat")
            sct <- callJMethod(statFunctions, "crosstab", col1, col2)
            collect(dataFrame(sct))
          })

#' Calculate the sample covariance of two numerical columns of a SparkDataFrame.
#'
#' @param colName1 the name of the first column
#' @param colName2 the name of the second column
#' @return The covariance of the two columns.
#'
#' @rdname cov
#' @name cov
#' @aliases cov,SparkDataFrame-method
#' @family stat functions
#' @export
#' @examples
#'\dontrun{
#' df <- read.json("/path/to/file.json")
#' cov <- cov(df, "title", "gender")
#' }
#' @note cov since 1.6.0
setMethod("cov",
          signature(x = "SparkDataFrame"),
          function(x, colName1, colName2) {
            stopifnot(class(colName1) == "character" && class(colName2) == "character")
            statFunctions <- callJMethod(x@sdf, "stat")
            callJMethod(statFunctions, "cov", colName1, colName2)
          })

#' Calculates the correlation of two columns of a SparkDataFrame.
#' Currently only supports the Pearson Correlation Coefficient.
#' For Spearman Correlation, consider using RDD methods found in MLlib's Statistics.
#'
#' @param colName1 the name of the first column
#' @param colName2 the name of the second column
#' @param method Optional. A character specifying the method for calculating the correlation.
#'               only "pearson" is allowed now.
#' @return The Pearson Correlation Coefficient as a Double.
#'
#' @rdname corr
#' @name corr
#' @aliases corr,SparkDataFrame-method
#' @family stat functions
#' @export
#' @examples
#'\dontrun{
#' df <- read.json("/path/to/file.json")
#' corr <- corr(df, "title", "gender")
#' corr <- corr(df, "title", "gender", method = "pearson")
#' }
#' @note corr since 1.6.0
setMethod("corr",
          signature(x = "SparkDataFrame"),
          function(x, colName1, colName2, method = "pearson") {
            stopifnot(class(colName1) == "character" && class(colName2) == "character")
            statFunctions <- callJMethod(x@sdf, "stat")
            callJMethod(statFunctions, "corr", colName1, colName2, method)
          })


#' Finding frequent items for columns, possibly with false positives
#'
#' Finding frequent items for columns, possibly with false positives.
#' Using the frequent element count algorithm described in
#' \url{http://dx.doi.org/10.1145/762471.762473}, proposed by Karp, Schenker, and Papadimitriou.
#'
#' @param x A SparkDataFrame.
#' @param cols A vector column names to search frequent items in.
#' @param support (Optional) The minimum frequency for an item to be considered \code{frequent}.
#'                Should be greater than 1e-4. Default support = 0.01.
#' @return a local R data.frame with the frequent items in each column
#'
#' @rdname freqItems
#' @name freqItems
#' @aliases freqItems,SparkDataFrame,character-method
#' @family stat functions
#' @export
#' @examples
#' \dontrun{
#' df <- read.json("/path/to/file.json")
#' fi = freqItems(df, c("title", "gender"))
#' }
#' @note freqItems since 1.6.0
setMethod("freqItems", signature(x = "SparkDataFrame", cols = "character"),
          function(x, cols, support = 0.01) {
            statFunctions <- callJMethod(x@sdf, "stat")
            sct <- callJMethod(statFunctions, "freqItems", as.list(cols), support)
            collect(dataFrame(sct))
          })

#' Calculates the approximate quantiles of numerical columns of a SparkDataFrame
#'
#' Calculates the approximate quantiles of numerical columns of a SparkDataFrame.
#' The result of this algorithm has the following deterministic bound:
#' If the SparkDataFrame has N elements and if we request the quantile at probability p up to
#' error err, then the algorithm will return a sample x from the SparkDataFrame so that the
#' *exact* rank of x is close to (p * N). More precisely,
#'   floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).
#' This method implements a variation of the Greenwald-Khanna algorithm (with some speed
#' optimizations). The algorithm was first present in [[http://dx.doi.org/10.1145/375663.375670
#' Space-efficient Online Computation of Quantile Summaries]] by Greenwald and Khanna.
#' Note that NA values will be ignored in numerical columns before calculation. For
#'   columns only containing NA values, an empty list is returned.
#'
#' @param x A SparkDataFrame.
#' @param cols A single column name, or a list of names for multiple columns.
#' @param probabilities A list of quantile probabilities. Each number must belong to [0, 1].
#'                      For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
#' @param relativeError The relative target precision to achieve (>= 0). If set to zero,
#'                      the exact quantiles are computed, which could be very expensive.
#'                      Note that values greater than 1 are accepted but give the same result as 1.
#' @return The approximate quantiles at the given probabilities. If the input is a single column name,
#'         the output is a list of approximate quantiles in that column; If the input is
#'         multiple column names, the output should be a list, and each element in it is a list of
#'         numeric values which represents the approximate quantiles in corresponding column.
#'
#' @rdname approxQuantile
#' @name approxQuantile
#' @aliases approxQuantile,SparkDataFrame,character,numeric,numeric-method
#' @family stat functions
#' @export
#' @examples
#' \dontrun{
#' df <- read.json("/path/to/file.json")
#' quantiles <- approxQuantile(df, "key", c(0.5, 0.8), 0.0)
#' }
#' @note approxQuantile since 2.0.0
setMethod("approxQuantile",
          signature(x = "SparkDataFrame", cols = "character",
                    probabilities = "numeric", relativeError = "numeric"),
          function(x, cols, probabilities, relativeError) {
            statFunctions <- callJMethod(x@sdf, "stat")
            quantiles <- callJMethod(statFunctions, "approxQuantile", as.list(cols),
                                     as.list(probabilities), relativeError)
            if (length(cols) == 1) {
              quantiles[[1]]
            } else {
              quantiles
            }
          })

#' Returns a stratified sample without replacement
#'
#' Returns a stratified sample without replacement based on the fraction given on each
#' stratum.
#'
#' @param x A SparkDataFrame
#' @param col column that defines strata
#' @param fractions A named list giving sampling fraction for each stratum. If a stratum is
#'                  not specified, we treat its fraction as zero.
#' @param seed random seed
#' @return A new SparkDataFrame that represents the stratified sample
#'
#' @rdname sampleBy
#' @aliases sampleBy,SparkDataFrame,character,list,numeric-method
#' @name sampleBy
#' @family stat functions
#' @export
#' @examples
#'\dontrun{
#' df <- read.json("/path/to/file.json")
#' sample <- sampleBy(df, "key", fractions, 36)
#' }
#' @note sampleBy since 1.6.0
setMethod("sampleBy",
          signature(x = "SparkDataFrame", col = "character",
                    fractions = "list", seed = "numeric"),
          function(x, col, fractions, seed) {
            fractionsEnv <- convertNamedListToEnv(fractions)

            statFunctions <- callJMethod(x@sdf, "stat")
            # Seed is expected to be Long on Scala side, here convert it to an integer
            # due to SerDe limitation now.
            sdf <- callJMethod(statFunctions, "sampleBy", col, fractionsEnv, as.integer(seed))
            dataFrame(sdf)
          })

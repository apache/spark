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

# mllib_feature.R: Provides methods for MLlib feature processing algorithms integration

#' Process of thresholding numerical features to binary 0/1
#'
#' @rdname spark.binarizer
#' @name spark.binarizer
#' @param data input SparkDataFrame
#' @param inputCol column name of which to be binarized
#' @param outputCol column name to be used for output column
#' @param threshold values greater than the threshold are binarized to 1.0,
#' values equal to or less than the threshold are binarized to 0.0.
#' @return SparkDataFrame with binarized column
#' @export
#' @examples
#' sparkR.session()
#' data <- createDataFrame(iris)
#' output <- spark.binarizer(data, "Petal_Length", "Petal_Length_binary", 1.3)
#' showDF(output)
#' @note spark.binarizer since 2.2.0
setMethod(
"spark.binarizer",
signature(data = "SparkDataFrame", inputCol = "character",
outputCol = "character", threshold = "numeric"),
function(data, inputCol, outputCol, threshold) {
  inputCol <- match.arg(inputCol, columns(data))
    if (length(column) != 1) {
      stop("Please input valid column name.")
    }
    res <- callJStatic(
    "org.apache.spark.ml.r.BinarizerWrapper",
    "transform",
    data@sdf,
    inputCol,
    outputCol,
    threshold
    )
    dataFrame(res)
})

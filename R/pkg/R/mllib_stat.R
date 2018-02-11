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

# mllib_stat.R: Provides methods for MLlib statistics algorithms integration

#' S4 class that represents an KSTest
#'
#' @param jobj a Java object reference to the backing Scala KSTestWrapper
#' @export
#' @note KSTest since 2.1.0
setClass("KSTest", representation(jobj = "jobj"))

#' (One-Sample) Kolmogorov-Smirnov Test
#'
#' @description
#' \code{spark.kstest} Conduct the two-sided Kolmogorov-Smirnov (KS) test for data sampled from a
#' continuous distribution.
#'
#' By comparing the largest difference between the empirical cumulative
#' distribution of the sample data and the theoretical distribution we can provide a test for the
#' the null hypothesis that the sample data comes from that theoretical distribution.
#'
#' Users can call \code{summary} to obtain a summary of the test, and \code{print.summary.KSTest}
#' to print out a summary result.
#'
#' @param data a SparkDataFrame of user data.
#' @param testCol column name where the test data is from. It should be a column of double type.
#' @param nullHypothesis name of the theoretical distribution tested against. Currently only
#'                       \code{"norm"} for normal distribution is supported.
#' @param distParams parameters(s) of the distribution. For \code{nullHypothesis = "norm"},
#'                   we can provide as a vector the mean and standard deviation of
#'                   the distribution. If none is provided, then standard normal will be used.
#'                   If only one is provided, then the standard deviation will be set to be one.
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.kstest} returns a test result object.
#' @rdname spark.kstest
#' @aliases spark.kstest,SparkDataFrame-method
#' @name spark.kstest
#' @seealso \href{http://spark.apache.org/docs/latest/mllib-statistics.html#hypothesis-testing}{
#'          MLlib: Hypothesis Testing}
#' @export
#' @examples
#' \dontrun{
#' data <- data.frame(test = c(0.1, 0.15, 0.2, 0.3, 0.25))
#' df <- createDataFrame(data)
#' test <- spark.kstest(df, "test", "norm", c(0, 1))
#'
#' # get a summary of the test result
#' testSummary <- summary(test)
#' testSummary
#'
#' # print out the summary in an organized way
#' print.summary.KSTest(testSummary)
#' }
#' @note spark.kstest since 2.1.0
setMethod("spark.kstest", signature(data = "SparkDataFrame"),
          function(data, testCol = "test", nullHypothesis = c("norm"), distParams = c(0, 1)) {
            tryCatch(match.arg(nullHypothesis),
                     error = function(e) {
                       msg <- paste("Distribution", nullHypothesis, "is not supported.")
                       stop(msg)
                     })
            if (nullHypothesis == "norm") {
              distParams <- as.numeric(distParams)
              mu <- ifelse(length(distParams) < 1, 0, distParams[1])
              sigma <- ifelse(length(distParams) < 2, 1, distParams[2])
              jobj <- callJStatic("org.apache.spark.ml.r.KSTestWrapper",
                                  "test", data@sdf, testCol, nullHypothesis,
                                  as.array(c(mu, sigma)))
              new("KSTest", jobj = jobj)
            }
})

#  Get the summary of Kolmogorov-Smirnov (KS) Test.

#' @param object test result object of KSTest by \code{spark.kstest}.
#' @return \code{summary} returns summary information of KSTest object, which is a list.
#'         The list includes the \code{p.value} (p-value), \code{statistic} (test statistic
#'         computed for the test), \code{nullHypothesis} (the null hypothesis with its
#'         parameters tested against) and \code{degreesOfFreedom} (degrees of freedom of the test).
#' @rdname spark.kstest
#' @aliases summary,KSTest-method
#' @export
#' @note summary(KSTest) since 2.1.0
setMethod("summary", signature(object = "KSTest"),
          function(object) {
            jobj <- object@jobj
            pValue <- callJMethod(jobj, "pValue")
            statistic <- callJMethod(jobj, "statistic")
            nullHypothesis <- callJMethod(jobj, "nullHypothesis")
            distName <- callJMethod(jobj, "distName")
            distParams <- unlist(callJMethod(jobj, "distParams"))
            degreesOfFreedom <- callJMethod(jobj, "degreesOfFreedom")

            ans <- list(p.value = pValue, statistic = statistic, nullHypothesis = nullHypothesis,
                        nullHypothesis.name = distName, nullHypothesis.parameters = distParams,
                        degreesOfFreedom = degreesOfFreedom, jobj = jobj)
            class(ans) <- "summary.KSTest"
            ans
          })

#  Prints the summary of KSTest

#' @rdname spark.kstest
#' @param x summary object of KSTest returned by \code{summary}.
#' @export
#' @note print.summary.KSTest since 2.1.0
print.summary.KSTest <- function(x, ...) {
  jobj <- x$jobj
  summaryStr <- callJMethod(jobj, "summary")
  cat(summaryStr, "\n")
  invisible(x)
}

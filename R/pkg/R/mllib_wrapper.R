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

#' S4 class that represents a Java ML model
#'
#' @param jobj a Java object reference to the backing Scala model
#' @export
#' @note JavaModel since 2.3.0
setClass("JavaModel", representation(jobj = "jobj"))

#' Makes predictions from a Java ML model
#'
#' @param object a Spark ML model.
#' @param newData a SparkDataFrame for testing.
#' @return \code{predict} returns a SparkDataFrame containing predicted value.
#' @rdname spark.predict
#' @aliases predict,JavaModel-method
#' @export
#' @note predict since 2.3.0
setMethod("predict", signature(object = "JavaModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' S4 class that represents a writable Java ML model
#'
#' @param jobj a Java object reference to the backing Scala model
#' @export
#' @note JavaMLWritable since 2.3.0
setClass("JavaMLWritable", representation(jobj = "jobj"))

#  Save the ML model to the output path.

#' @param object A fitted ML model.
#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @aliases write.ml,JavaMLWritable,character-method
#' @rdname write.ml
#' @export
#' @seealso \link{read.ml}
#' @note write.ml(JavaMLWritable, character) since 2.3.0
setMethod("write.ml", signature(object = "JavaMLWritable", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

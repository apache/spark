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

# Methods to directly access the JVM running the SparkR backend.

#' Call Java Methods
#'
#' Call a Java method in the JVM running the Spark driver. The return
#' values are automatically converted to R objects for simple objects. Other
#' values are returned as "jobj" which are references to objects on JVM.
#'
#' @details
#' This is a low level function to access the JVM directly and should only be used
#' for advanced use cases. The arguments and return values that are primitive R
#' types (like integer, numeric, character, lists) are automatically translated to/from
#' Java types (like Integer, Double, String, Array). A full list can be found in
#' serialize.R and deserialize.R in the Apache Spark code base.
#'
#' @param x object to invoke the method on. Should be a "jobj" created by newJObject.
#' @param methodName method name to call.
#' @param ... parameters to pass to the Java method.
#' @return the return value of the Java method. Either returned as a R object
#'  if it can be deserialized or returned as a "jobj". See details section for more.
#' @seealso \link{sparkR.callJStatic}, \link{sparkR.newJObject}
#' @rdname sparkR.callJMethod
#' @examples
#' \dontrun{
#' sparkR.session() # Need to have a Spark JVM running before calling newJObject
#' # Create a Java ArrayList and populate it
#' jarray <- sparkR.newJObject("java.util.ArrayList")
#' sparkR.callJMethod(jarray, "add", 42L)
#' sparkR.callJMethod(jarray, "get", 0L) # Will print 42
#' }
#' @note sparkR.callJMethod since 2.0.1
sparkR.callJMethod <- function(x, methodName, ...) {
  callJMethod(x, methodName, ...)
}

#' Call Static Java Methods
#'
#' Call a static method in the JVM running the Spark driver. The return
#' value is automatically converted to R objects for simple objects. Other
#' values are returned as "jobj" which are references to objects on JVM.
#'
#' @details
#' This is a low level function to access the JVM directly and should only be used
#' for advanced use cases. The arguments and return values that are primitive R
#' types (like integer, numeric, character, lists) are automatically translated to/from
#' Java types (like Integer, Double, String, Array). A full list can be found in
#' serialize.R and deserialize.R in the Apache Spark code base.
#'
#' @param x fully qualified Java class name that contains the static method to invoke.
#' @param methodName name of static method to invoke.
#' @param ... parameters to pass to the Java method.
#' @return the return value of the Java method. Either returned as a R object
#'  if it can be deserialized or returned as a "jobj". See details section for more.
#' @seealso \link{sparkR.callJMethod}, \link{sparkR.newJObject}
#' @rdname sparkR.callJStatic
#' @examples
#' \dontrun{
#' sparkR.session() # Need to have a Spark JVM running before calling callJStatic
#' sparkR.callJStatic("java.lang.System", "currentTimeMillis")
#' sparkR.callJStatic("java.lang.System", "getProperty", "java.home")
#' }
#' @note sparkR.callJStatic since 2.0.1
sparkR.callJStatic <- function(x, methodName, ...) {
  callJStatic(x, methodName, ...)
}

#' Create Java Objects
#'
#' Create a new Java object in the JVM running the Spark driver. The return
#' value is automatically converted to an R object for simple objects. Other
#' values are returned as a "jobj" which is a reference to an object on JVM.
#'
#' @details
#' This is a low level function to access the JVM directly and should only be used
#' for advanced use cases. The arguments and return values that are primitive R
#' types (like integer, numeric, character, lists) are automatically translated to/from
#' Java types (like Integer, Double, String, Array). A full list can be found in
#' serialize.R and deserialize.R in the Apache Spark code base.
#'
#' @param x fully qualified Java class name.
#' @param ... arguments to be passed to the constructor.
#' @return the object created. Either returned as a R object
#'   if it can be deserialized or returned as a "jobj". See details section for more.
#' @seealso \link{sparkR.callJMethod}, \link{sparkR.callJStatic}
#' @rdname sparkR.newJObject
#' @examples
#' \dontrun{
#' sparkR.session() # Need to have a Spark JVM running before calling newJObject
#' # Create a Java ArrayList and populate it
#' jarray <- sparkR.newJObject("java.util.ArrayList")
#' sparkR.callJMethod(jarray, "add", 42L)
#' sparkR.callJMethod(jarray, "get", 0L) # Will print 42
#' }
#' @note sparkR.newJObject since 2.0.1
sparkR.newJObject <- function(x, ...) {
  newJObject(x, ...)
}

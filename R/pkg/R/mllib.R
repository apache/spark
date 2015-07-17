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

# mllib.R: Provides methods for MLLib integration

glm <- function(formula, data, family = c("gaussian", "binomial"), lambda = 0, alpha = 0) {
  model <- callJStatic("org.apache.spark.mllib.api.r.MLUtils",
                       "fitRModelFormula", deparse(formula), family, data@sdf, lambda, alpha)
  return(model)
}

predict <- function(model, newdata) {
  return(dataFrame(callJMethod(model, "transform", newdata@sdf)))
}

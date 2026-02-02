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

context("tests RDD function take()")

# Mock data
numVector <- c(-10:97)
numList <- list(sqrt(1), sqrt(2), sqrt(3), 4 ** 10)
strVector <- c("Dexter Morgan: I suppose I should be upset, even feel",
               "violated, but I'm not. No, in fact, I think this is a friendly",
               "message, like \"Hey, wanna play?\" and yes, I want to play. ",
               "I really, really do.")
strList <- list("Dexter Morgan: Blood. Sometimes it sets my teeth on edge, ",
                "other times it helps me control the chaos.",
                "Dexter Morgan: Harry and Dorris Morgan did a wonderful job ",
                "raising me. But they're both dead now. I didn't kill them. Honest.")

# JavaSparkContext handle
sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)
sc <- callJStatic("org.apache.spark.sql.api.r.SQLUtils", "getJavaSparkContext", sparkSession)

test_that("take() gives back the original elements in correct count and order", {
  numVectorRDD <- parallelize(sc, numVector, 10)
  # case: number of elements to take is less than the size of the first partition
  expect_equal(takeRDD(numVectorRDD, 1), as.list(head(numVector, n = 1)))
  # case: number of elements to take is the same as the size of the first partition
  expect_equal(takeRDD(numVectorRDD, 11), as.list(head(numVector, n = 11)))
  # case: number of elements to take is greater than all elements
  expect_equal(takeRDD(numVectorRDD, length(numVector)), as.list(numVector))
  expect_equal(takeRDD(numVectorRDD, length(numVector) + 1), as.list(numVector))

  numListRDD <- parallelize(sc, numList, 1)
  numListRDD2 <- parallelize(sc, numList, 4)
  expect_equal(takeRDD(numListRDD, 3), takeRDD(numListRDD2, 3))
  expect_equal(takeRDD(numListRDD, 5), takeRDD(numListRDD2, 5))
  expect_equal(takeRDD(numListRDD, 1), as.list(head(numList, n = 1)))
  expect_equal(takeRDD(numListRDD2, 999), numList)

  strVectorRDD <- parallelize(sc, strVector, 2)
  strVectorRDD2 <- parallelize(sc, strVector, 3)
  expect_equal(takeRDD(strVectorRDD, 4), as.list(strVector))
  expect_equal(takeRDD(strVectorRDD2, 2), as.list(head(strVector, n = 2)))

  strListRDD <- parallelize(sc, strList, 4)
  strListRDD2 <- parallelize(sc, strList, 1)
  expect_equal(takeRDD(strListRDD, 3), as.list(head(strList, n = 3)))
  expect_equal(takeRDD(strListRDD2, 1), as.list(head(strList, n = 1)))

  expect_equal(length(takeRDD(strListRDD, 0)), 0)
  expect_equal(length(takeRDD(strVectorRDD, 0)), 0)
  expect_equal(length(takeRDD(numListRDD, 0)), 0)
  expect_equal(length(takeRDD(numVectorRDD, 0)), 0)
})

sparkR.session.stop()

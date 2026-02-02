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

library(testthat)

context("MLlib tree-based algorithms")

# Tests for MLlib tree-based algorithms in SparkR
sparkSession <- sparkR.session(master = sparkRTestMaster, enableHiveSupport = FALSE)

absoluteSparkPath <- function(x) {
  sparkHome <- sparkR.conf("spark.home")
  file.path(sparkHome, x)
}

test_that("spark.gbt", {
  # regression
  data <- suppressWarnings(createDataFrame(longley))
  model <- spark.gbt(data, Employed ~ ., "regression", maxDepth = 5, maxBins = 16, seed = 123)
  predictions <- collect(predict(model, data))
  expect_equal(predictions$prediction, c(60.323, 61.122, 60.171, 61.187,
                                         63.221, 63.639, 64.989, 63.761,
                                         66.019, 67.857, 68.169, 66.513,
                                         68.655, 69.564, 69.331, 70.551),
               tolerance = 1e-4)
  stats <- summary(model)
  expect_equal(stats$numTrees, 20)
  expect_equal(stats$maxDepth, 5)
  expect_equal(stats$formula, "Employed ~ .")
  expect_equal(stats$numFeatures, 6)
  expect_equal(length(stats$treeWeights), 20)

  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-gbtRegression", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    stats2 <- summary(model2)
    expect_equal(stats$formula, stats2$formula)
    expect_equal(stats$numFeatures, stats2$numFeatures)
    expect_equal(stats$features, stats2$features)
    expect_equal(stats$featureImportances, stats2$featureImportances)
    expect_equal(stats$maxDepth, stats2$maxDepth)
    expect_equal(stats$numTrees, stats2$numTrees)
    expect_equal(stats$treeWeights, stats2$treeWeights)

    unlink(modelPath)
  }

  # classification
  # label must be binary - GBTClassifier currently only supports binary classification.
  iris2 <- iris[iris$Species != "virginica", ]
  data <- suppressWarnings(createDataFrame(iris2))
  model <- spark.gbt(data, Species ~ Petal_Length + Petal_Width, "classification", seed = 12)
  stats <- summary(model)
  expect_equal(stats$numFeatures, 2)
  expect_equal(stats$numTrees, 20)
  expect_equal(stats$maxDepth, 5)
  expect_error(capture.output(stats), NA)
  expect_true(length(capture.output(stats)) > 6)
  predictions <- collect(predict(model, data))$prediction
  # test string prediction values
  expect_equal(length(grep("setosa", predictions)), 50)
  expect_equal(length(grep("versicolor", predictions)), 50)

  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-gbtClassification", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    stats2 <- summary(model2)
    expect_equal(stats$depth, stats2$depth)
    expect_equal(stats$numNodes, stats2$numNodes)
    expect_equal(stats$numClasses, stats2$numClasses)

    unlink(modelPath)
  }

  iris2$NumericSpecies <- ifelse(iris2$Species == "setosa", 0, 1)
  df <- suppressWarnings(createDataFrame(iris2))
  m <- spark.gbt(df, NumericSpecies ~ ., type = "classification", seed = 12)
  s <- summary(m)
  # test numeric prediction values
  expect_equal(iris2$NumericSpecies, as.double(collect(predict(m, df))$prediction))
  expect_equal(s$numFeatures, 5)
  expect_equal(s$numTrees, 20)
  expect_equal(stats$maxDepth, 5)

  # spark.gbt classification can work on libsvm data
  if (windows_with_hadoop()) {
    data <- read.df(absoluteSparkPath("data/mllib/sample_binary_classification_data.txt"),
                  source = "libsvm")
    model <- spark.gbt(data, label ~ features, "classification", seed = 12)
    expect_equal(summary(model)$numFeatures, 692)
  }

  # Test unseen labels
  data <- data.frame(clicked = base::sample(c(0, 1), 10, replace = TRUE),
  someString = base::sample(c("this", "that"), 10, replace = TRUE),
                            stringsAsFactors = FALSE)
  trainidxs <- base::sample(nrow(data), nrow(data) * 0.7)
  traindf <- as.DataFrame(data[trainidxs, ])
  testdf <- as.DataFrame(rbind(data[-trainidxs, ], c(0, "the other")))
  model <- spark.gbt(traindf, clicked ~ ., type = "classification", seed = 23)
  predictions <- predict(model, testdf)
  expect_error(collect(predictions))
  model <- spark.gbt(traindf, clicked ~ ., type = "classification", handleInvalid = "keep",
                    seed = 23)
  predictions <- predict(model, testdf)
  expect_equal(class(collect(predictions)$clicked[1]), "character")
})

test_that("spark.randomForest", {
  # regression
  data <- suppressWarnings(createDataFrame(longley))
  model <- spark.randomForest(data, Employed ~ ., "regression", maxDepth = 5, maxBins = 16,
                              numTrees = 1, seed = 1, bootstrap = FALSE)

  predictions <- collect(predict(model, data))
  expect_equal(predictions$prediction, c(60.323, 61.122, 60.171, 61.187,
                                         63.221, 63.639, 64.989, 63.761,
                                         66.019, 67.857, 68.169, 66.513,
                                         68.655, 69.564, 69.331, 70.551),
               tolerance = 1e-4)

  stats <- summary(model)
  expect_equal(stats$numTrees, 1)
  expect_equal(stats$maxDepth, 5)
  expect_error(capture.output(stats), NA)
  expect_true(length(capture.output(stats)) > 6)

  model <- spark.randomForest(data, Employed ~ ., "regression", maxDepth = 5, maxBins = 16,
                              numTrees = 20, seed = 123)
  predictions <- collect(predict(model, data))
  expect_equal(predictions$prediction, c(60.32495, 61.06495, 60.52120, 61.98500,
                                         63.64450, 64.21910, 65.00810, 64.30450,
                                         66.70910, 67.96875, 68.22140, 67.21865,
                                         68.89275, 69.55900, 69.30160, 69.93050),
               tolerance = 1e-4)
  stats <- summary(model)
  expect_equal(stats$numTrees, 20)
  expect_equal(stats$maxDepth, 5)

  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-randomForestRegression", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    stats2 <- summary(model2)
    expect_equal(stats$formula, stats2$formula)
    expect_equal(stats$numFeatures, stats2$numFeatures)
    expect_equal(stats$features, stats2$features)
    expect_equal(stats$featureImportances, stats2$featureImportances)
    expect_equal(stats$numTrees, stats2$numTrees)
    expect_equal(stats$maxDepth, stats2$maxDepth)
    expect_equal(stats$treeWeights, stats2$treeWeights)

    unlink(modelPath)
  }

  # classification
  data <- suppressWarnings(createDataFrame(iris))
  model <- spark.randomForest(data, Species ~ Petal_Length + Petal_Width, "classification",
                              maxDepth = 5, maxBins = 16, seed = 123)

  stats <- summary(model)
  expect_equal(stats$numFeatures, 2)
  expect_equal(stats$numTrees, 20)
  expect_equal(stats$maxDepth, 5)
  expect_error(capture.output(stats), NA)
  expect_true(length(capture.output(stats)) > 6)
  # Test string prediction values
  predictions <- collect(predict(model, data))$prediction
  expect_equal(length(grep("setosa", predictions)), 50)
  expect_equal(length(grep("versicolor", predictions)), 50)

  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-randomForestClassification", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    stats2 <- summary(model2)
    expect_equal(stats$depth, stats2$depth)
    expect_equal(stats$numNodes, stats2$numNodes)
    expect_equal(stats$numClasses, stats2$numClasses)

    unlink(modelPath)
  }

  # Test numeric response variable
  labelToIndex <- function(species) {
    switch(as.character(species),
      setosa = 0.0,
      versicolor = 1.0,
      virginica = 2.0
    )
  }
  iris$NumericSpecies <- lapply(iris$Species, labelToIndex)
  data <- suppressWarnings(createDataFrame(iris[-5]))
  model <- spark.randomForest(data, NumericSpecies ~ Petal_Length + Petal_Width, "classification",
                              maxDepth = 5, maxBins = 16, seed = 123)
  stats <- summary(model)
  expect_equal(stats$numFeatures, 2)
  expect_equal(stats$numTrees, 20)
  expect_equal(stats$maxDepth, 5)

  # Test numeric prediction values
  predictions <- collect(predict(model, data))$prediction
  expect_equal(length(grep("1.0", predictions)), 50)
  expect_equal(length(grep("2.0", predictions)), 50)

  # Test unseen labels
  data <- data.frame(clicked = base::sample(c(0, 1), 10, replace = TRUE),
                    someString = base::sample(c("this", "that"), 10, replace = TRUE),
                    stringsAsFactors = FALSE)
  trainidxs <- base::sample(nrow(data), nrow(data) * 0.7)
  traindf <- as.DataFrame(data[trainidxs, ])
  testdf <- as.DataFrame(rbind(data[-trainidxs, ], c(0, "the other")))
  model <- spark.randomForest(traindf, clicked ~ ., type = "classification",
                          maxDepth = 10, maxBins = 10, numTrees = 10, seed = 123)
  predictions <- predict(model, testdf)
  expect_error(collect(predictions))
  model <- spark.randomForest(traindf, clicked ~ ., type = "classification",
                             maxDepth = 10, maxBins = 10, numTrees = 10,
                             handleInvalid = "keep", seed = 123)
  predictions <- predict(model, testdf)
  expect_equal(class(collect(predictions)$clicked[1]), "character")

  # spark.randomForest classification can work on libsvm data
  if (windows_with_hadoop()) {
    data <- read.df(absoluteSparkPath("data/mllib/sample_multiclass_classification_data.txt"),
                  source = "libsvm")
    model <- spark.randomForest(data, label ~ features, "classification", seed = 123)
    expect_equal(summary(model)$numFeatures, 4)
  }
})

test_that("spark.decisionTree", {
  # regression
  data <- suppressWarnings(createDataFrame(longley))
  model <- spark.decisionTree(data, Employed ~ ., "regression", maxDepth = 5, maxBins = 16,
                            seed = 42)

  predictions <- collect(predict(model, data))
  expect_equal(predictions$prediction, c(60.323, 61.122, 60.171, 61.187,
                                         63.221, 63.639, 64.989, 63.761,
                                         66.019, 67.857, 68.169, 66.513,
                                         68.655, 69.564, 69.331, 70.551),
               tolerance = 1e-4)

  stats <- summary(model)
  expect_equal(stats$maxDepth, 5)
  expect_error(capture.output(stats), NA)
  expect_true(length(capture.output(stats)) > 6)

  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-decisionTreeRegression", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    stats2 <- summary(model2)
    expect_equal(stats$formula, stats2$formula)
    expect_equal(stats$numFeatures, stats2$numFeatures)
    expect_equal(stats$features, stats2$features)
    expect_equal(stats$featureImportances, stats2$featureImportances)
    expect_equal(stats$maxDepth, stats2$maxDepth)

    unlink(modelPath)
  }

  # classification
  data <- suppressWarnings(createDataFrame(iris))
  model <- spark.decisionTree(data, Species ~ Petal_Length + Petal_Width, "classification",
                              maxDepth = 5, maxBins = 16, seed = 43)

  stats <- summary(model)
  expect_equal(stats$numFeatures, 2)
  expect_equal(stats$maxDepth, 5)
  expect_error(capture.output(stats), NA)
  expect_true(length(capture.output(stats)) > 6)
  # Test string prediction values
  predictions <- collect(predict(model, data))$prediction
  expect_equal(length(grep("setosa", predictions)), 50)
  expect_equal(length(grep("versicolor", predictions)), 50)

  if (windows_with_hadoop()) {
    modelPath <- tempfile(pattern = "spark-decisionTreeClassification", fileext = ".tmp")
    write.ml(model, modelPath)
    expect_error(write.ml(model, modelPath))
    write.ml(model, modelPath, overwrite = TRUE)
    model2 <- read.ml(modelPath)
    stats2 <- summary(model2)
    expect_equal(stats$depth, stats2$depth)
    expect_equal(stats$numNodes, stats2$numNodes)
    expect_equal(stats$numClasses, stats2$numClasses)

    unlink(modelPath)
  }

  # Test numeric response variable
  labelToIndex <- function(species) {
    switch(as.character(species),
      setosa = 0.0,
      versicolor = 1.0,
      virginica = 2.0
    )
  }
  iris$NumericSpecies <- lapply(iris$Species, labelToIndex)
  data <- suppressWarnings(createDataFrame(iris[-5]))
  model <- spark.decisionTree(data, NumericSpecies ~ Petal_Length + Petal_Width, "classification",
                              maxDepth = 5, maxBins = 16, seed = 44)
  stats <- summary(model)
  expect_equal(stats$numFeatures, 2)
  expect_equal(stats$maxDepth, 5)

  # Test numeric prediction values
  predictions <- collect(predict(model, data))$prediction
  expect_equal(length(grep("1.0", predictions)), 50)
  expect_equal(length(grep("2.0", predictions)), 50)

  # spark.decisionTree classification can work on libsvm data
  if (windows_with_hadoop()) {
    data <- read.df(absoluteSparkPath("data/mllib/sample_multiclass_classification_data.txt"),
                  source = "libsvm")
    model <- spark.decisionTree(data, label ~ features, "classification", seed = 45)
    expect_equal(summary(model)$numFeatures, 4)
  }

  # Test unseen labels
  data <- data.frame(clicked = base::sample(c(0, 1), 10, replace = TRUE),
  someString = base::sample(c("this", "that"), 10, replace = TRUE),
                            stringsAsFactors = FALSE)
  trainidxs <- base::sample(nrow(data), nrow(data) * 0.7)
  traindf <- as.DataFrame(data[trainidxs, ])
  testdf <- as.DataFrame(rbind(data[-trainidxs, ], c(0, "the other")))
  model <- spark.decisionTree(traindf, clicked ~ ., type = "classification",
                              maxDepth = 5, maxBins = 16, seed = 46)
  predictions <- predict(model, testdf)
  expect_error(collect(predictions))
  model <- spark.decisionTree(traindf, clicked ~ ., type = "classification",
                              maxDepth = 5, maxBins = 16, handleInvalid = "keep", seed = 46)
  predictions <- predict(model, testdf)
  expect_equal(class(collect(predictions)$clicked[1]), "character")
})

sparkR.session.stop()

# MLlib - Machine Learning Library

MLlib is Apache Spark's scalable machine learning library.

## Overview

MLlib provides:

- **ML Algorithms**: Classification, regression, clustering, collaborative filtering
- **Featurization**: Feature extraction, transformation, dimensionality reduction, selection
- **Pipelines**: Tools for constructing, evaluating, and tuning ML workflows
- **Utilities**: Linear algebra, statistics, data handling

## Important Note

MLlib includes two packages:

1. **`spark.ml`** (DataFrame-based API) - **Primary API** (Recommended)
2. **`spark.mllib`** (RDD-based API) - **Maintenance mode only**

The RDD-based API (`spark.mllib`) is in maintenance mode. The DataFrame-based API (`spark.ml`) is the primary API and is recommended for all new applications.

## Package Structure

### spark.ml (Primary API)

**Location**: `../sql/core/src/main/scala/org/apache/spark/ml/`

DataFrame-based API with:
- **ML Pipeline API**: For building ML workflows
- **Transformers**: Feature transformers
- **Estimators**: Learning algorithms
- **Models**: Fitted models

```scala
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler

// Create pipeline
val assembler = new VectorAssembler()
  .setInputCols(Array("feature1", "feature2"))
  .setOutputCol("features")

val lr = new LogisticRegression()
  .setMaxIter(10)

val pipeline = new Pipeline().setStages(Array(assembler, lr))

// Fit model
val model = pipeline.fit(trainingData)

// Make predictions
val predictions = model.transform(testData)
```

### spark.mllib (RDD-based API - Maintenance Mode)

**Location**: `src/main/scala/org/apache/spark/mllib/`

RDD-based API with:
- Classic algorithms using RDDs
- Maintained for backward compatibility
- No new features added

```scala
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint

// Train model (old API)
val data: RDD[LabeledPoint] = ...
val model = LogisticRegressionWithLBFGS.train(data)

// Make predictions
val predictions = data.map { point => model.predict(point.features) }
```

## Key Concepts

### Pipeline API (spark.ml)

Machine learning pipelines provide:

1. **DataFrame**: Unified data representation
2. **Transformer**: Algorithms that transform DataFrames
3. **Estimator**: Algorithms that fit on DataFrames to produce Transformers
4. **Pipeline**: Chains multiple Transformers and Estimators
5. **Parameter**: Common API for specifying parameters

**Example Pipeline:**
```scala
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

// Configure pipeline stages
val tokenizer = new Tokenizer()
  .setInputCol("text")
  .setOutputCol("words")

val hashingTF = new HashingTF()
  .setInputCol("words")
  .setOutputCol("features")

val lr = new LogisticRegression()
  .setMaxIter(10)

val pipeline = new Pipeline()
  .setStages(Array(tokenizer, hashingTF, lr))

// Fit the pipeline
val model = pipeline.fit(trainingData)

// Make predictions
model.transform(testData)
```

### Transformers

Algorithms that transform one DataFrame into another.

**Examples:**
- `Tokenizer`: Splits text into words
- `HashingTF`: Maps word sequences to feature vectors
- `StandardScaler`: Normalizes features
- `VectorAssembler`: Combines multiple columns into a vector
- `PCA`: Dimensionality reduction

### Estimators

Algorithms that fit on a DataFrame to produce a Transformer.

**Examples:**
- `LogisticRegression`: Produces LogisticRegressionModel
- `DecisionTreeClassifier`: Produces DecisionTreeClassificationModel
- `KMeans`: Produces KMeansModel
- `StringIndexer`: Produces StringIndexerModel

## ML Algorithms

### Classification

**Binary and Multiclass:**
- Logistic Regression
- Decision Tree Classifier
- Random Forest Classifier
- Gradient-Boosted Tree Classifier
- Naive Bayes
- Linear Support Vector Machine

**Multilabel:**
- OneVsRest

**Example:**
```scala
import org.apache.spark.ml.classification.LogisticRegression

val lr = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

val model = lr.fit(trainingData)
val predictions = model.transform(testData)
```

**Location**: `../sql/core/src/main/scala/org/apache/spark/ml/classification/`

### Regression

- Linear Regression
- Generalized Linear Regression
- Decision Tree Regression
- Random Forest Regression
- Gradient-Boosted Tree Regression
- Survival Regression (AFT)
- Isotonic Regression

**Example:**
```scala
import org.apache.spark.ml.regression.LinearRegression

val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

val model = lr.fit(trainingData)
```

**Location**: `../sql/core/src/main/scala/org/apache/spark/ml/regression/`

### Clustering

- K-means
- Latent Dirichlet Allocation (LDA)
- Bisecting K-means
- Gaussian Mixture Model (GMM)

**Example:**
```scala
import org.apache.spark.ml.clustering.KMeans

val kmeans = new KMeans()
  .setK(3)
  .setSeed(1L)

val model = kmeans.fit(dataset)
val predictions = model.transform(dataset)
```

**Location**: `../sql/core/src/main/scala/org/apache/spark/ml/clustering/`

### Collaborative Filtering

Alternating Least Squares (ALS) for recommendation systems.

**Example:**
```scala
import org.apache.spark.ml.recommendation.ALS

val als = new ALS()
  .setMaxIter(10)
  .setRegParam(0.01)
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")

val model = als.fit(ratings)
val predictions = model.transform(testData)
```

**Location**: `../sql/core/src/main/scala/org/apache/spark/ml/recommendation/`

## Feature Engineering

### Feature Extractors

- `TF-IDF`: Text feature extraction
- `Word2Vec`: Word embeddings
- `CountVectorizer`: Converts text to vectors of token counts

### Feature Transformers

- `Tokenizer`: Text tokenization
- `StopWordsRemover`: Removes stop words
- `StringIndexer`: Encodes string labels to indices
- `IndexToString`: Converts indices back to strings
- `OneHotEncoder`: One-hot encoding
- `VectorAssembler`: Combines columns into feature vector
- `StandardScaler`: Standardizes features
- `MinMaxScaler`: Scales features to a range
- `Normalizer`: Normalizes vectors to unit norm
- `Binarizer`: Binarizes based on threshold

### Feature Selectors

- `VectorSlicer`: Extracts subset of features
- `RFormula`: R model formula for feature specification
- `ChiSqSelector`: Chi-square feature selection

**Location**: `../sql/core/src/main/scala/org/apache/spark/ml/feature/`

## Model Selection and Tuning

### Cross-Validation

```scala
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.RegressionEvaluator

val paramGrid = new ParamGridBuilder()
  .addGrid(lr.regParam, Array(0.1, 0.01))
  .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
  .build()

val cv = new CrossValidator()
  .setEstimator(lr)
  .setEvaluator(new RegressionEvaluator())
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)

val cvModel = cv.fit(trainingData)
```

### Train-Validation Split

```scala
import org.apache.spark.ml.tuning.TrainValidationSplit

val trainValidationSplit = new TrainValidationSplit()
  .setEstimator(lr)
  .setEvaluator(new RegressionEvaluator())
  .setEstimatorParamMaps(paramGrid)
  .setTrainRatio(0.8)

val model = trainValidationSplit.fit(trainingData)
```

**Location**: `../sql/core/src/main/scala/org/apache/spark/ml/tuning/`

## Evaluation Metrics

### Classification

```scala
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
```

### Regression

```scala
import org.apache.spark.ml.evaluation.RegressionEvaluator

val evaluator = new RegressionEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val rmse = evaluator.evaluate(predictions)
```

**Location**: `../sql/core/src/main/scala/org/apache/spark/ml/evaluation/`

## Linear Algebra

MLlib provides distributed linear algebra through Breeze.

**Location**: `src/main/scala/org/apache/spark/mllib/linalg/`

**Local vectors and matrices:**
```scala
import org.apache.spark.ml.linalg.{Vector, Vectors, Matrix, Matrices}

// Dense vector
val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)

// Sparse vector
val sv: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))

// Dense matrix
val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
```

**Distributed matrices:**
- `RowMatrix`: Distributed row-oriented matrix
- `IndexedRowMatrix`: Indexed rows
- `CoordinateMatrix`: Coordinate list format
- `BlockMatrix`: Block-partitioned matrix

## Statistics

Basic statistics and hypothesis testing.

**Location**: `src/main/scala/org/apache/spark/mllib/stat/`

**Examples:**
- Summary statistics
- Correlations
- Stratified sampling
- Hypothesis testing
- Random data generation

## Building and Testing

### Build MLlib Module

```bash
# Build mllib module (RDD-based)
./build/mvn -pl mllib -am package

# The DataFrame-based ml package is in sql/core
./build/mvn -pl sql/core -am package
```

### Run Tests

```bash
# Run mllib tests
./build/mvn test -pl mllib

# Run specific test
./build/mvn test -pl mllib -Dtest=LinearRegressionSuite
```

## Source Code Organization

```
mllib/src/main/
├── scala/org/apache/spark/mllib/
│   ├── classification/         # Classification algorithms (RDD-based)
│   ├── clustering/            # Clustering algorithms (RDD-based)
│   ├── evaluation/            # Evaluation metrics (RDD-based)
│   ├── feature/               # Feature engineering (RDD-based)
│   ├── fpm/                   # Frequent pattern mining
│   ├── linalg/                # Linear algebra
│   ├── optimization/          # Optimization algorithms
│   ├── recommendation/        # Collaborative filtering (RDD-based)
│   ├── regression/            # Regression algorithms (RDD-based)
│   ├── stat/                  # Statistics
│   ├── tree/                  # Decision trees (RDD-based)
│   └── util/                  # Utilities
└── resources/
```

## Performance Considerations

### Caching

Cache datasets that are used multiple times:
```scala
val trainingData = data.cache()
```

### Parallelism

Adjust parallelism for better performance:
```scala
import org.apache.spark.ml.classification.LogisticRegression

val lr = new LogisticRegression()
  .setMaxIter(10)
  .setParallelism(4)  // Parallel model fitting
```

### Data Format

Use Parquet format for efficient storage and reading:
```scala
df.write.parquet("training_data.parquet")
val data = spark.read.parquet("training_data.parquet")
```

### Feature Scaling

Normalize features for better convergence:
```scala
import org.apache.spark.ml.feature.StandardScaler

val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setWithStd(true)
  .setWithMean(false)
```

## Best Practices

1. **Use spark.ml**: Prefer DataFrame-based API over RDD-based API
2. **Build pipelines**: Use Pipeline API for reproducible workflows
3. **Cache data**: Cache datasets used in iterative algorithms
4. **Scale features**: Normalize features for better performance
5. **Cross-validate**: Use cross-validation for model selection
6. **Monitor convergence**: Check convergence for iterative algorithms
7. **Save models**: Persist trained models for reuse
8. **Use appropriate algorithms**: Choose algorithms based on data characteristics

## Model Persistence

Save and load models:

```scala
// Save model
model.write.overwrite().save("path/to/model")

// Load model
val loadedModel = PipelineModel.load("path/to/model")
```

## Migration Guide

### From RDD-based API to DataFrame-based API

**Old (RDD-based):**
```scala
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint

val data: RDD[LabeledPoint] = ...
val model = LogisticRegressionWithLBFGS.train(data)
```

**New (DataFrame-based):**
```scala
import org.apache.spark.ml.classification.LogisticRegression

val data: DataFrame = ...
val lr = new LogisticRegression()
val model = lr.fit(data)
```

## Examples

See [examples/src/main/scala/org/apache/spark/examples/ml/](../examples/src/main/scala/org/apache/spark/examples/ml/) for complete examples.

## Further Reading

- [ML Programming Guide](../docs/ml-guide.md) (DataFrame-based API)
- [MLlib Programming Guide](../docs/mllib-guide.md) (RDD-based API - legacy)
- [ML Pipelines](../docs/ml-pipeline.md)
- [ML Tuning](../docs/ml-tuning.md)
- [Feature Extraction](../docs/ml-features.md)

## Contributing

For contributing to MLlib, see [CONTRIBUTING.md](../CONTRIBUTING.md).

New features should use the DataFrame-based API (`spark.ml`).

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.classification import LogisticRegression

if __name__ == "__main__":
    sc = SparkContext(appName="SimpleTextClassificationPipeline")
    sqlCtx = SQLContext(sc)
    training = sqlCtx.inferSchema(
        sc.parallelize([(0L, "a b c d e spark", 1.0), (1L, "b d", 0.0), (2L, "spark f g h", 1.0), (3L, "hadoop mapreduce", 0.0)]) \
          .map(lambda x: Row(id=x[0], text=x[1], label=x[2])))

    tokenizer = Tokenizer() \
      .setInputCol("text") \
      .setOutputCol("words")
    hashingTF = HashingTF() \
      .setInputCol(tokenizer.getOutputCol()) \
      .setOutputCol("features")
    lr = LogisticRegression() \
      .setMaxIter(10) \
      .setRegParam(0.01)
    pipeline = Pipeline() \
      .setStages([tokenizer, hashingTF, lr])

    model = pipeline.fit(training)

    test = sqlCtx.inferSchema(
        sc.parallelize([(4L, "spark i j k"), (5L, "l m n"), (6L, "mapreduce spark"), (7L, "apache hadoop")]) \
          .map(lambda x: Row(id=x[0], text=x[1])))

    for row in model.transform(test).collect():
        print row

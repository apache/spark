---
layout: global
title: Feature Extraction, Transformation, and Selection - SparkML
displayTitle: <a href="ml-guide.html">ML</a> - Features
---

This section covers algorithms for working with features, roughly divided into these groups:

* Extraction: Extracting features from "raw" data
* Transformation: Scaling, converting, or modifying features
* Selection: Selecting a subset from a larger set of features

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}


# Feature Extractors

## Hashing Term-Frequency (HashingTF)

`HashingTF` is a `Transformer` which takes sets of terms (e.g., `String` terms can be sets of words) and converts those sets into fixed-length feature vectors.
The algorithm combines [Term Frequency (TF)](http://en.wikipedia.org/wiki/Tf%E2%80%93idf) counts with the [hashing trick](http://en.wikipedia.org/wiki/Feature_hashing) for dimensionality reduction.  Please refer to the [MLlib user guide on TF-IDF](mllib-feature-extraction.html#tf-idf) for more details on Term-Frequency.

HashingTF is implemented in
[HashingTF](api/scala/index.html#org.apache.spark.ml.feature.HashingTF).
In the following code segment, we start with a set of sentences.  We split each sentence into words using `Tokenizer`.  For each sentence (bag of words), we hash it into a feature vector.  This feature vector could then be passed to a learning algorithm.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

val sentenceDataFrame = sqlContext.createDataFrame(Seq(
  (0, "Hi I heard about Spark"),
  (0, "I wish Java could use case classes"),
  (1, "Logistic regression models are neat")
)).toDF("label", "sentence")
val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
val wordsDataFrame = tokenizer.transform(sentenceDataFrame)
val hashingTF = new HashingTF().setInputCol("words").setOutputCol("features").setNumFeatures(20)
val featurized = hashingTF.transform(wordsDataFrame)
featurized.select("features", "label").take(3).foreach(println)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import com.google.common.collect.Lists;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

JavaRDD<Row> jrdd = jsc.parallelize(Lists.newArrayList(
  RowFactory.create(0, "Hi I heard about Spark"),
  RowFactory.create(0, "I wish Java could use case classes"),
  RowFactory.create(1, "Logistic regression models are neat")
));
StructType schema = new StructType(new StructField[]{
  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
  new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
});
DataFrame sentenceDataFrame = sqlContext.createDataFrame(jrdd, schema);
Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
DataFrame wordsDataFrame = tokenizer.transform(sentenceDataFrame);
int numFeatures = 20;
HashingTF hashingTF = new HashingTF()
  .setInputCol("words")
  .setOutputCol("features")
  .setNumFeatures(numFeatures);
DataFrame featurized = hashingTF.transform(wordsDataFrame);
for (Row r : featurized.select("features", "label").take(3)) {
  Vector features = r.getAs(0);
  Double label = r.getDouble(1);
  System.out.println(features);
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.ml.feature import HashingTF, Tokenizer

sentenceDataFrame = sqlContext.createDataFrame([
  (0, "Hi I heard about Spark"),
  (0, "I wish Java could use case classes"),
  (1, "Logistic regression models are neat")
], ["label", "sentence"])
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsDataFrame = tokenizer.transform(sentenceDataFrame)
hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=20)
featurized = hashingTF.transform(wordsDataFrame)
for features_label in featurized.select("features", "label").take(3):
  print features_label
{% endhighlight %}
</div>
</div>


# Feature Transformers

## Tokenizer

[Tokenization](http://en.wikipedia.org/wiki/Lexical_analysis#Tokenization) is the process of taking text (such as a sentence) and breaking it into individual terms (usually words).  A simple [Tokenizer](api/scala/index.html#org.apache.spark.ml.feature.Tokenizer) class provides this functionality.  The example below shows how to split sentences into sequences of words.

Note: A more advanced tokenizer is provided via [RegexTokenizer](api/scala/index.html#org.apache.spark.ml.feature.RegexTokenizer).

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.Tokenizer

val sentenceDataFrame = sqlContext.createDataFrame(Seq(
  (0, "Hi I heard about Spark"),
  (0, "I wish Java could use case classes"),
  (1, "Logistic regression models are neat")
)).toDF("label", "sentence")
val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
val wordsDataFrame = tokenizer.transform(sentenceDataFrame)
wordsDataFrame.select("words", "label").take(3).foreach(println)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import com.google.common.collect.Lists;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

JavaRDD<Row> jrdd = jsc.parallelize(Lists.newArrayList(
  RowFactory.create(0, "Hi I heard about Spark"),
  RowFactory.create(0, "I wish Java could use case classes"),
  RowFactory.create(1, "Logistic regression models are neat")
));
StructType schema = new StructType(new StructField[]{
  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
  new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
});
DataFrame sentenceDataFrame = sqlContext.createDataFrame(jrdd, schema);
Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
DataFrame wordsDataFrame = tokenizer.transform(sentenceDataFrame);
for (Row r : wordsDataFrame.select("words", "label").take(3)) {
  java.util.List<String> words = r.getList(0);
  for (String word : words) System.out.print(word + " ");
  System.out.println();
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.ml.feature import Tokenizer

sentenceDataFrame = sqlContext.createDataFrame([
  (0, "Hi I heard about Spark"),
  (0, "I wish Java could use case classes"),
  (1, "Logistic regression models are neat")
], ["label", "sentence"])
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsDataFrame = tokenizer.transform(sentenceDataFrame)
for words_label in wordsDataFrame.select("words", "label").take(3):
  print words_label
{% endhighlight %}
</div>
</div>


# Feature Selectors


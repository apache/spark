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

## TF-IDF (HashingTF and IDF)

[Term Frequency-Inverse Document Frequency (TF-IDF)](http://en.wikipedia.org/wiki/Tf%E2%80%93idf) is a common text pre-processing step.  In Spark ML, TF-IDF is separate into two parts: TF (+hashing) and IDF.

**TF**: `HashingTF` is a `Transformer` which takes sets of terms and converts those sets into fixed-length feature vectors.  In text processing, a "set of terms" might be a bag of words.
The algorithm combines Term Frequency (TF) counts with the [hashing trick](http://en.wikipedia.org/wiki/Feature_hashing) for dimensionality reduction.

**IDF**: `IDF` is an `Estimator` which fits on a dataset and produces an `IDFModel`.  The `IDFModel` takes feature vectors (generally created from `HashingTF`) and scales each column.  Intuitively, it down-weights columns which appear frequently in a corpus.

Please refer to the [MLlib user guide on TF-IDF](mllib-feature-extraction.html#tf-idf) for more details on Term Frequency and Inverse Document Frequency.
For API details, refer to the [HashingTF API docs](api/scala/index.html#org.apache.spark.ml.feature.HashingTF) and the [IDF API docs](api/scala/index.html#org.apache.spark.ml.feature.IDF).

In the following code segment, we start with a set of sentences.  We split each sentence into words using `Tokenizer`.  For each sentence (bag of words), we use `HashingTF` to hash the sentence into a feature vector.  We use `IDF` to rescale the feature vectors; this generally improves performance when using text as features.  Our feature vectors could then be passed to a learning algorithm.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

val sentenceData = sqlContext.createDataFrame(Seq(
  (0, "Hi I heard about Spark"),
  (0, "I wish Java could use case classes"),
  (1, "Logistic regression models are neat")
)).toDF("label", "sentence")
val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
val wordsData = tokenizer.transform(sentenceData)
val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
val featurizedData = hashingTF.transform(wordsData)
val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val idfModel = idf.fit(featurizedData)
val rescaledData = idfModel.transform(featurizedData)
rescaledData.select("features", "label").take(3).foreach(println)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
  RowFactory.create(0, "Hi I heard about Spark"),
  RowFactory.create(0, "I wish Java could use case classes"),
  RowFactory.create(1, "Logistic regression models are neat")
));
StructType schema = new StructType(new StructField[]{
  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
  new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
});
DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);
Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
DataFrame wordsData = tokenizer.transform(sentenceData);
int numFeatures = 20;
HashingTF hashingTF = new HashingTF()
  .setInputCol("words")
  .setOutputCol("rawFeatures")
  .setNumFeatures(numFeatures);
DataFrame featurizedData = hashingTF.transform(wordsData);
IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
IDFModel idfModel = idf.fit(featurizedData);
DataFrame rescaledData = idfModel.transform(featurizedData);
for (Row r : rescaledData.select("features", "label").take(3)) {
  Vector features = r.getAs(0);
  Double label = r.getDouble(1);
  System.out.println(features);
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

sentenceData = sqlContext.createDataFrame([
  (0, "Hi I heard about Spark"),
  (0, "I wish Java could use case classes"),
  (1, "Logistic regression models are neat")
], ["label", "sentence"])
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)
for features_label in rescaledData.select("features", "label").take(3):
  print(features_label)
{% endhighlight %}
</div>
</div>

## Word2Vec

`Word2Vec` is an `Estimator` which takes sequences of words that represents documents and trains a `Word2VecModel`. The model is a `Map(String, Vector)` essentially, which maps each word to an unique fix-sized vector. The `Word2VecModel` transforms each documents into a vector using the average of all words in the document, which aims to other computations of documents such as similarity calculation consequencely. Please refer to the [MLlib user guide on Word2Vec](mllib-feature-extraction.html#Word2Vec) for more details on Word2Vec.

Word2Vec is implemented in [Word2Vec](api/scala/index.html#org.apache.spark.ml.feature.Word2Vec). In the following code segment, we start with a set of documents, each of them is represented as a sequence of words. For each document, we transform it into a feature vector. This feature vector could then be passed to a learning algorithm.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.Word2Vec

// Input data: Each row is a bag of words from a sentence or document.
val documentDF = sqlContext.createDataFrame(Seq(
  "Hi I heard about Spark".split(" "),
  "I wish Java could use case classes".split(" "),
  "Logistic regression models are neat".split(" ")
).map(Tuple1.apply)).toDF("text")

// Learn a mapping from words to Vectors.
val word2Vec = new Word2Vec()
  .setInputCol("text")
  .setOutputCol("result")
  .setVectorSize(3)
  .setMinCount(0)
val model = word2Vec.fit(documentDF)
val result = model.transform(documentDF)
result.select("result").take(3).foreach(println)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

JavaSparkContext jsc = ...
SQLContext sqlContext = ...

// Input data: Each row is a bag of words from a sentence or document.
JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
  RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
  RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
  RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
));
StructType schema = new StructType(new StructField[]{
  new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
});
DataFrame documentDF = sqlContext.createDataFrame(jrdd, schema);

// Learn a mapping from words to Vectors.
Word2Vec word2Vec = new Word2Vec()
  .setInputCol("text")
  .setOutputCol("result")
  .setVectorSize(3)
  .setMinCount(0);
Word2VecModel model = word2Vec.fit(documentDF);
DataFrame result = model.transform(documentDF);
for (Row r: result.select("result").take(3)) {
  System.out.println(r);
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.ml.feature import Word2Vec

# Input data: Each row is a bag of words from a sentence or document.
documentDF = sqlContext.createDataFrame([
  ("Hi I heard about Spark".split(" "), ),
  ("I wish Java could use case classes".split(" "), ),
  ("Logistic regression models are neat".split(" "), )
], ["text"])
# Learn a mapping from words to Vectors.
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="result")
model = word2Vec.fit(documentDF)
result = model.transform(documentDF)
for feature in result.select("result").take(3):
  print(feature)
{% endhighlight %}
</div>
</div>

## CountVectorizer

`CountVectorizer` and `CountVectorizerModel` aim to help convert a collection of text documents
 to vectors of token counts. When an a-priori dictionary is not available, `CountVectorizer` can
 be used as an `Estimator` to extract the vocabulary and generates a `CountVectorizerModel`. The
 model produces sparse representations for the documents over the vocabulary, which can then be
 passed to other algorithms like LDA.

 During the fitting process, `CountVectorizer` will select the top `vocabSize` words ordered by
 term frequency across the corpus. An optional parameter "minDF" also affect the fitting process
 by specifying the minimum number (or fraction if < 1.0) of documents a term must appear in to be
 included in the vocabulary.

**Examples**

Assume that we have the following DataFrame with columns `id` and `texts`:

~~~~
 id | texts
----|----------
 0  | Array("a", "b", "c")
 1  | Array("a", "b", "b", "c", "a")
~~~~

each row in`texts` is a document of type Array[String].
Invoking fit of `CountVectorizer` produces a `CountVectorizerModel` with vocabulary (a, b, c),
then the output column "vector" after transformation contains:

~~~~
 id | texts                           | vector
----|---------------------------------|---------------
 0  | Array("a", "b", "c")            | (3,[0,1,2],[1.0,1.0,1.0])
 1  | Array("a", "b", "b", "c", "a")  | (3,[0,1,2],[2.0,2.0,1.0])
~~~~

each vector represents the token counts of the document over the vocabulary.

<div class="codetabs">
<div data-lang="scala" markdown="1">
More details can be found in the API docs for
[CountVectorizer](api/scala/index.html#org.apache.spark.ml.feature.CountVectorizer) and
[CountVectorizerModel](api/scala/index.html#org.apache.spark.ml.feature.CountVectorizerModel).
{% highlight scala %}
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.util.CountVectorizerModel

val df = sqlContext.createDataFrame(Seq(
  (0, Array("a", "b", "c")),
  (1, Array("a", "b", "b", "c", "a"))
)).toDF("id", "words")

// fit a CountVectorizerModel from the corpus
val cvModel: CountVectorizerModel = new CountVectorizer()
  .setInputCol("words")
  .setOutputCol("features")
  .setVocabSize(3)
  .setMinDF(2) // a term must appear in more or equal to 2 documents to be included in the vocabulary
  .fit(df)

// alternatively, define CountVectorizerModel with a-priori vocabulary
val cvm = new CountVectorizerModel(Array("a", "b", "c"))
  .setInputCol("words")
  .setOutputCol("features")

cvModel.transform(df).select("features").show()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
More details can be found in the API docs for
[CountVectorizer](api/java/org/apache/spark/ml/feature/CountVectorizer.html) and
[CountVectorizerModel](api/java/org/apache/spark/ml/feature/CountVectorizerModel.html).
{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

// Input data: Each row is a bag of words from a sentence or document.
JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
  RowFactory.create(Arrays.asList("a", "b", "c")),
  RowFactory.create(Arrays.asList("a", "b", "b", "c", "a"))
));
StructType schema = new StructType(new StructField [] {
  new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
});
DataFrame df = sqlContext.createDataFrame(jrdd, schema);

// fit a CountVectorizerModel from the corpus
CountVectorizerModel cvModel = new CountVectorizer()
  .setInputCol("text")
  .setOutputCol("feature")
  .setVocabSize(3)
  .setMinDF(2) // a term must appear in more or equal to 2 documents to be included in the vocabulary
  .fit(df);

// alternatively, define CountVectorizerModel with a-priori vocabulary
CountVectorizerModel cvm = new CountVectorizerModel(new String[]{"a", "b", "c"})
  .setInputCol("text")
  .setOutputCol("feature");

cvModel.transform(df).show();
{% endhighlight %}
</div>
</div>

# Feature Transformers

## Tokenizer

[Tokenization](http://en.wikipedia.org/wiki/Lexical_analysis#Tokenization) is the process of taking text (such as a sentence) and breaking it into individual terms (usually words).  A simple [Tokenizer](api/scala/index.html#org.apache.spark.ml.feature.Tokenizer) class provides this functionality.  The example below shows how to split sentences into sequences of words.

[RegexTokenizer](api/scala/index.html#org.apache.spark.ml.feature.RegexTokenizer) allows more
 advanced tokenization based on regular expression (regex) matching.
 By default, the parameter "pattern" (regex, default: \\s+) is used as delimiters to split the input text.
 Alternatively, users can set parameter "gaps" to false indicating the regex "pattern" denotes
 "tokens" rather than splitting gaps, and find all matching occurrences as the tokenization result.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.{Tokenizer, RegexTokenizer}

val sentenceDataFrame = sqlContext.createDataFrame(Seq(
  (0, "Hi I heard about Spark"),
  (1, "I wish Java could use case classes"),
  (2, "Logistic,regression,models,are,neat")
)).toDF("label", "sentence")
val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
val regexTokenizer = new RegexTokenizer()
  .setInputCol("sentence")
  .setOutputCol("words")
  .setPattern("\\W")  // alternatively .setPattern("\\w+").setGaps(false)

val tokenized = tokenizer.transform(sentenceDataFrame)
tokenized.select("words", "label").take(3).foreach(println)
val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
regexTokenized.select("words", "label").take(3).foreach(println)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
  RowFactory.create(0, "Hi I heard about Spark"),
  RowFactory.create(1, "I wish Java could use case classes"),
  RowFactory.create(2, "Logistic,regression,models,are,neat")
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

RegexTokenizer regexTokenizer = new RegexTokenizer()
  .setInputCol("sentence")
  .setOutputCol("words")
  .setPattern("\\W");  // alternatively .setPattern("\\w+").setGaps(false);
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.ml.feature import Tokenizer, RegexTokenizer

sentenceDataFrame = sqlContext.createDataFrame([
  (0, "Hi I heard about Spark"),
  (1, "I wish Java could use case classes"),
  (2, "Logistic,regression,models,are,neat")
], ["label", "sentence"])
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsDataFrame = tokenizer.transform(sentenceDataFrame)
for words_label in wordsDataFrame.select("words", "label").take(3):
  print(words_label)
regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", pattern="\\W")
# alternatively, pattern="\\w+", gaps(False)
{% endhighlight %}
</div>
</div>

## StopWordsRemover
[Stop words](https://en.wikipedia.org/wiki/Stop_words) are words which
should be excluded from the input, typically because the words appear
frequently and don't carry as much meaning.

`StopWordsRemover` takes as input a sequence of strings (e.g. the output
of a [Tokenizer](ml-features.html#tokenizer)) and drops all the stop
words from the input sequences. The list of stopwords is specified by
the `stopWords` parameter.  We provide [a list of stop
words](http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words) by
default, accessible by calling `getStopWords` on a newly instantiated
`StopWordsRemover` instance.

**Examples**

Assume that we have the following DataFrame with columns `id` and `raw`:

~~~~
 id | raw
----|----------
 0  | [I, saw, the, red, baloon]
 1  | [Mary, had, a, little, lamb]
~~~~

Applying `StopWordsRemover` with `raw` as the input column and `filtered` as the output
column, we should get the following:

~~~~
 id | raw                         | filtered
----|-----------------------------|--------------------
 0  | [I, saw, the, red, baloon]  |  [saw, red, baloon]
 1  | [Mary, had, a, little, lamb]|[Mary, little, lamb]
~~~~

In `filtered`, the stop words "I", "the", "had", and "a" have been
filtered out.

<div class="codetabs">

<div data-lang="scala" markdown="1">

[`StopWordsRemover`](api/scala/index.html#org.apache.spark.ml.feature.StopWordsRemover)
takes an input column name, an output column name, a list of stop words,
and a boolean indicating if the matches should be case sensitive (false
by default).

{% highlight scala %}
import org.apache.spark.ml.feature.StopWordsRemover

val remover = new StopWordsRemover()
  .setInputCol("raw")
  .setOutputCol("filtered")
val dataSet = sqlContext.createDataFrame(Seq(
  (0, Seq("I", "saw", "the", "red", "baloon")),
  (1, Seq("Mary", "had", "a", "little", "lamb"))
)).toDF("id", "raw")

remover.transform(dataSet).show()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[`StopWordsRemover`](api/java/org/apache/spark/ml/feature/StopWordsRemover.html)
takes an input column name, an output column name, a list of stop words,
and a boolean indicating if the matches should be case sensitive (false
by default).

{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

StopWordsRemover remover = new StopWordsRemover()
  .setInputCol("raw")
  .setOutputCol("filtered");

JavaRDD<Row> rdd = jsc.parallelize(Arrays.asList(
  RowFactory.create(Arrays.asList("I", "saw", "the", "red", "baloon")),
  RowFactory.create(Arrays.asList("Mary", "had", "a", "little", "lamb"))
));
StructType schema = new StructType(new StructField[] {
  new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
});
DataFrame dataset = jsql.createDataFrame(rdd, schema);

remover.transform(dataset).show();
{% endhighlight %}
</div>
</div>

## $n$-gram

An [n-gram](https://en.wikipedia.org/wiki/N-gram) is a sequence of $n$ tokens (typically words) for some integer $n$. The `NGram` class can be used to transform input features into $n$-grams.

`NGram` takes as input a sequence of strings (e.g. the output of a [Tokenizer](ml-features.html#tokenizer)).  The parameter `n` is used to determine the number of terms in each $n$-gram. The output will consist of a sequence of $n$-grams where each $n$-gram is represented by a space-delimited string of $n$ consecutive words.  If the input sequence contains fewer than `n` strings, no output is produced.

<div class="codetabs">

<div data-lang="scala" markdown="1">

[`NGram`](api/scala/index.html#org.apache.spark.ml.feature.NGram) takes an input column name, an output column name, and an optional length parameter n (n=2 by default).

{% highlight scala %}
import org.apache.spark.ml.feature.NGram

val wordDataFrame = sqlContext.createDataFrame(Seq(
  (0, Array("Hi", "I", "heard", "about", "Spark")),
  (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
  (2, Array("Logistic", "regression", "models", "are", "neat"))
)).toDF("label", "words")

val ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
val ngramDataFrame = ngram.transform(wordDataFrame)
ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[`NGram`](api/java/org/apache/spark/ml/feature/NGram.html) takes an input column name, an output column name, and an optional length parameter n (n=2 by default).

{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.NGram;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
  RowFactory.create(0.0, Arrays.asList("Hi", "I", "heard", "about", "Spark")),
  RowFactory.create(1.0, Arrays.asList("I", "wish", "Java", "could", "use", "case", "classes")),
  RowFactory.create(2.0, Arrays.asList("Logistic", "regression", "models", "are", "neat"))
));
StructType schema = new StructType(new StructField[]{
  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
  new StructField("words", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
});
DataFrame wordDataFrame = sqlContext.createDataFrame(jrdd, schema);
NGram ngramTransformer = new NGram().setInputCol("words").setOutputCol("ngrams");
DataFrame ngramDataFrame = ngramTransformer.transform(wordDataFrame);
for (Row r : ngramDataFrame.select("ngrams", "label").take(3)) {
  java.util.List<String> ngrams = r.getList(0);
  for (String ngram : ngrams) System.out.print(ngram + " --- ");
  System.out.println();
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

[`NGram`](api/python/pyspark.ml.html#pyspark.ml.feature.NGram) takes an input column name, an output column name, and an optional length parameter n (n=2 by default).

{% highlight python %}
from pyspark.ml.feature import NGram

wordDataFrame = sqlContext.createDataFrame([
  (0, ["Hi", "I", "heard", "about", "Spark"]),
  (1, ["I", "wish", "Java", "could", "use", "case", "classes"]),
  (2, ["Logistic", "regression", "models", "are", "neat"])
], ["label", "words"])
ngram = NGram(inputCol="words", outputCol="ngrams")
ngramDataFrame = ngram.transform(wordDataFrame)
for ngrams_label in ngramDataFrame.select("ngrams", "label").take(3):
  print(ngrams_label)
{% endhighlight %}
</div>
</div>


## Binarizer

Binarization is the process of thresholding numerical features to binary features. As some probabilistic estimators make assumption that the input data is distributed according to [Bernoulli distribution](http://en.wikipedia.org/wiki/Bernoulli_distribution), a binarizer is useful for pre-processing the input data with continuous numerical features.

A simple [Binarizer](api/scala/index.html#org.apache.spark.ml.feature.Binarizer) class provides this functionality. Besides the common parameters of `inputCol` and `outputCol`, `Binarizer` has the parameter `threshold` used for binarizing continuous numerical features. The features greater than the threshold, will be binarized to 1.0. The features equal to or less than the threshold, will be binarized to 0.0. The example below shows how to binarize numerical features.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.DataFrame

val data = Array(
  (0, 0.1),
  (1, 0.8),
  (2, 0.2)
)
val dataFrame: DataFrame = sqlContext.createDataFrame(data).toDF("label", "feature")

val binarizer: Binarizer = new Binarizer()
  .setInputCol("feature")
  .setOutputCol("binarized_feature")
  .setThreshold(0.5)

val binarizedDataFrame = binarizer.transform(dataFrame)
val binarizedFeatures = binarizedDataFrame.select("binarized_feature")
binarizedFeatures.collect().foreach(println)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.Binarizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
  RowFactory.create(0, 0.1),
  RowFactory.create(1, 0.8),
  RowFactory.create(2, 0.2)
));
StructType schema = new StructType(new StructField[]{
  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
  new StructField("feature", DataTypes.DoubleType, false, Metadata.empty())
});
DataFrame continuousDataFrame = jsql.createDataFrame(jrdd, schema);
Binarizer binarizer = new Binarizer()
  .setInputCol("feature")
  .setOutputCol("binarized_feature")
  .setThreshold(0.5);
DataFrame binarizedDataFrame = binarizer.transform(continuousDataFrame);
DataFrame binarizedFeatures = binarizedDataFrame.select("binarized_feature");
for (Row r : binarizedFeatures.collect()) {
  Double binarized_value = r.getDouble(0);
  System.out.println(binarized_value);
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.ml.feature import Binarizer

continuousDataFrame = sqlContext.createDataFrame([
  (0, 0.1),
  (1, 0.8),
  (2, 0.2)
], ["label", "feature"])
binarizer = Binarizer(threshold=0.5, inputCol="feature", outputCol="binarized_feature")
binarizedDataFrame = binarizer.transform(continuousDataFrame)
binarizedFeatures = binarizedDataFrame.select("binarized_feature")
for binarized_feature, in binarizedFeatures.collect():
  print(binarized_feature)
{% endhighlight %}
</div>
</div>

## PCA

[PCA](http://en.wikipedia.org/wiki/Principal_component_analysis) is a statistical procedure that uses an orthogonal transformation to convert a set of observations of possibly correlated variables into a set of values of linearly uncorrelated variables called principal components. A [PCA](api/scala/index.html#org.apache.spark.ml.feature.PCA) class trains a model to project vectors to a low-dimensional space using PCA. The example below shows how to project 5-dimensional feature vectors into 3-dimensional principal components.

<div class="codetabs">
<div data-lang="scala" markdown="1">
See the [Scala API documentation](api/scala/index.html#org.apache.spark.ml.feature.PCA) for API details.
{% highlight scala %}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.Vectors

val data = Array(
  Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
  Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
  Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
)
val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
val pca = new PCA()
  .setInputCol("features")
  .setOutputCol("pcaFeatures")
  .setK(3)
  .fit(df)
val pcaDF = pca.transform(df)
val result = pcaDF.select("pcaFeatures")
result.show()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
See the [Java API documentation](api/java/org/apache/spark/ml/feature/PCA.html) for API details.
{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

JavaSparkContext jsc = ...
SQLContext jsql = ...
JavaRDD<Row> data = jsc.parallelize(Arrays.asList(
  RowFactory.create(Vectors.sparse(5, new int[]{1, 3}, new double[]{1.0, 7.0})),
  RowFactory.create(Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0)),
  RowFactory.create(Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
));
StructType schema = new StructType(new StructField[] {
  new StructField("features", new VectorUDT(), false, Metadata.empty()),
});
DataFrame df = jsql.createDataFrame(data, schema);
PCAModel pca = new PCA()
  .setInputCol("features")
  .setOutputCol("pcaFeatures")
  .setK(3)
  .fit(df);
DataFrame result = pca.transform(df).select("pcaFeatures");
result.show();
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
See the [Python API documentation](api/python/pyspark.ml.html#pyspark.ml.feature.PCA) for API details.
{% highlight python %}
from pyspark.ml.feature import PCA
from pyspark.mllib.linalg import Vectors

data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
  (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
  (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
df = sqlContext.createDataFrame(data,["features"])
pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
model = pca.fit(df)
result = model.transform(df).select("pcaFeatures")
result.show(truncate=False)
{% endhighlight %}
</div>
</div>

## PolynomialExpansion

[Polynomial expansion](http://en.wikipedia.org/wiki/Polynomial_expansion) is the process of expanding your features into a polynomial space, which is formulated by an n-degree combination of original dimensions. A [PolynomialExpansion](api/scala/index.html#org.apache.spark.ml.feature.PolynomialExpansion) class provides this functionality.  The example below shows how to expand your features into a 3-degree polynomial space.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.mllib.linalg.Vectors

val data = Array(
  Vectors.dense(-2.0, 2.3),
  Vectors.dense(0.0, 0.0),
  Vectors.dense(0.6, -1.1)
)
val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
val polynomialExpansion = new PolynomialExpansion()
  .setInputCol("features")
  .setOutputCol("polyFeatures")
  .setDegree(3)
val polyDF = polynomialExpansion.transform(df)
polyDF.select("polyFeatures").take(3).foreach(println)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

JavaSparkContext jsc = ...
SQLContext jsql = ...
PolynomialExpansion polyExpansion = new PolynomialExpansion()
  .setInputCol("features")
  .setOutputCol("polyFeatures")
  .setDegree(3);
JavaRDD<Row> data = jsc.parallelize(Arrays.asList(
  RowFactory.create(Vectors.dense(-2.0, 2.3)),
  RowFactory.create(Vectors.dense(0.0, 0.0)),
  RowFactory.create(Vectors.dense(0.6, -1.1))
));
StructType schema = new StructType(new StructField[] {
  new StructField("features", new VectorUDT(), false, Metadata.empty()),
});
DataFrame df = jsql.createDataFrame(data, schema);
DataFrame polyDF = polyExpansion.transform(df);
Row[] row = polyDF.select("polyFeatures").take(3);
for (Row r : row) {
  System.out.println(r.get(0));
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.ml.feature import PolynomialExpansion
from pyspark.mllib.linalg import Vectors

df = sqlContext.createDataFrame(
  [(Vectors.dense([-2.0, 2.3]), ),
  (Vectors.dense([0.0, 0.0]), ),
  (Vectors.dense([0.6, -1.1]), )],
  ["features"])
px = PolynomialExpansion(degree=2, inputCol="features", outputCol="polyFeatures")
polyDF = px.transform(df)
for expanded in polyDF.select("polyFeatures").take(3):
  print(expanded)
{% endhighlight %}
</div>
</div>

## Discrete Cosine Transform (DCT)

The [Discrete Cosine
Transform](https://en.wikipedia.org/wiki/Discrete_cosine_transform)
transforms a length $N$ real-valued sequence in the time domain into
another length $N$ real-valued sequence in the frequency domain. A
[DCT](api/scala/index.html#org.apache.spark.ml.feature.DCT) class
provides this functionality, implementing the
[DCT-II](https://en.wikipedia.org/wiki/Discrete_cosine_transform#DCT-II)
and scaling the result by $1/\sqrt{2}$ such that the representing matrix
for the transform is unitary. No shift is applied to the transformed
sequence (e.g. the $0$th element of the transformed sequence is the
$0$th DCT coefficient and _not_ the $N/2$th).

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.DCT
import org.apache.spark.mllib.linalg.Vectors

val data = Seq(
  Vectors.dense(0.0, 1.0, -2.0, 3.0),
  Vectors.dense(-1.0, 2.0, 4.0, -7.0),
  Vectors.dense(14.0, -2.0, -5.0, 1.0))
val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
val dct = new DCT()
  .setInputCol("features")
  .setOutputCol("featuresDCT")
  .setInverse(false)
val dctDf = dct.transform(df)
dctDf.select("featuresDCT").show(3)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.DCT;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

JavaRDD<Row> data = jsc.parallelize(Arrays.asList(
  RowFactory.create(Vectors.dense(0.0, 1.0, -2.0, 3.0)),
  RowFactory.create(Vectors.dense(-1.0, 2.0, 4.0, -7.0)),
  RowFactory.create(Vectors.dense(14.0, -2.0, -5.0, 1.0))
));
StructType schema = new StructType(new StructField[] {
  new StructField("features", new VectorUDT(), false, Metadata.empty()),
});
DataFrame df = jsql.createDataFrame(data, schema);
DCT dct = new DCT()
  .setInputCol("features")
  .setOutputCol("featuresDCT")
  .setInverse(false);
DataFrame dctDf = dct.transform(df);
dctDf.select("featuresDCT").show(3);
{% endhighlight %}
</div>
</div>

## StringIndexer

`StringIndexer` encodes a string column of labels to a column of label indices.
The indices are in `[0, numLabels)`, ordered by label frequencies.
So the most frequent label gets index `0`.
If the input column is numeric, we cast it to string and index the string 
values. When downstream pipeline components such as `Estimator` or 
`Transformer` make use of this string-indexed label, you must set the input 
column of the component to this string-indexed column name. In many cases, 
you can set the input column with `setInputCol`.

**Examples**

Assume that we have the following DataFrame with columns `id` and `category`:

~~~~
 id | category
----|----------
 0  | a
 1  | b
 2  | c
 3  | a
 4  | a
 5  | c
~~~~

`category` is a string column with three labels: "a", "b", and "c".
Applying `StringIndexer` with `category` as the input column and `categoryIndex` as the output
column, we should get the following:

~~~~
 id | category | categoryIndex
----|----------|---------------
 0  | a        | 0.0
 1  | b        | 2.0
 2  | c        | 1.0
 3  | a        | 0.0
 4  | a        | 0.0
 5  | c        | 1.0
~~~~

"a" gets index `0` because it is the most frequent, followed by "c" with index `1` and "b" with
index `2`.

<div class="codetabs">

<div data-lang="scala" markdown="1">

[`StringIndexer`](api/scala/index.html#org.apache.spark.ml.feature.StringIndexer) takes an input
column name and an output column name.

{% highlight scala %}
import org.apache.spark.ml.feature.StringIndexer

val df = sqlContext.createDataFrame(
  Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
).toDF("id", "category")
val indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIndex")
val indexed = indexer.fit(df).transform(df)
indexed.show()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`StringIndexer`](api/java/org/apache/spark/ml/feature/StringIndexer.html) takes an input column
name and an output column name.

{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.types.DataTypes.*;

JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
  RowFactory.create(0, "a"),
  RowFactory.create(1, "b"),
  RowFactory.create(2, "c"),
  RowFactory.create(3, "a"),
  RowFactory.create(4, "a"),
  RowFactory.create(5, "c")
));
StructType schema = new StructType(new StructField[] {
  createStructField("id", DoubleType, false),
  createStructField("category", StringType, false)
});
DataFrame df = sqlContext.createDataFrame(jrdd, schema);
StringIndexer indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIndex");
DataFrame indexed = indexer.fit(df).transform(df);
indexed.show();
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

[`StringIndexer`](api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer) takes an input
column name and an output column name.

{% highlight python %}
from pyspark.ml.feature import StringIndexer

df = sqlContext.createDataFrame(
    [(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")],
    ["id", "category"])
indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
indexed = indexer.fit(df).transform(df)
indexed.show()
{% endhighlight %}
</div>
</div>

## OneHotEncoder

[One-hot encoding](http://en.wikipedia.org/wiki/One-hot) maps a column of label indices to a column of binary vectors, with at most a single one-value. This encoding allows algorithms which expect continuous features, such as Logistic Regression, to use categorical features 

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

val df = sqlContext.createDataFrame(Seq(
  (0, "a"),
  (1, "b"),
  (2, "c"),
  (3, "a"),
  (4, "a"),
  (5, "c")
)).toDF("id", "category")

val indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIndex")
  .fit(df)
val indexed = indexer.transform(df)

val encoder = new OneHotEncoder().setInputCol("categoryIndex").
  setOutputCol("categoryVec")
val encoded = encoder.transform(indexed)
encoded.select("id", "categoryVec").foreach(println)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
    RowFactory.create(0, "a"),
    RowFactory.create(1, "b"),
    RowFactory.create(2, "c"),
    RowFactory.create(3, "a"),
    RowFactory.create(4, "a"),
    RowFactory.create(5, "c")
));
StructType schema = new StructType(new StructField[]{
    new StructField("id", DataTypes.DoubleType, false, Metadata.empty()),
    new StructField("category", DataTypes.StringType, false, Metadata.empty())
});
DataFrame df = sqlContext.createDataFrame(jrdd, schema);
StringIndexerModel indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIndex")
  .fit(df);
DataFrame indexed = indexer.transform(df);

OneHotEncoder encoder = new OneHotEncoder()
  .setInputCol("categoryIndex")
  .setOutputCol("categoryVec");
DataFrame encoded = encoder.transform(indexed);
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.ml.feature import OneHotEncoder, StringIndexer

df = sqlContext.createDataFrame([
  (0, "a"),
  (1, "b"),
  (2, "c"),
  (3, "a"),
  (4, "a"),
  (5, "c")
], ["id", "category"])

stringIndexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
model = stringIndexer.fit(df)
indexed = model.transform(df)
encoder = OneHotEncoder(includeFirst=False, inputCol="categoryIndex", outputCol="categoryVec")
encoded = encoder.transform(indexed)
{% endhighlight %}
</div>
</div>

## VectorIndexer

`VectorIndexer` helps index categorical features in datasets of `Vector`s.
It can both automatically decide which features are categorical and convert original values to category indices.  Specifically, it does the following:

1. Take an input column of type [Vector](api/scala/index.html#org.apache.spark.mllib.linalg.Vector) and a parameter `maxCategories`.
2. Decide which features should be categorical based on the number of distinct values, where features with at most `maxCategories` are declared categorical.
3. Compute 0-based category indices for each categorical feature.
4. Index categorical features and transform original feature values to indices.

Indexing categorical features allows algorithms such as Decision Trees and Tree Ensembles to treat categorical features appropriately, improving performance.

Please refer to the [VectorIndexer API docs](api/scala/index.html#org.apache.spark.ml.feature.VectorIndexer) for more details.

In the example below, we read in a dataset of labeled points and then use `VectorIndexer` to decide which features should be treated as categorical.  We transform the categorical feature values to their indices.  This transformed data could then be passed to algorithms such as `DecisionTreeRegressor` that handle categorical features.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.mllib.util.MLUtils

val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()
val indexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexed")
  .setMaxCategories(10)
val indexerModel = indexer.fit(data)
val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
println(s"Chose ${categoricalFeatures.size} categorical features: " +
  categoricalFeatures.mkString(", "))

// Create new column "indexed" with categorical values transformed to indices
val indexedData = indexerModel.transform(data)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.DataFrame;

JavaRDD<LabeledPoint> rdd = MLUtils.loadLibSVMFile(sc.sc(),
  "data/mllib/sample_libsvm_data.txt").toJavaRDD();
DataFrame data = sqlContext.createDataFrame(rdd, LabeledPoint.class);
VectorIndexer indexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexed")
  .setMaxCategories(10);
VectorIndexerModel indexerModel = indexer.fit(data);
Map<Integer, Map<Double, Integer>> categoryMaps = indexerModel.javaCategoryMaps();
System.out.print("Chose " + categoryMaps.size() + "categorical features:");
for (Integer feature : categoryMaps.keySet()) {
  System.out.print(" " + feature);
}
System.out.println();

// Create new column "indexed" with categorical values transformed to indices
DataFrame indexedData = indexerModel.transform(data);
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.ml.feature import VectorIndexer
from pyspark.mllib.util import MLUtils

data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()
indexer = VectorIndexer(inputCol="features", outputCol="indexed", maxCategories=10)
indexerModel = indexer.fit(data)

# Create new column "indexed" with categorical values transformed to indices
indexedData = indexerModel.transform(data)
{% endhighlight %}
</div>
</div>


## Normalizer

`Normalizer` is a `Transformer` which transforms a dataset of `Vector` rows, normalizing each `Vector` to have unit norm.  It takes parameter `p`, which specifies the [p-norm](http://en.wikipedia.org/wiki/Norm_%28mathematics%29#p-norm) used for normalization.  ($p = 2$ by default.)  This normalization can help standardize your input data and improve the behavior of learning algorithms.

The following example demonstrates how to load a dataset in libsvm format and then normalize each row to have unit $L^2$ norm and unit $L^\infty$ norm.

<div class="codetabs">
<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.mllib.util.MLUtils

val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
val dataFrame = sqlContext.createDataFrame(data)

// Normalize each Vector using $L^1$ norm.
val normalizer = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normFeatures")
  .setP(1.0)
val l1NormData = normalizer.transform(dataFrame)

// Normalize each Vector using $L^\infty$ norm.
val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
{% endhighlight %}
</div>

<div data-lang="java">
{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.DataFrame;

JavaRDD<LabeledPoint> data =
  MLUtils.loadLibSVMFile(jsc.sc(), "data/mllib/sample_libsvm_data.txt").toJavaRDD();
DataFrame dataFrame = jsql.createDataFrame(data, LabeledPoint.class);

// Normalize each Vector using $L^1$ norm.
Normalizer normalizer = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normFeatures")
  .setP(1.0);
DataFrame l1NormData = normalizer.transform(dataFrame);

// Normalize each Vector using $L^\infty$ norm.
DataFrame lInfNormData =
  normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
{% endhighlight %}
</div>

<div data-lang="python">
{% highlight python %}
from pyspark.mllib.util import MLUtils
from pyspark.ml.feature import Normalizer

data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
dataFrame = sqlContext.createDataFrame(data)

# Normalize each Vector using $L^1$ norm.
normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=1.0)
l1NormData = normalizer.transform(dataFrame)

# Normalize each Vector using $L^\infty$ norm.
lInfNormData = normalizer.transform(dataFrame, {normalizer.p: float("inf")})
{% endhighlight %}
</div>
</div>


## StandardScaler

`StandardScaler` transforms a dataset of `Vector` rows, normalizing each feature to have unit standard deviation and/or zero mean.  It takes parameters:

* `withStd`: True by default. Scales the data to unit standard deviation.
* `withMean`: False by default. Centers the data with mean before scaling. It will build a dense output, so this does not work on sparse input and will raise an exception.

`StandardScaler` is a `Model` which can be `fit` on a dataset to produce a `StandardScalerModel`; this amounts to computing summary statistics.  The model can then transform a `Vector` column in a dataset to have unit standard deviation and/or zero mean features.

Note that if the standard deviation of a feature is zero, it will return default `0.0` value in the `Vector` for that feature.

More details can be found in the API docs for
[StandardScaler](api/scala/index.html#org.apache.spark.ml.feature.StandardScaler) and
[StandardScalerModel](api/scala/index.html#org.apache.spark.ml.feature.StandardScalerModel).

The following example demonstrates how to load a dataset in libsvm format and then normalize each feature to have unit standard deviation.

<div class="codetabs">
<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.util.MLUtils

val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
val dataFrame = sqlContext.createDataFrame(data)
val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setWithStd(true)
  .setWithMean(false)

// Compute summary statistics by fitting the StandardScaler
val scalerModel = scaler.fit(dataFrame)

// Normalize each feature to have unit standard deviation.
val scaledData = scalerModel.transform(dataFrame)
{% endhighlight %}
</div>

<div data-lang="java">
{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.DataFrame;

JavaRDD<LabeledPoint> data =
  MLUtils.loadLibSVMFile(jsc.sc(), "data/mllib/sample_libsvm_data.txt").toJavaRDD();
DataFrame dataFrame = jsql.createDataFrame(data, LabeledPoint.class);
StandardScaler scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setWithStd(true)
  .setWithMean(false);

// Compute summary statistics by fitting the StandardScaler
StandardScalerModel scalerModel = scaler.fit(dataFrame);

// Normalize each feature to have unit standard deviation.
DataFrame scaledData = scalerModel.transform(dataFrame);
{% endhighlight %}
</div>

<div data-lang="python">
{% highlight python %}
from pyspark.mllib.util import MLUtils
from pyspark.ml.feature import StandardScaler

data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
dataFrame = sqlContext.createDataFrame(data)
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                        withStd=True, withMean=False)

# Compute summary statistics by fitting the StandardScaler
scalerModel = scaler.fit(dataFrame)

# Normalize each feature to have unit standard deviation.
scaledData = scalerModel.transform(dataFrame)
{% endhighlight %}
</div>
</div>

## MinMaxScaler

`MinMaxScaler` transforms a dataset of `Vector` rows, rescaling each feature to a specific range (often [0, 1]).  It takes parameters:

* `min`: 0.0 by default. Lower bound after transformation, shared by all features.
* `max`: 1.0 by default. Upper bound after transformation, shared by all features.

`MinMaxScaler` computes summary statistics on a data set and produces a `MinMaxScalerModel`. The model can then transform each feature individually such that it is in the given range.

The rescaled value for a feature E is calculated as,
`\begin{equation}
  Rescaled(e_i) = \frac{e_i - E_{min}}{E_{max} - E_{min}} * (max - min) + min
\end{equation}`
For the case `E_{max} == E_{min}`, `Rescaled(e_i) = 0.5 * (max + min)`

Note that since zero values will probably be transformed to non-zero values, output of the transformer will be DenseVector even for sparse input.

The following example demonstrates how to load a dataset in libsvm format and then rescale each feature to [0, 1].

<div class="codetabs">
<div data-lang="scala" markdown="1">
More details can be found in the API docs for
[MinMaxScaler](api/scala/index.html#org.apache.spark.ml.feature.MinMaxScaler) and
[MinMaxScalerModel](api/scala/index.html#org.apache.spark.ml.feature.MinMaxScalerModel).
{% highlight scala %}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.util.MLUtils

val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
val dataFrame = sqlContext.createDataFrame(data)
val scaler = new MinMaxScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")

// Compute summary statistics and generate MinMaxScalerModel
val scalerModel = scaler.fit(dataFrame)

// rescale each feature to range [min, max].
val scaledData = scalerModel.transform(dataFrame)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
More details can be found in the API docs for
[MinMaxScaler](api/java/org/apache/spark/ml/feature/MinMaxScaler.html) and
[MinMaxScalerModel](api/java/org/apache/spark/ml/feature/MinMaxScalerModel.html).
{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.DataFrame;

JavaRDD<LabeledPoint> data =
  MLUtils.loadLibSVMFile(jsc.sc(), "data/mllib/sample_libsvm_data.txt").toJavaRDD();
DataFrame dataFrame = jsql.createDataFrame(data, LabeledPoint.class);
MinMaxScaler scaler = new MinMaxScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures");

// Compute summary statistics and generate MinMaxScalerModel
MinMaxScalerModel scalerModel = scaler.fit(dataFrame);

// rescale each feature to range [min, max].
DataFrame scaledData = scalerModel.transform(dataFrame);
{% endhighlight %}
</div>
</div>

## Bucketizer

`Bucketizer` transforms a column of continuous features to a column of feature buckets, where the buckets are specified by users. It takes a parameter:

* `splits`: Parameter for mapping continuous features into buckets. With n+1 splits, there are n buckets. A bucket defined by splits x,y holds values in the range [x,y) except the last bucket, which also includes y. Splits should be strictly increasing. Values at -inf, inf must be explicitly provided to cover all Double values; Otherwise, values outside the splits specified will be treated as errors. Two examples of `splits` are `Array(Double.NegativeInfinity, 0.0, 1.0, Double.PositiveInfinity)` and `Array(0.0, 1.0, 2.0)`.

Note that if you have no idea of the upper bound and lower bound of the targeted column, you would better add the `Double.NegativeInfinity` and `Double.PositiveInfinity` as the bounds of your splits to prevent a potenial out of Bucketizer bounds exception.

Note also that the splits that you provided have to be in strictly increasing order, i.e. `s0 < s1 < s2 < ... < sn`.

More details can be found in the API docs for [Bucketizer](api/scala/index.html#org.apache.spark.ml.feature.Bucketizer).

The following example demonstrates how to bucketize a column of `Double`s into another index-wised column.

<div class="codetabs">
<div data-lang="scala">
{% highlight scala %}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.DataFrame

val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

val data = Array(-0.5, -0.3, 0.0, 0.2)
val dataFrame = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

val bucketizer = new Bucketizer()
  .setInputCol("features")
  .setOutputCol("bucketedFeatures")
  .setSplits(splits)

// Transform original data into its bucket index.
val bucketedData = bucketizer.transform(dataFrame)
{% endhighlight %}
</div>

<div data-lang="java">
{% highlight java %}
import java.util.Arrays;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

double[] splits = {Double.NEGATIVE_INFINITY, -0.5, 0.0, 0.5, Double.POSITIVE_INFINITY};

JavaRDD<Row> data = jsc.parallelize(Arrays.asList(
  RowFactory.create(-0.5),
  RowFactory.create(-0.3),
  RowFactory.create(0.0),
  RowFactory.create(0.2)
));
StructType schema = new StructType(new StructField[] {
  new StructField("features", DataTypes.DoubleType, false, Metadata.empty())
});
DataFrame dataFrame = jsql.createDataFrame(data, schema);

Bucketizer bucketizer = new Bucketizer()
  .setInputCol("features")
  .setOutputCol("bucketedFeatures")
  .setSplits(splits);

// Transform original data into its bucket index.
DataFrame bucketedData = bucketizer.transform(dataFrame);
{% endhighlight %}
</div>

<div data-lang="python">
{% highlight python %}
from pyspark.ml.feature import Bucketizer

splits = [-float("inf"), -0.5, 0.0, 0.5, float("inf")]

data = [(-0.5,), (-0.3,), (0.0,), (0.2,)]
dataFrame = sqlContext.createDataFrame(data, ["features"])

bucketizer = Bucketizer(splits=splits, inputCol="features", outputCol="bucketedFeatures")

# Transform original data into its bucket index.
bucketedData = bucketizer.transform(dataFrame)
{% endhighlight %}
</div>
</div>

## ElementwiseProduct

ElementwiseProduct multiplies each input vector by a provided "weight" vector, using element-wise multiplication. In other words, it scales each column of the dataset by a scalar multiplier.  This represents the [Hadamard product](https://en.wikipedia.org/wiki/Hadamard_product_%28matrices%29) between the input vector, `v` and transforming vector, `w`, to yield a result vector.

`\[ \begin{pmatrix}
v_1 \\
\vdots \\
v_N
\end{pmatrix} \circ \begin{pmatrix}
                    w_1 \\
                    \vdots \\
                    w_N
                    \end{pmatrix}
= \begin{pmatrix}
  v_1 w_1 \\
  \vdots \\
  v_N w_N
  \end{pmatrix}
\]`

[`ElementwiseProduct`](api/scala/index.html#org.apache.spark.ml.feature.ElementwiseProduct) takes the following parameter:

* `scalingVec`: the transforming vector.

This example below demonstrates how to transform vectors using a transforming vector value.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.Vectors

// Create some vector data; also works for sparse vectors
val dataFrame = sqlContext.createDataFrame(Seq(
  ("a", Vectors.dense(1.0, 2.0, 3.0)),
  ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
val transformer = new ElementwiseProduct()
  .setScalingVec(transformingVector)
  .setInputCol("vector")
  .setOutputCol("transformedVector")

// Batch transform the vectors to create new column:
transformer.transform(dataFrame).show()

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.ElementwiseProduct;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

// Create some vector data; also works for sparse vectors
JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
  RowFactory.create("a", Vectors.dense(1.0, 2.0, 3.0)),
  RowFactory.create("b", Vectors.dense(4.0, 5.0, 6.0))
));
List<StructField> fields = new ArrayList<StructField>(2);
fields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
fields.add(DataTypes.createStructField("vector", DataTypes.StringType, false));
StructType schema = DataTypes.createStructType(fields);
DataFrame dataFrame = sqlContext.createDataFrame(jrdd, schema);
Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);
ElementwiseProduct transformer = new ElementwiseProduct()
  .setScalingVec(transformingVector)
  .setInputCol("vector")
  .setOutputCol("transformedVector");
// Batch transform the vectors to create new column:
transformer.transform(dataFrame).show();

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.ml.feature import ElementwiseProduct
from pyspark.mllib.linalg import Vectors

data = [(Vectors.dense([1.0, 2.0, 3.0]),), (Vectors.dense([4.0, 5.0, 6.0]),)]
df = sqlContext.createDataFrame(data, ["vector"])
transformer = ElementwiseProduct(scalingVec=Vectors.dense([0.0, 1.0, 2.0]), 
                                 inputCol="vector", outputCol="transformedVector")
transformer.transform(df).show()

{% endhighlight %}
</div>

</div>

## VectorAssembler

`VectorAssembler` is a transformer that combines a given list of columns into a single vector
column.
It is useful for combining raw features and features generated by different feature transformers
into a single feature vector, in order to train ML models like logistic regression and decision
trees.
`VectorAssembler` accepts the following input column types: all numeric types, boolean type,
and vector type.
In each row, the values of the input columns will be concatenated into a vector in the specified
order.

**Examples**

Assume that we have a DataFrame with the columns `id`, `hour`, `mobile`, `userFeatures`,
and `clicked`:

~~~
 id | hour | mobile | userFeatures     | clicked
----|------|--------|------------------|---------
 0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0
~~~

`userFeatures` is a vector column that contains three user features.
We want to combine `hour`, `mobile`, and `userFeatures` into a single feature vector
called `features` and use it to predict `clicked` or not.
If we set `VectorAssembler`'s input columns to `hour`, `mobile`, and `userFeatures` and
output column to `features`, after transformation we should get the following DataFrame:

~~~
 id | hour | mobile | userFeatures     | clicked | features
----|------|--------|------------------|---------|-----------------------------
 0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0     | [18.0, 1.0, 0.0, 10.0, 0.5]
~~~

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`VectorAssembler`](api/scala/index.html#org.apache.spark.ml.feature.VectorAssembler) takes an array
of input column names and an output column name.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler

val dataset = sqlContext.createDataFrame(
  Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
).toDF("id", "hour", "mobile", "userFeatures", "clicked")
val assembler = new VectorAssembler()
  .setInputCols(Array("hour", "mobile", "userFeatures"))
  .setOutputCol("features")
val output = assembler.transform(dataset)
println(output.select("features", "clicked").first())
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[`VectorAssembler`](api/java/org/apache/spark/ml/feature/VectorAssembler.html) takes an array
of input column names and an output column name.

{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.types.DataTypes.*;

StructType schema = createStructType(new StructField[] {
  createStructField("id", IntegerType, false),
  createStructField("hour", IntegerType, false),
  createStructField("mobile", DoubleType, false),
  createStructField("userFeatures", new VectorUDT(), false),
  createStructField("clicked", DoubleType, false)
});
Row row = RowFactory.create(0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0);
JavaRDD<Row> rdd = jsc.parallelize(Arrays.asList(row));
DataFrame dataset = sqlContext.createDataFrame(rdd, schema);

VectorAssembler assembler = new VectorAssembler()
  .setInputCols(new String[] {"hour", "mobile", "userFeatures"})
  .setOutputCol("features");

DataFrame output = assembler.transform(dataset);
System.out.println(output.select("features", "clicked").first());
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

[`VectorAssembler`](api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler) takes a list
of input column names and an output column name.

{% highlight python %}
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

dataset = sqlContext.createDataFrame(
    [(0, 18, 1.0, Vectors.dense([0.0, 10.0, 0.5]), 1.0)],
    ["id", "hour", "mobile", "userFeatures", "clicked"])
assembler = VectorAssembler(
    inputCols=["hour", "mobile", "userFeatures"],
    outputCol="features")
output = assembler.transform(dataset)
print(output.select("features", "clicked").first())
{% endhighlight %}
</div>
</div>

# Feature Selectors

## VectorSlicer

`VectorSlicer` is a transformer that takes a feature vector and outputs a new feature vector with a
sub-array of the original features. It is useful for extracting features from a vector column.

`VectorSlicer` accepts a vector column with a specified indices, then outputs a new vector column
whose values are selected via those indices. There are two types of indices, 

 1. Integer indices that represents the indices into the vector, `setIndices()`;

 2. String indices that represents the names of features into the vector, `setNames()`. 
 *This requires the vector column to have an `AttributeGroup` since the implementation matches on
 the name field of an `Attribute`.*

Specification by integer and string are both acceptable. Moreover, you can use integer index and 
string name simultaneously. At least one feature must be selected. Duplicate features are not
allowed, so there can be no overlap between selected indices and names. Note that if names of
features are selected, an exception will be threw out when encountering with empty input attributes.

The output vector will order features with the selected indices first (in the order given),
followed by the selected names (in the order given).

**Examples**

Suppose that we have a DataFrame with the column `userFeatures`:

~~~
 userFeatures     
------------------
 [0.0, 10.0, 0.5] 
~~~

`userFeatures` is a vector column that contains three user features. Assuming that the first column
of `userFeatures` are all zeros, so we want to remove it and only the last two columns are selected.
The `VectorSlicer` selects the last two elements with `setIndices(1, 2)` then produces a new vector
column named `features`:

~~~
 userFeatures     | features
------------------|-----------------------------
 [0.0, 10.0, 0.5] | [10.0, 0.5]
~~~

Suppose also that we have a potential input attributes for the `userFeatures`, i.e. 
`["f1", "f2", "f3"]`, then we can use `setNames("f2", "f3")` to select them.

~~~
 userFeatures     | features
------------------|-----------------------------
 [0.0, 10.0, 0.5] | [10.0, 0.5]
 ["f1", "f2", "f3"] | ["f2", "f3"]
~~~

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`VectorSlicer`](api/scala/index.html#org.apache.spark.ml.feature.VectorSlicer) takes an input
column name with specified indices or names and an output column name.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

val data = Array(
  Vectors.sparse(3, Seq((0, -2.0), (1, 2.3))),
  Vectors.dense(-2.0, 2.3, 0.0)
)

val defaultAttr = NumericAttribute.defaultAttr
val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

val dataRDD = sc.parallelize(data).map(Row.apply)
val dataset = sqlContext.createDataFrame(dataRDD, StructType(attrGroup.toStructField()))

val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

slicer.setIndices(1).setNames("f3")
// or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

val output = slicer.transform(dataset)
println(output.select("userFeatures", "features").first())
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[`VectorSlicer`](api/java/org/apache/spark/ml/feature/VectorSlicer.html) takes an input column name
with specified indices or names and an output column name.

{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.types.DataTypes.*;

Attribute[] attrs = new Attribute[]{
  NumericAttribute.defaultAttr().withName("f1"),
  NumericAttribute.defaultAttr().withName("f2"),
  NumericAttribute.defaultAttr().withName("f3")
};
AttributeGroup group = new AttributeGroup("userFeatures", attrs);

JavaRDD<Row> jrdd = jsc.parallelize(Lists.newArrayList(
  RowFactory.create(Vectors.sparse(3, new int[]{0, 1}, new double[]{-2.0, 2.3})),
  RowFactory.create(Vectors.dense(-2.0, 2.3, 0.0))
));

DataFrame dataset = jsql.createDataFrame(jrdd, (new StructType()).add(group.toStructField()));

VectorSlicer vectorSlicer = new VectorSlicer()
  .setInputCol("userFeatures").setOutputCol("features");

vectorSlicer.setIndices(new int[]{1}).setNames(new String[]{"f3"});
// or slicer.setIndices(new int[]{1, 2}), or slicer.setNames(new String[]{"f2", "f3"})

DataFrame output = vectorSlicer.transform(dataset);

System.out.println(output.select("userFeatures", "features").first());
{% endhighlight %}
</div>
</div>

## RFormula

`RFormula` selects columns specified by an [R model formula](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/formula.html). It produces a vector column of features and a double column of labels. Like when formulas are used in R for linear regression, string input columns will be one-hot encoded, and numeric columns will be cast to doubles. If not already present in the DataFrame, the output label column will be created from the specified response variable in the formula.

**Examples**

Assume that we have a DataFrame with the columns `id`, `country`, `hour`, and `clicked`:

~~~
id | country | hour | clicked
---|---------|------|---------
 7 | "US"    | 18   | 1.0
 8 | "CA"    | 12   | 0.0
 9 | "NZ"    | 15   | 0.0
~~~

If we use `RFormula` with a formula string of `clicked ~ country + hour`, which indicates that we want to
predict `clicked` based on `country` and `hour`, after transformation we should get the following DataFrame:

~~~
id | country | hour | clicked | features         | label
---|---------|------|---------|------------------|-------
 7 | "US"    | 18   | 1.0     | [0.0, 0.0, 18.0] | 1.0
 8 | "CA"    | 12   | 0.0     | [0.0, 1.0, 12.0] | 0.0
 9 | "NZ"    | 15   | 0.0     | [1.0, 0.0, 15.0] | 0.0
~~~

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`RFormula`](api/scala/index.html#org.apache.spark.ml.feature.RFormula) takes an R formula string, and optional parameters for the names of its output columns.

{% highlight scala %}
import org.apache.spark.ml.feature.RFormula

val dataset = sqlContext.createDataFrame(Seq(
  (7, "US", 18, 1.0),
  (8, "CA", 12, 0.0),
  (9, "NZ", 15, 0.0)
)).toDF("id", "country", "hour", "clicked")
val formula = new RFormula()
  .setFormula("clicked ~ country + hour")
  .setFeaturesCol("features")
  .setLabelCol("label")
val output = formula.fit(dataset).transform(dataset)
output.select("features", "label").show()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[`RFormula`](api/java/org/apache/spark/ml/feature/RFormula.html) takes an R formula string, and optional parameters for the names of its output columns.

{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.types.DataTypes.*;

StructType schema = createStructType(new StructField[] {
  createStructField("id", IntegerType, false),
  createStructField("country", StringType, false),
  createStructField("hour", IntegerType, false),
  createStructField("clicked", DoubleType, false)
});
JavaRDD<Row> rdd = jsc.parallelize(Arrays.asList(
  RowFactory.create(7, "US", 18, 1.0),
  RowFactory.create(8, "CA", 12, 0.0),
  RowFactory.create(9, "NZ", 15, 0.0)
));
DataFrame dataset = sqlContext.createDataFrame(rdd, schema);

RFormula formula = new RFormula()
  .setFormula("clicked ~ country + hour")
  .setFeaturesCol("features")
  .setLabelCol("label");

DataFrame output = formula.fit(dataset).transform(dataset);
output.select("features", "label").show();
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

[`RFormula`](api/python/pyspark.ml.html#pyspark.ml.feature.RFormula) takes an R formula string, and optional parameters for the names of its output columns.

{% highlight python %}
from pyspark.ml.feature import RFormula

dataset = sqlContext.createDataFrame(
    [(7, "US", 18, 1.0),
     (8, "CA", 12, 0.0),
     (9, "NZ", 15, 0.0)],
    ["id", "country", "hour", "clicked"])
formula = RFormula(
    formula="clicked ~ country + hour",
    featuresCol="features",
    labelCol="label")
output = formula.fit(dataset).transform(dataset)
output.select("features", "label").show()
{% endhighlight %}
</div>
</div>

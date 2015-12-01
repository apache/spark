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

In the following code segment, we start with a set of sentences.  We split each sentence into words using `Tokenizer`.  For each sentence (bag of words), we use `HashingTF` to hash the sentence into a feature vector.  We use `IDF` to rescale the feature vectors; this generally improves performance when using text as features.  Our feature vectors could then be passed to a learning algorithm.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [HashingTF Scala docs](api/scala/index.html#org.apache.spark.ml.feature.HashingTF) and
the [IDF Scala docs](api/scala/index.html#org.apache.spark.ml.feature.IDF) for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/TfIdfExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [HashingTF Java docs](api/java/org/apache/spark/ml/feature/HashingTF.html) and the
[IDF Java docs](api/java/org/apache/spark/ml/feature/IDF.html) for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaTfIdfExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [HashingTF Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.HashingTF) and
the [IDF Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.IDF) for more details on the API.

{% include_example python/ml/tf_idf_example.py %}
</div>
</div>

## Word2Vec

`Word2Vec` is an `Estimator` which takes sequences of words representing documents and trains a
`Word2VecModel`. The model maps each word to a unique fixed-size vector. The `Word2VecModel`
transforms each document into a vector using the average of all words in the document; this vector
can then be used for as features for prediction, document similarity calculations, etc.
Please refer to the [MLlib user guide on Word2Vec](mllib-feature-extraction.html#Word2Vec) for more
details.

In the following code segment, we start with a set of documents, each of which is represented as a sequence of words. For each document, we transform it into a feature vector. This feature vector could then be passed to a learning algorithm.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Word2Vec Scala docs](api/scala/index.html#org.apache.spark.ml.feature.Word2Vec)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/Word2VecExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Word2Vec Java docs](api/java/org/apache/spark/ml/feature/Word2Vec.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaWord2VecExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Word2Vec Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.Word2Vec)
for more details on the API.

{% include_example python/ml/word2vec_example.py %}
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

Refer to the [CountVectorizer Scala docs](api/scala/index.html#org.apache.spark.ml.feature.CountVectorizer)
and the [CountVectorizerModel Scala docs](api/scala/index.html#org.apache.spark.ml.feature.CountVectorizerModel)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/CountVectorizerExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [CountVectorizer Java docs](api/java/org/apache/spark/ml/feature/CountVectorizer.html)
and the [CountVectorizerModel Java docs](api/java/org/apache/spark/ml/feature/CountVectorizerModel.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaCountVectorizerExample.java %}
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

Refer to the [Tokenizer Scala docs](api/scala/index.html#org.apache.spark.ml.feature.Tokenizer)
and the [RegexTokenizer Scala docs](api/scala/index.html#org.apache.spark.ml.feature.Tokenizer)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/TokenizerExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Tokenizer Java docs](api/java/org/apache/spark/ml/feature/Tokenizer.html)
and the [RegexTokenizer Java docs](api/java/org/apache/spark/ml/feature/RegexTokenizer.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaTokenizerExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Tokenizer Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.Tokenizer) and
the the [RegexTokenizer Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.RegexTokenizer)
for more details on the API.

{% include_example python/ml/tokenizer_example.py %}
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
`StopWordsRemover` instance. A boolean parameter `caseSensitive` indicates
if the matches should be case sensitive (false by default).

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

Refer to the [StopWordsRemover Scala docs](api/scala/index.html#org.apache.spark.ml.feature.StopWordsRemover)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/StopWordsRemoverExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [StopWordsRemover Java docs](api/java/org/apache/spark/ml/feature/StopWordsRemover.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaStopWordsRemoverExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [StopWordsRemover Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.StopWordsRemover)
for more details on the API.

{% include_example python/ml/stopwords_remover_example.py %}
</div>
</div>

## $n$-gram

An [n-gram](https://en.wikipedia.org/wiki/N-gram) is a sequence of $n$ tokens (typically words) for some integer $n$. The `NGram` class can be used to transform input features into $n$-grams.

`NGram` takes as input a sequence of strings (e.g. the output of a [Tokenizer](ml-features.html#tokenizer)).  The parameter `n` is used to determine the number of terms in each $n$-gram. The output will consist of a sequence of $n$-grams where each $n$-gram is represented by a space-delimited string of $n$ consecutive words.  If the input sequence contains fewer than `n` strings, no output is produced.

<div class="codetabs">

<div data-lang="scala" markdown="1">

Refer to the [NGram Scala docs](api/scala/index.html#org.apache.spark.ml.feature.NGram)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/NGramExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [NGram Java docs](api/java/org/apache/spark/ml/feature/NGram.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaNGramExample.java %}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

Refer to the [NGram Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.NGram)
for more details on the API.

{% include_example python/ml/n_gram_example.py %}
</div>
</div>


## Binarizer

Binarization is the process of thresholding numerical features to binary (0/1) features.

`Binarizer` takes the common parameters `inputCol` and `outputCol`, as well as the `threshold` for binarization. Feature values greater than the threshold are binarized to 1.0; values equal to or less than the threshold are binarized to 0.0.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [Binarizer Scala docs](api/scala/index.html#org.apache.spark.ml.feature.Binarizer)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/BinarizerExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [Binarizer Java docs](api/java/org/apache/spark/ml/feature/Binarizer.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaBinarizerExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [Binarizer Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.Binarizer)
for more details on the API.

{% include_example python/ml/binarizer_example.py %}
</div>
</div>

## PCA

[PCA](http://en.wikipedia.org/wiki/Principal_component_analysis) is a statistical procedure that uses an orthogonal transformation to convert a set of observations of possibly correlated variables into a set of values of linearly uncorrelated variables called principal components. A [PCA](api/scala/index.html#org.apache.spark.ml.feature.PCA) class trains a model to project vectors to a low-dimensional space using PCA. The example below shows how to project 5-dimensional feature vectors into 3-dimensional principal components.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [PCA Scala docs](api/scala/index.html#org.apache.spark.ml.feature.PCA)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/PCAExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [PCA Java docs](api/java/org/apache/spark/ml/feature/PCA.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaPCAExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [PCA Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.PCA)
for more details on the API.

{% include_example python/ml/pca_example.py %}
</div>
</div>

## PolynomialExpansion

[Polynomial expansion](http://en.wikipedia.org/wiki/Polynomial_expansion) is the process of expanding your features into a polynomial space, which is formulated by an n-degree combination of original dimensions. A [PolynomialExpansion](api/scala/index.html#org.apache.spark.ml.feature.PolynomialExpansion) class provides this functionality.  The example below shows how to expand your features into a 3-degree polynomial space.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [PolynomialExpansion Scala docs](api/scala/index.html#org.apache.spark.ml.feature.PolynomialExpansion)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/PolynomialExpansionExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [PolynomialExpansion Java docs](api/java/org/apache/spark/ml/feature/PolynomialExpansion.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaPolynomialExpansionExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [PolynomialExpansion Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.PolynomialExpansion)
for more details on the API.

{% include_example python/ml/polynomial_expansion_example.py %}
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

Refer to the [DCT Scala docs](api/scala/index.html#org.apache.spark.ml.feature.DCT)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/DCTExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [DCT Java docs](api/java/org/apache/spark/ml/feature/DCT.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaDCTExample.java %}}
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

Refer to the [StringIndexer Scala docs](api/scala/index.html#org.apache.spark.ml.feature.StringIndexer)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/StringIndexerExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [StringIndexer Java docs](api/java/org/apache/spark/ml/feature/StringIndexer.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaStringIndexerExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [StringIndexer Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
for more details on the API.

{% include_example python/ml/string_indexer_example.py %}
</div>
</div>

## OneHotEncoder

[One-hot encoding](http://en.wikipedia.org/wiki/One-hot) maps a column of label indices to a column of binary vectors, with at most a single one-value. This encoding allows algorithms which expect continuous features, such as Logistic Regression, to use categorical features 

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [OneHotEncoder Scala docs](api/scala/index.html#org.apache.spark.ml.feature.OneHotEncoder)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/OneHotEncoderExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [OneHotEncoder Java docs](api/java/org/apache/spark/ml/feature/OneHotEncoder.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaOneHotEncoderExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [OneHotEncoder Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.OneHotEncoder)
for more details on the API.

{% include_example python/ml/onehot_encoder_example.py %}
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

In the example below, we read in a dataset of labeled points and then use `VectorIndexer` to decide which features should be treated as categorical.  We transform the categorical feature values to their indices.  This transformed data could then be passed to algorithms such as `DecisionTreeRegressor` that handle categorical features.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [VectorIndexer Scala docs](api/scala/index.html#org.apache.spark.ml.feature.VectorIndexer)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/VectorIndexerExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [VectorIndexer Java docs](api/java/org/apache/spark/ml/feature/VectorIndexer.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaVectorIndexerExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [VectorIndexer Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.VectorIndexer)
for more details on the API.

{% include_example python/ml/vector_indexer_example.py %}
</div>
</div>


## Normalizer

`Normalizer` is a `Transformer` which transforms a dataset of `Vector` rows, normalizing each `Vector` to have unit norm.  It takes parameter `p`, which specifies the [p-norm](http://en.wikipedia.org/wiki/Norm_%28mathematics%29#p-norm) used for normalization.  ($p = 2$ by default.)  This normalization can help standardize your input data and improve the behavior of learning algorithms.

The following example demonstrates how to load a dataset in libsvm format and then normalize each row to have unit $L^2$ norm and unit $L^\infty$ norm.

<div class="codetabs">
<div data-lang="scala">

Refer to the [Normalizer Scala docs](api/scala/index.html#org.apache.spark.ml.feature.Normalizer)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/NormalizerExample.scala %}
</div>

<div data-lang="java">

Refer to the [Normalizer Java docs](api/java/org/apache/spark/ml/feature/Normalizer.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaNormalizerExample.java %}
</div>

<div data-lang="python">

Refer to the [Normalizer Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.Normalizer)
for more details on the API.

{% include_example python/ml/normalizer_example.py %}
</div>
</div>


## StandardScaler

`StandardScaler` transforms a dataset of `Vector` rows, normalizing each feature to have unit standard deviation and/or zero mean.  It takes parameters:

* `withStd`: True by default. Scales the data to unit standard deviation.
* `withMean`: False by default. Centers the data with mean before scaling. It will build a dense output, so this does not work on sparse input and will raise an exception.

`StandardScaler` is a `Model` which can be `fit` on a dataset to produce a `StandardScalerModel`; this amounts to computing summary statistics.  The model can then transform a `Vector` column in a dataset to have unit standard deviation and/or zero mean features.

Note that if the standard deviation of a feature is zero, it will return default `0.0` value in the `Vector` for that feature.

The following example demonstrates how to load a dataset in libsvm format and then normalize each feature to have unit standard deviation.

<div class="codetabs">
<div data-lang="scala">

Refer to the [StandardScaler Scala docs](api/scala/index.html#org.apache.spark.ml.feature.StandardScaler)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/StandardScalerExample.scala %}
</div>

<div data-lang="java">

Refer to the [StandardScaler Java docs](api/java/org/apache/spark/ml/feature/StandardScaler.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaStandardScalerExample.java %}
</div>

<div data-lang="python">

Refer to the [StandardScaler Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.StandardScaler)
for more details on the API.

{% include_example python/ml/standard_scaler_example.py %}
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

Refer to the [MinMaxScaler Scala docs](api/scala/index.html#org.apache.spark.ml.feature.MinMaxScaler)
and the [MinMaxScalerModel Scala docs](api/scala/index.html#org.apache.spark.ml.feature.MinMaxScalerModel)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/MinMaxScalerExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [MinMaxScaler Java docs](api/java/org/apache/spark/ml/feature/MinMaxScaler.html)
and the [MinMaxScalerModel Java docs](api/java/org/apache/spark/ml/feature/MinMaxScalerModel.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaMinMaxScalerExample.java %}
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

Refer to the [Bucketizer Scala docs](api/scala/index.html#org.apache.spark.ml.feature.Bucketizer)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/BucketizerExample.scala %}
</div>

<div data-lang="java">

Refer to the [Bucketizer Java docs](api/java/org/apache/spark/ml/feature/Bucketizer.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaBucketizerExample.java %}
</div>

<div data-lang="python">

Refer to the [Bucketizer Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.Bucketizer)
for more details on the API.

{% include_example python/ml/bucketizer_example.py %}
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

This example below demonstrates how to transform vectors using a transforming vector value.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [ElementwiseProduct Scala docs](api/scala/index.html#org.apache.spark.ml.feature.ElementwiseProduct)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/ElementwiseProductExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [ElementwiseProduct Java docs](api/java/org/apache/spark/ml/feature/ElementwiseProduct.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaElementwiseProductExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [ElementwiseProduct Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.ElementwiseProduct)
for more details on the API.

{% include_example python/ml/elementwise_product_example.py %}
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

Refer to the [VectorAssembler Scala docs](api/scala/index.html#org.apache.spark.ml.feature.VectorAssembler)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/VectorAssemblerExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [VectorAssembler Java docs](api/java/org/apache/spark/ml/feature/VectorAssembler.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaVectorAssemblerExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [VectorAssembler Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler)
for more details on the API.

{% include_example python/ml/vector_assembler_example.py %}
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

Refer to the [VectorSlicer Scala docs](api/scala/index.html#org.apache.spark.ml.feature.VectorSlicer)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/VectorSlicerExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [VectorSlicer Java docs](api/java/org/apache/spark/ml/feature/VectorSlicer.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaVectorSlicerExample.java %}
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

Refer to the [RFormula Scala docs](api/scala/index.html#org.apache.spark.ml.feature.RFormula)
for more details on the API.

{% include_example scala/org/apache/spark/examples/ml/RFormulaExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [RFormula Java docs](api/java/org/apache/spark/ml/feature/RFormula.html)
for more details on the API.

{% include_example java/org/apache/spark/examples/ml/JavaRFormulaExample.java %}
</div>

<div data-lang="python" markdown="1">

Refer to the [RFormula Python docs](api/python/pyspark.ml.html#pyspark.ml.feature.RFormula)
for more details on the API.

{% include_example python/ml/rformula_example.py %}
</div>
</div>

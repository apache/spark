---
layout: global
title: Feature Extraction and Transformation - RDD-based API
displayTitle: Feature Extraction and Transformation - RDD-based API
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}


## TF-IDF

**Note** We recommend using the DataFrame-based API, which is detailed in the [ML user guide on 
TF-IDF](ml-features.html#tf-idf).

[Term frequency-inverse document frequency (TF-IDF)](http://en.wikipedia.org/wiki/Tf%E2%80%93idf) is a feature 
vectorization method widely used in text mining to reflect the importance of a term to a document in the corpus.
Denote a term by `$t$`, a document by `$d$`, and the corpus by `$D$`.
Term frequency `$TF(t, d)$` is the number of times that term `$t$` appears in document `$d$`,
while document frequency `$DF(t, D)$` is the number of documents that contains term `$t$`.
If we only use term frequency to measure the importance, it is very easy to over-emphasize terms that
appear very often but carry little information about the document, e.g., "a", "the", and "of".
If a term appears very often across the corpus, it means it doesn't carry special information about
a particular document.
Inverse document frequency is a numerical measure of how much information a term provides:
`\[
IDF(t, D) = \log \frac{|D| + 1}{DF(t, D) + 1},
\]`
where `$|D|$` is the total number of documents in the corpus.
Since logarithm is used, if a term appears in all documents, its IDF value becomes 0.
Note that a smoothing term is applied to avoid dividing by zero for terms outside the corpus.
The TF-IDF measure is simply the product of TF and IDF:
`\[
TFIDF(t, d, D) = TF(t, d) \cdot IDF(t, D).
\]`
There are several variants on the definition of term frequency and document frequency.
In `spark.mllib`, we separate TF and IDF to make them flexible.

Our implementation of term frequency utilizes the
[hashing trick](http://en.wikipedia.org/wiki/Feature_hashing).
A raw feature is mapped into an index (term) by applying a hash function.
Then term frequencies are calculated based on the mapped indices.
This approach avoids the need to compute a global term-to-index map,
which can be expensive for a large corpus, but it suffers from potential hash collisions,
where different raw features may become the same term after hashing.
To reduce the chance of collision, we can increase the target feature dimension, i.e., 
the number of buckets of the hash table.
The default feature dimension is `$2^{20} = 1,048,576$`.

**Note:** `spark.mllib` doesn't provide tools for text segmentation.
We refer users to the [Stanford NLP Group](http://nlp.stanford.edu/) and 
[scalanlp/chalk](https://github.com/scalanlp/chalk).

<div class="codetabs">
<div data-lang="scala" markdown="1">

TF and IDF are implemented in [HashingTF](api/scala/org/apache/spark/mllib/feature/HashingTF.html)
and [IDF](api/scala/org/apache/spark/mllib/feature/IDF.html).
`HashingTF` takes an `RDD[Iterable[_]]` as the input.
Each record could be an iterable of strings or other types.

Refer to the [`HashingTF` Scala docs](api/scala/org/apache/spark/mllib/feature/HashingTF.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/TFIDFExample.scala %}
</div>
<div data-lang="python" markdown="1">

TF and IDF are implemented in [HashingTF](api/python/reference/api/pyspark.mllib.feature.HashingTF.html)
and [IDF](api/python/reference/api/pyspark.mllib.feature.IDF.html).
`HashingTF` takes an RDD of list as the input.
Each record could be an iterable of strings or other types.


Refer to the [`HashingTF` Python docs](api/python/reference/api/pyspark.mllib.feature.HashingTF.html) for details on the API.

{% include_example python/mllib/tf_idf_example.py %}
</div>
</div>

## Word2Vec 

[Word2Vec](https://code.google.com/p/word2vec/) computes distributed vector representation of words.
The main advantage of the distributed
representations is that similar words are close in the vector space, which makes generalization to 
novel patterns easier and model estimation more robust. Distributed vector representation is 
showed to be useful in many natural language processing applications such as named entity 
recognition, disambiguation, parsing, tagging and machine translation.

### Model

In our implementation of Word2Vec, we used skip-gram model. The training objective of skip-gram is
to learn word vector representations that are good at predicting its context in the same sentence. 
Mathematically, given a sequence of training words `$w_1, w_2, \dots, w_T$`, the objective of the
skip-gram model is to maximize the average log-likelihood 
`\[
\frac{1}{T} \sum_{t = 1}^{T}\sum_{j=-k}^{j=k} \log p(w_{t+j} | w_t)
\]`
where $k$ is the size of the training window.  

In the skip-gram model, every word $w$ is associated with two vectors $u_w$ and $v_w$ which are 
vector representations of $w$ as word and context respectively. The probability of correctly 
predicting word $w_i$ given word $w_j$ is determined by the softmax model, which is 
`\[
p(w_i | w_j ) = \frac{\exp(u_{w_i}^{\top}v_{w_j})}{\sum_{l=1}^{V} \exp(u_l^{\top}v_{w_j})}
\]`
where $V$ is the vocabulary size. 

The skip-gram model with softmax is expensive because the cost of computing $\log p(w_i | w_j)$
is proportional to $V$, which can be easily in order of millions. To speed up training of Word2Vec, 
we used hierarchical softmax, which reduced the complexity of computing of $\log p(w_i | w_j)$ to
$O(\log(V))$

### Example 

The example below demonstrates how to load a text file, parse it as an RDD of `Seq[String]`,
construct a `Word2Vec` instance and then fit a `Word2VecModel` with the input data. Finally,
we display the top 40 synonyms of the specified word. To run the example, first download
the [text8](http://mattmahoney.net/dc/text8.zip) data and extract it to your preferred directory.
Here we assume the extracted file is `text8` and in same directory as you run the spark shell.  

<div class="codetabs">
<div data-lang="scala" markdown="1">
Refer to the [`Word2Vec` Scala docs](api/scala/org/apache/spark/mllib/feature/Word2Vec.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/Word2VecExample.scala %}
</div>
<div data-lang="python" markdown="1">
Refer to the [`Word2Vec` Python docs](api/python/reference/api/pyspark.mllib.feature.Word2Vec.html) for more details on the API.

{% include_example python/mllib/word2vec_example.py %}
</div>
</div>

## StandardScaler

Standardizes features by scaling to unit variance and/or removing the mean using column summary
statistics on the samples in the training set. This is a very common pre-processing step.

For example, RBF kernel of Support Vector Machines or the L1 and L2 regularized linear models
typically work better when all features have unit variance and/or zero mean.

Standardization can improve the convergence rate during the optimization process, and also prevents
against features with very large variances exerting an overly large influence during model training.

### Model Fitting

[`StandardScaler`](api/scala/org/apache/spark/mllib/feature/StandardScaler.html) has the
following parameters in the constructor:

* `withMean` False by default. Centers the data with mean before scaling. It will build a dense
output, so take care when applying to sparse input.
* `withStd` True by default. Scales the data to unit standard deviation.

We provide a [`fit`](api/scala/org/apache/spark/mllib/feature/StandardScaler.html) method in
`StandardScaler` which can take an input of `RDD[Vector]`, learn the summary statistics, and then
return a model which can transform the input dataset into unit standard deviation and/or zero mean features
depending how we configure the `StandardScaler`.

This model implements [`VectorTransformer`](api/scala/org/apache/spark/mllib/feature/VectorTransformer.html)
which can apply the standardization on a `Vector` to produce a transformed `Vector` or on
an `RDD[Vector]` to produce a transformed `RDD[Vector]`.

Note that if the variance of a feature is zero, it will return default `0.0` value in the `Vector`
for that feature.

### Example

The example below demonstrates how to load a dataset in libsvm format, and standardize the features
so that the new features have unit standard deviation and/or zero mean.

<div class="codetabs">
<div data-lang="scala" markdown="1">
Refer to the [`StandardScaler` Scala docs](api/scala/org/apache/spark/mllib/feature/StandardScaler.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/StandardScalerExample.scala %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`StandardScaler` Python docs](api/python/reference/api/pyspark.mllib.feature.StandardScaler.html) for more details on the API.

{% include_example python/mllib/standard_scaler_example.py %}
</div>
</div>

## Normalizer

Normalizer scales individual samples to have unit $L^p$ norm. This is a common operation for text
classification or clustering. For example, the dot product of two $L^2$ normalized TF-IDF vectors
is the cosine similarity of the vectors.

[`Normalizer`](api/scala/org/apache/spark/mllib/feature/Normalizer.html) has the following
parameter in the constructor:

* `p` Normalization in $L^p$ space, $p = 2$ by default.

`Normalizer` implements [`VectorTransformer`](api/scala/org/apache/spark/mllib/feature/VectorTransformer.html)
which can apply the normalization on a `Vector` to produce a transformed `Vector` or on
an `RDD[Vector]` to produce a transformed `RDD[Vector]`.

Note that if the norm of the input is zero, it will return the input vector.

### Example

The example below demonstrates how to load a dataset in libsvm format, and normalizes the features
with $L^2$ norm, and $L^\infty$ norm.

<div class="codetabs">
<div data-lang="scala" markdown="1">
Refer to the [`Normalizer` Scala docs](api/scala/org/apache/spark/mllib/feature/Normalizer.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/NormalizerExample.scala %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`Normalizer` Python docs](api/python/reference/api/pyspark.mllib.feature.Normalizer.html) for more details on the API.

{% include_example python/mllib/normalizer_example.py %}
</div>
</div>

## ChiSqSelector

[Feature selection](http://en.wikipedia.org/wiki/Feature_selection) tries to identify relevant
features for use in model construction. It reduces the size of the feature space, which can improve
both speed and statistical learning behavior.

[`ChiSqSelector`](api/scala/org/apache/spark/mllib/feature/ChiSqSelector.html) implements
Chi-Squared feature selection. It operates on labeled data with categorical features. ChiSqSelector uses the
[Chi-Squared test of independence](https://en.wikipedia.org/wiki/Chi-squared_test) to decide which
features to choose. It supports five selection methods: `numTopFeatures`, `percentile`, `fpr`, `fdr`, `fwe`:

* `numTopFeatures` chooses a fixed number of top features according to a chi-squared test. This is akin to yielding the features with the most predictive power.
* `percentile` is similar to `numTopFeatures` but chooses a fraction of all features instead of a fixed number.
* `fpr` chooses all features whose p-values are below a threshold, thus controlling the false positive rate of selection.
* `fdr` uses the [Benjamini-Hochberg procedure](https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure) to choose all features whose false discovery rate is below a threshold.
* `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by 1/numFeatures, thus controlling the family-wise error rate of selection.

By default, the selection method is `numTopFeatures`, with the default number of top features set to 50.
The user can choose a selection method using `setSelectorType`.

The number of features to select can be tuned using a held-out validation set.

### Model Fitting

The [`fit`](api/scala/org/apache/spark/mllib/feature/ChiSqSelector.html) method takes
an input of `RDD[LabeledPoint]` with categorical features, learns the summary statistics, and then
returns a `ChiSqSelectorModel` which can transform an input dataset into the reduced feature space.
The `ChiSqSelectorModel` can be applied either to a `Vector` to produce a reduced `Vector`, or to
an `RDD[Vector]` to produce a reduced `RDD[Vector]`.

Note that the user can also construct a `ChiSqSelectorModel` by hand by providing an array of selected feature indices (which must be sorted in ascending order).

### Example

The following example shows the basic use of ChiSqSelector. The data set used has a feature matrix consisting of greyscale values that vary from 0 to 255 for each feature.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [`ChiSqSelector` Scala docs](api/scala/org/apache/spark/mllib/feature/ChiSqSelector.html)
for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/ChiSqSelectorExample.scala %}
</div>

<div data-lang="java" markdown="1">

Refer to the [`ChiSqSelector` Java docs](api/java/org/apache/spark/mllib/feature/ChiSqSelector.html)
for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaChiSqSelectorExample.java %}
</div>
</div>

## ElementwiseProduct

`ElementwiseProduct` multiplies each input vector by a provided "weight" vector, using element-wise
multiplication. In other words, it scales each column of the dataset by a scalar multiplier. This
represents the [Hadamard product](https://en.wikipedia.org/wiki/Hadamard_product_%28matrices%29)
between the input vector, `v` and transforming vector, `scalingVec`, to yield a result vector.

Denoting the `scalingVec` as "`w`", this transformation may be written as:

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

[`ElementwiseProduct`](api/scala/org/apache/spark/mllib/feature/ElementwiseProduct.html) has the following parameter in the constructor:

* `scalingVec`: the transforming vector.

`ElementwiseProduct` implements [`VectorTransformer`](api/scala/org/apache/spark/mllib/feature/VectorTransformer.html) which can apply the weighting on a `Vector` to produce a transformed `Vector` or on an `RDD[Vector]` to produce a transformed `RDD[Vector]`.

### Example

This example below demonstrates how to transform vectors using a transforming vector value.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [`ElementwiseProduct` Scala docs](api/scala/org/apache/spark/mllib/feature/ElementwiseProduct.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/ElementwiseProductExample.scala %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`ElementwiseProduct` Java docs](api/java/org/apache/spark/mllib/feature/ElementwiseProduct.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaElementwiseProductExample.java %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`ElementwiseProduct` Python docs](api/python/reference/api/pyspark.mllib.feature.ElementwiseProduct.html) for more details on the API.

{% include_example python/mllib/elementwise_product_example.py %}
</div>
</div>


## PCA

A feature transformer that projects vectors to a low-dimensional space using PCA.
Details you can read at [dimensionality reduction](mllib-dimensionality-reduction.html).

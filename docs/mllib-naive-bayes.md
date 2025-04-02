---
layout: global
title: Naive Bayes - RDD-based API
displayTitle: Naive Bayes - RDD-based API
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

[Naive Bayes](http://en.wikipedia.org/wiki/Naive_Bayes_classifier) is a simple
multiclass classification algorithm with the assumption of independence between
every pair of features. Naive Bayes can be trained very efficiently. Within a
single pass to the training data, it computes the conditional probability
distribution of each feature given label, and then it applies Bayes' theorem to
compute the conditional probability distribution of label given an observation
and use it for prediction.

`spark.mllib` supports [multinomial naive
Bayes](http://en.wikipedia.org/wiki/Naive_Bayes_classifier#Multinomial_naive_Bayes)
and [Bernoulli naive Bayes](http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html).
These models are typically used for [document classification](http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html).
Within that context, each observation is a document and each
feature represents a term whose value is the frequency of the term (in multinomial naive Bayes) or
a zero or one indicating whether the term was found in the document (in Bernoulli naive Bayes).
Feature values must be nonnegative. The model type is selected with an optional parameter
"multinomial" or "bernoulli" with "multinomial" as the default.
[Additive smoothing](http://en.wikipedia.org/wiki/Lidstone_smoothing) can be used by
setting the parameter $\lambda$ (default to $1.0$). For document classification, the input feature
vectors are usually sparse, and sparse vectors should be supplied as input to take advantage of
sparsity. Since the training data is only used once, it is not necessary to cache it.

## Examples

<div class="codetabs">

<div data-lang="python" markdown="1">

[NaiveBayes](api/python/reference/api/pyspark.mllib.classification.NaiveBayes.html) implements multinomial
naive Bayes. It takes an RDD of
[LabeledPoint](api/python/reference/api/pyspark.mllib.regression.LabeledPoint.html) and an optionally
smoothing parameter `lambda` as input, and output a
[NaiveBayesModel](api/python/reference/api/pyspark.mllib.classification.NaiveBayesModel.html), which can be
used for evaluation and prediction.

Note that the Python API does not yet support model save/load but will in the future.

Refer to the [`NaiveBayes` Python docs](api/python/reference/api/pyspark.mllib.classification.NaiveBayes.html) and [`NaiveBayesModel` Python docs](api/python/reference/api/pyspark.mllib.classification.NaiveBayesModel.html) for more details on the API.

{% include_example python/mllib/naive_bayes_example.py %}
</div>

<div data-lang="scala" markdown="1">

[NaiveBayes](api/scala/org/apache/spark/mllib/classification/NaiveBayes$.html) implements
multinomial naive Bayes. It takes an RDD of
[LabeledPoint](api/scala/org/apache/spark/mllib/regression/LabeledPoint.html) and an optional
smoothing parameter `lambda` as input, an optional model type parameter (default is "multinomial"), and outputs a
[NaiveBayesModel](api/scala/org/apache/spark/mllib/classification/NaiveBayesModel.html), which
can be used for evaluation and prediction.

Refer to the [`NaiveBayes` Scala docs](api/scala/org/apache/spark/mllib/classification/NaiveBayes$.html) and [`NaiveBayesModel` Scala docs](api/scala/org/apache/spark/mllib/classification/NaiveBayesModel.html) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/NaiveBayesExample.scala %}
</div>
<div data-lang="java" markdown="1">

[NaiveBayes](api/java/org/apache/spark/mllib/classification/NaiveBayes.html) implements
multinomial naive Bayes. It takes a Scala RDD of
[LabeledPoint](api/java/org/apache/spark/mllib/regression/LabeledPoint.html) and an
optionally smoothing parameter `lambda` as input, and output a
[NaiveBayesModel](api/java/org/apache/spark/mllib/classification/NaiveBayesModel.html), which
can be used for evaluation and prediction.

Refer to the [`NaiveBayes` Java docs](api/java/org/apache/spark/mllib/classification/NaiveBayes.html) and [`NaiveBayesModel` Java docs](api/java/org/apache/spark/mllib/classification/NaiveBayesModel.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaNaiveBayesExample.java %}
</div>

</div>

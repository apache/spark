---
layout: global
title: Feature Extraction and Transformation - spark.mllib
displayTitle: Feature Extraction and Transformation - spark.mllib
---

* Table of contents
{:toc}


## TF-IDF

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

TF and IDF are implemented in [HashingTF](api/scala/index.html#org.apache.spark.mllib.feature.HashingTF)
and [IDF](api/scala/index.html#org.apache.spark.mllib.feature.IDF).
`HashingTF` takes an `RDD[Iterable[_]]` as the input.
Each record could be an iterable of strings or other types.

Refer to the [`HashingTF` Scala docs](api/scala/index.html#org.apache.spark.mllib.feature.HashingTF) for details on the API.


{% highlight scala %}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

val sc: SparkContext = ...

// Load documents (one per line).
val documents: RDD[Seq[String]] = sc.textFile("...").map(_.split(" ").toSeq)

val hashingTF = new HashingTF()
val tf: RDD[Vector] = hashingTF.transform(documents)
{% endhighlight %}

While applying `HashingTF` only needs a single pass to the data, applying `IDF` needs two passes: 
first to compute the IDF vector and second to scale the term frequencies by IDF.

{% highlight scala %}
import org.apache.spark.mllib.feature.IDF

// ... continue from the previous example
tf.cache()
val idf = new IDF().fit(tf)
val tfidf: RDD[Vector] = idf.transform(tf)
{% endhighlight %}

`spark.mllib`'s IDF implementation provides an option for ignoring terms which occur in less than a
minimum number of documents.  In such cases, the IDF for these terms is set to 0.  This feature
can be used by passing the `minDocFreq` value to the IDF constructor.

{% highlight scala %}
import org.apache.spark.mllib.feature.IDF

// ... continue from the previous example
tf.cache()
val idf = new IDF(minDocFreq = 2).fit(tf)
val tfidf: RDD[Vector] = idf.transform(tf)
{% endhighlight %}
</div>
<div data-lang="python" markdown="1">

TF and IDF are implemented in [HashingTF](api/python/pyspark.mllib.html#pyspark.mllib.feature.HashingTF)
and [IDF](api/python/pyspark.mllib.html#pyspark.mllib.feature.IDF).
`HashingTF` takes an RDD of list as the input.
Each record could be an iterable of strings or other types.


Refer to the [`HashingTF` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.feature.HashingTF) for details on the API.

{% highlight python %}
from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF

sc = SparkContext()

# Load documents (one per line).
documents = sc.textFile("...").map(lambda line: line.split(" "))

hashingTF = HashingTF()
tf = hashingTF.transform(documents)
{% endhighlight %}

While applying `HashingTF` only needs a single pass to the data, applying `IDF` needs two passes: 
first to compute the IDF vector and second to scale the term frequencies by IDF.

{% highlight python %}
from pyspark.mllib.feature import IDF

# ... continue from the previous example
tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)
{% endhighlight %}

`spark.mllib`'s IDF implementation provides an option for ignoring terms which occur in less than a
minimum number of documents.  In such cases, the IDF for these terms is set to 0.  This feature
can be used by passing the `minDocFreq` value to the IDF constructor.

{% highlight python %}
# ... continue from the previous example
tf.cache()
idf = IDF(minDocFreq=2).fit(tf)
tfidf = idf.transform(tf)
{% endhighlight %}
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
Refer to the [`Word2Vec` Scala docs](api/scala/index.html#org.apache.spark.mllib.feature.Word2Vec) for details on the API.

{% highlight scala %}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

val input = sc.textFile("text8").map(line => line.split(" ").toSeq)

val word2vec = new Word2Vec()

val model = word2vec.fit(input)

val synonyms = model.findSynonyms("china", 40)

for((synonym, cosineSimilarity) <- synonyms) {
  println(s"$synonym $cosineSimilarity")
}

// Save and load model
model.save(sc, "myModelPath")
val sameModel = Word2VecModel.load(sc, "myModelPath")
{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
Refer to the [`Word2Vec` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.feature.Word2Vec) for more details on the API.

{% highlight python %}
from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

sc = SparkContext(appName='Word2Vec')
inp = sc.textFile("text8_lines").map(lambda row: row.split(" "))

word2vec = Word2Vec()
model = word2vec.fit(inp)

synonyms = model.findSynonyms('china', 40)

for word, cosine_distance in synonyms:
    print("{}: {}".format(word, cosine_distance))
{% endhighlight %}
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

[`StandardScaler`](api/scala/index.html#org.apache.spark.mllib.feature.StandardScaler) has the
following parameters in the constructor:

* `withMean` False by default. Centers the data with mean before scaling. It will build a dense
output, so this does not work on sparse input and will raise an exception.
* `withStd` True by default. Scales the data to unit standard deviation.

We provide a [`fit`](api/scala/index.html#org.apache.spark.mllib.feature.StandardScaler) method in
`StandardScaler` which can take an input of `RDD[Vector]`, learn the summary statistics, and then
return a model which can transform the input dataset into unit standard deviation and/or zero mean features
depending how we configure the `StandardScaler`.

This model implements [`VectorTransformer`](api/scala/index.html#org.apache.spark.mllib.feature.VectorTransformer)
which can apply the standardization on a `Vector` to produce a transformed `Vector` or on
an `RDD[Vector]` to produce a transformed `RDD[Vector]`.

Note that if the variance of a feature is zero, it will return default `0.0` value in the `Vector`
for that feature.

### Example

The example below demonstrates how to load a dataset in libsvm format, and standardize the features
so that the new features have unit standard deviation and/or zero mean.

<div class="codetabs">
<div data-lang="scala" markdown="1">
Refer to the [`StandardScaler` Scala docs](api/scala/index.html#org.apache.spark.mllib.feature.StandardScaler) for details on the API.

{% highlight scala %}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

val scaler1 = new StandardScaler().fit(data.map(x => x.features))
val scaler2 = new StandardScaler(withMean = true, withStd = true).fit(data.map(x => x.features))
// scaler3 is an identical model to scaler2, and will produce identical transformations
val scaler3 = new StandardScalerModel(scaler2.std, scaler2.mean)

// data1 will be unit variance.
val data1 = data.map(x => (x.label, scaler1.transform(x.features)))

// Without converting the features into dense vectors, transformation with zero mean will raise
// exception on sparse vector.
// data2 will be unit variance and zero mean.
val data2 = data.map(x => (x.label, scaler2.transform(Vectors.dense(x.features.toArray))))
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`StandardScaler` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.feature.StandardScaler) for more details on the API.

{% highlight python %}
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.feature import StandardScaler

data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
label = data.map(lambda x: x.label)
features = data.map(lambda x: x.features)

scaler1 = StandardScaler().fit(features)
scaler2 = StandardScaler(withMean=True, withStd=True).fit(features)
# scaler3 is an identical model to scaler2, and will produce identical transformations
scaler3 = StandardScalerModel(scaler2.std, scaler2.mean)


# data1 will be unit variance.
data1 = label.zip(scaler1.transform(features))

# Without converting the features into dense vectors, transformation with zero mean will raise
# exception on sparse vector.
# data2 will be unit variance and zero mean.
data2 = label.zip(scaler1.transform(features.map(lambda x: Vectors.dense(x.toArray()))))
{% endhighlight %}
</div>
</div>

## Normalizer

Normalizer scales individual samples to have unit $L^p$ norm. This is a common operation for text
classification or clustering. For example, the dot product of two $L^2$ normalized TF-IDF vectors
is the cosine similarity of the vectors.

[`Normalizer`](api/scala/index.html#org.apache.spark.mllib.feature.Normalizer) has the following
parameter in the constructor:

* `p` Normalization in $L^p$ space, $p = 2$ by default.

`Normalizer` implements [`VectorTransformer`](api/scala/index.html#org.apache.spark.mllib.feature.VectorTransformer)
which can apply the normalization on a `Vector` to produce a transformed `Vector` or on
an `RDD[Vector]` to produce a transformed `RDD[Vector]`.

Note that if the norm of the input is zero, it will return the input vector.

### Example

The example below demonstrates how to load a dataset in libsvm format, and normalizes the features
with $L^2$ norm, and $L^\infty$ norm.

<div class="codetabs">
<div data-lang="scala" markdown="1">
Refer to the [`Normalizer` Scala docs](api/scala/index.html#org.apache.spark.mllib.feature.Normalizer) for details on the API.

{% highlight scala %}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

val normalizer1 = new Normalizer()
val normalizer2 = new Normalizer(p = Double.PositiveInfinity)

// Each sample in data1 will be normalized using $L^2$ norm.
val data1 = data.map(x => (x.label, normalizer1.transform(x.features)))

// Each sample in data2 will be normalized using $L^\infty$ norm.
val data2 = data.map(x => (x.label, normalizer2.transform(x.features)))
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`Normalizer` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.feature.Normalizer) for more details on the API.

{% highlight python %}
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.feature import Normalizer

data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
labels = data.map(lambda x: x.label)
features = data.map(lambda x: x.features)

normalizer1 = Normalizer()
normalizer2 = Normalizer(p=float("inf"))

# Each sample in data1 will be normalized using $L^2$ norm.
data1 = labels.zip(normalizer1.transform(features))

# Each sample in data2 will be normalized using $L^\infty$ norm.
data2 = labels.zip(normalizer2.transform(features))
{% endhighlight %}
</div>
</div>

## ChiSqSelector

[Feature selection](http://en.wikipedia.org/wiki/Feature_selection) tries to identify relevant
features for use in model construction. It reduces the size of the feature space, which can improve
both speed and statistical learning behavior.

[`ChiSqSelector`](api/scala/index.html#org.apache.spark.mllib.feature.ChiSqSelector) implements
Chi-Squared feature selection. It operates on labeled data with categorical features.
`ChiSqSelector` orders features based on a Chi-Squared test of independence from the class,
and then filters (selects) the top features which the class label depends on the most.
This is akin to yielding the features with the most predictive power.

The number of features to select can be tuned using a held-out validation set.

### Model Fitting

`ChiSqSelector` takes a `numTopFeatures` parameter specifying the number of top features that
the selector will select.

The [`fit`](api/scala/index.html#org.apache.spark.mllib.feature.ChiSqSelector) method takes
an input of `RDD[LabeledPoint]` with categorical features, learns the summary statistics, and then
returns a `ChiSqSelectorModel` which can transform an input dataset into the reduced feature space.
The `ChiSqSelectorModel` can be applied either to a `Vector` to produce a reduced `Vector`, or to
an `RDD[Vector]` to produce a reduced `RDD[Vector]`.

Note that the user can also construct a `ChiSqSelectorModel` by hand by providing an array of selected feature indices (which must be sorted in ascending order).

### Example

The following example shows the basic use of ChiSqSelector. The data set used has a feature matrix consisting of greyscale values that vary from 0 to 255 for each feature.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [`ChiSqSelector` Scala docs](api/scala/index.html#org.apache.spark.mllib.feature.ChiSqSelector)
for details on the API.

{% highlight scala %}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.feature.ChiSqSelector

// Load some data in libsvm format
val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
// Discretize data in 16 equal bins since ChiSqSelector requires categorical features
// Even though features are doubles, the ChiSqSelector treats each unique value as a category
val discretizedData = data.map { lp =>
  LabeledPoint(lp.label, Vectors.dense(lp.features.toArray.map { x => (x / 16).floor } ) )
}
// Create ChiSqSelector that will select top 50 of 692 features
val selector = new ChiSqSelector(50)
// Create ChiSqSelector model (selecting features)
val transformer = selector.fit(discretizedData)
// Filter the top 50 features from each feature vector
val filteredData = discretizedData.map { lp => 
  LabeledPoint(lp.label, transformer.transform(lp.features)) 
}
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

Refer to the [`ChiSqSelector` Java docs](api/java/org/apache/spark/mllib/feature/ChiSqSelector.html)
for details on the API.

{% highlight java %}
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.ChiSqSelectorModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

SparkConf sparkConf = new SparkConf().setAppName("JavaChiSqSelector");
JavaSparkContext sc = new JavaSparkContext(sparkConf);
JavaRDD<LabeledPoint> points = MLUtils.loadLibSVMFile(sc.sc(),
    "data/mllib/sample_libsvm_data.txt").toJavaRDD().cache();

// Discretize data in 16 equal bins since ChiSqSelector requires categorical features
// Even though features are doubles, the ChiSqSelector treats each unique value as a category
JavaRDD<LabeledPoint> discretizedData = points.map(
    new Function<LabeledPoint, LabeledPoint>() {
      @Override
      public LabeledPoint call(LabeledPoint lp) {
        final double[] discretizedFeatures = new double[lp.features().size()];
        for (int i = 0; i < lp.features().size(); ++i) {
          discretizedFeatures[i] = Math.floor(lp.features().apply(i) / 16);
        }
        return new LabeledPoint(lp.label(), Vectors.dense(discretizedFeatures));
      }
    });

// Create ChiSqSelector that will select top 50 of 692 features
ChiSqSelector selector = new ChiSqSelector(50);
// Create ChiSqSelector model (selecting features)
final ChiSqSelectorModel transformer = selector.fit(discretizedData.rdd());
// Filter the top 50 features from each feature vector
JavaRDD<LabeledPoint> filteredData = discretizedData.map(
    new Function<LabeledPoint, LabeledPoint>() {
      @Override
      public LabeledPoint call(LabeledPoint lp) {
        return new LabeledPoint(lp.label(), transformer.transform(lp.features()));
      }
    }
);

sc.stop();
{% endhighlight %}
</div>
</div>

## ElementwiseProduct

`ElementwiseProduct` multiplies each input vector by a provided "weight" vector, using element-wise
multiplication. In other words, it scales each column of the dataset by a scalar multiplier. This
represents the [Hadamard product](https://en.wikipedia.org/wiki/Hadamard_product_%28matrices%29)
between the input vector, `v` and transforming vector, `scalingVec`, to yield a result vector.
Qu8T948*1#
Denoting the `scalingVec` as "`w`," this transformation may be written as:

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

[`ElementwiseProduct`](api/scala/index.html#org.apache.spark.mllib.feature.ElementwiseProduct) has the following parameter in the constructor:

* `scalingVec`: the transforming vector.

`ElementwiseProduct` implements [`VectorTransformer`](api/scala/index.html#org.apache.spark.mllib.feature.VectorTransformer) which can apply the weighting on a `Vector` to produce a transformed `Vector` or on an `RDD[Vector]` to produce a transformed `RDD[Vector]`.

### Example

This example below demonstrates how to transform vectors using a transforming vector value.

<div class="codetabs">
<div data-lang="scala" markdown="1">

Refer to the [`ElementwiseProduct` Scala docs](api/scala/index.html#org.apache.spark.mllib.feature.ElementwiseProduct) for details on the API.

{% highlight scala %}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.Vectors

// Create some vector data; also works for sparse vectors
val data = sc.parallelize(Array(Vectors.dense(1.0, 2.0, 3.0), Vectors.dense(4.0, 5.0, 6.0)))

val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
val transformer = new ElementwiseProduct(transformingVector)

// Batch transform and per-row transform give the same results:
val transformedData = transformer.transform(data)
val transformedData2 = data.map(x => transformer.transform(x))

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
Refer to the [`ElementwiseProduct` Java docs](api/java/org/apache/spark/mllib/feature/ElementwiseProduct.html) for details on the API.

{% highlight java %}
import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.ElementwiseProduct;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

// Create some vector data; also works for sparse vectors
JavaRDD<Vector> data = sc.parallelize(Arrays.asList(
  Vectors.dense(1.0, 2.0, 3.0), Vectors.dense(4.0, 5.0, 6.0)));
Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);
ElementwiseProduct transformer = new ElementwiseProduct(transformingVector);

// Batch transform and per-row transform give the same results:
JavaRDD<Vector> transformedData = transformer.transform(data);
JavaRDD<Vector> transformedData2 = data.map(
  new Function<Vector, Vector>() {
    @Override
    public Vector call(Vector v) {
      return transformer.transform(v);
    }
  }
);

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
Refer to the [`ElementwiseProduct` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.feature.ElementwiseProduct) for more details on the API.

{% highlight python %}
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.feature import ElementwiseProduct

# Load and parse the data
sc = SparkContext()
data = sc.textFile("data/mllib/kmeans_data.txt")
parsedData = data.map(lambda x: [float(t) for t in x.split(" ")])

# Create weight vector.
transformingVector = Vectors.dense([0.0, 1.0, 2.0])
transformer = ElementwiseProduct(transformingVector)

# Batch transform
transformedData = transformer.transform(parsedData)
# Single-row transform
transformedData2 = transformer.transform(parsedData.first())

{% endhighlight %}
</div>
</div>


## PCA

A feature transformer that projects vectors to a low-dimensional space using PCA.
Details you can read at [dimensionality reduction](mllib-dimensionality-reduction.html).

### Example

The following code demonstrates how to compute principal components on a `Vector`
and use them to project the vectors into a low-dimensional space while keeping associated labels
for calculation a [Linear Regression]((mllib-linear-methods.html))

<div class="codetabs">
<div data-lang="scala" markdown="1">
Refer to the [`PCA` Scala docs](api/scala/index.html#org.apache.spark.mllib.feature.PCA) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.PCA

val data = sc.textFile("data/mllib/ridge-data/lpsa.data").map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
}.cache()

val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

val pca = new PCA(training.first().features.size/2).fit(data.map(_.features))
val training_pca = training.map(p => p.copy(features = pca.transform(p.features)))
val test_pca = test.map(p => p.copy(features = pca.transform(p.features)))

val numIterations = 100
val model = LinearRegressionWithSGD.train(training, numIterations)
val model_pca = LinearRegressionWithSGD.train(training_pca, numIterations)

val valuesAndPreds = test.map { point =>
  val score = model.predict(point.features)
  (score, point.label)
}

val valuesAndPreds_pca = test_pca.map { point =>
  val score = model_pca.predict(point.features)
  (score, point.label)
}

val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
val MSE_pca = valuesAndPreds_pca.map{case(v, p) => math.pow((v - p), 2)}.mean()

println("Mean Squared Error = " + MSE)
println("PCA Mean Squared Error = " + MSE_pca)
{% endhighlight %}
</div>
</div>

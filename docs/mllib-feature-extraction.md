---
layout: global
title: Feature Extraction - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Feature Extraction 
---

* Table of contents
{:toc}

## Word2Vec 

Wor2Vec computes distributed vector representation of words. The main advantage of the distributed representations is that similar words are close in the vector space, which makes generalization to novel patterns easier and model estimation more robust. Distributed vector representation is showed to be useful in many natural language processing applications such as named entity recognition, disambiguation, parsing, tagging and machine translation.

### Model
In our implementation of Word2Vec, we used skip-gram model. The training objective of skip-gram is to learn word vector representations that are good at predicting its context in the same sentence. Mathematically, given a sequence of training words `$w_1, w_2, \dots, w_T$`, the objective of the skip-gram model is to maximize the average log-likelihood 
`\[
\frac{1}{T} \sum_{t = 1}^{T}\sum_{j=-k}^{j=k} \log p(w_{t+j} | w_t)
\]`
where $k$ is the size of the training window.  

In the skip-gram model, every word $w$ is associated with two vectors $u_w$ and $v_w$ which are vector representations of $w$ as word and context respectively. The probability of correctly predicting word $w_i$ given word $w_j$ is determined by the softmax model, which is 
`\[
p(w_i | w_j ) = \frac{\exp(u_{w_i}^{\top}v_{w_j})}{\sum_{l=1}^{V} \exp(u_l^{\top}v_{w_j})}
\]`
where $V$ is the vocabulary size. 

The skip-gram model with softmax is expensive because the cost of computing $\log p(w_i | w_j)$ 
is proportional to $V$, which can be easily in order of millions. To speed up Word2Vec training, we used hierarchical softmax, which reduced the complexity of computing of $\log p(w_i | w_j)$ to
$O(\log(V))$

### Example 

The example below demonstrates how to load a text file, parse it as an RDD of `Seq[String]` and then construct a `Word2Vec` instance with specified parameters. Then we fit a Word2Vec model with the input data. Finally, we display the top 40 similar words to the specified word. 

<div class="codetabs">
<div data-lang="scala">
{% highlight scala %}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.Word2Vec

val input = sc.textFile().map(line => line.split(" ").toSeq)
val size = 100
val startingAlpha = 0.025
val numPartitions = 1
val numIterations = 1

val word2vec = new Word2Vec()
  .setVectorSize(size)
  .setSeed(42L)
  .setNumPartitions(numPartitions)
  .setNumIterations(numIterations)

val model = word2vec.fit(input)

val vec = model.findSynonyms("china", 40)

for((word, cosineSimilarity) <- vec) {
  println(word + " " + cosineSimilarity.toString)
}
{% endhighlight %}
</div>
</div>

## TFIDF

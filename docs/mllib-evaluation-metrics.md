---
layout: global
title: Evaluation Metrics - spark.mllib
displayTitle: Evaluation Metrics - spark.mllib
---

* Table of contents
{:toc}

`spark.mllib` comes with a number of machine learning algorithms that can be used to learn from and make predictions
on data. When these algorithms are applied to build machine learning models, there is a need to evaluate the performance
of the model on some criteria, which depends on the application and its requirements. `spark.mllib` also provides a
suite of metrics for the purpose of evaluating the performance of machine learning models.

Specific machine learning algorithms fall under broader types of machine learning applications like classification,
regression, clustering, etc. Each of these types have well established metrics for performance evaluation and those
metrics that are currently available in `spark.mllib` are detailed in this section.

## Classification model evaluation

While there are many different types of classification algorithms, the evaluation of classification models all share
similar principles. In a [supervised classification problem](https://en.wikipedia.org/wiki/Statistical_classification),
there exists a true output and a model-generated predicted output for each data point. For this reason, the results for
each data point can be assigned to one of four categories:

* True Positive (TP) - label is positive and prediction is also positive
* True Negative (TN) - label is negative and prediction is also negative
* False Positive (FP) - label is negative but prediction is positive
* False Negative (FN) - label is positive but prediction is negative

These four numbers are the building blocks for most classifier evaluation metrics. A fundamental point when considering
classifier evaluation is that pure accuracy (i.e. was the prediction correct or incorrect) is not generally a good metric. The
reason for this is because a dataset may be highly unbalanced. For example, if a model is designed to predict fraud from
a dataset where 95% of the data points are _not fraud_ and 5% of the data points are _fraud_, then a naive classifier
that predicts _not fraud_, regardless of input, will be 95% accurate. For this reason, metrics like
[precision and recall](https://en.wikipedia.org/wiki/Precision_and_recall) are typically used because they take into
account the *type* of error. In most applications there is some desired balance between precision and recall, which can
be captured by combining the two into a single metric, called the [F-measure](https://en.wikipedia.org/wiki/F1_score).

### Binary classification

[Binary classifiers](https://en.wikipedia.org/wiki/Binary_classification) are used to separate the elements of a given
dataset into one of two possible groups (e.g. fraud or not fraud) and is a special case of multiclass classification.
Most binary classification metrics can be generalized to multiclass classification metrics.

#### Threshold tuning

It is import to understand that many classification models actually output a "score" (often times a probability) for
each class, where a higher score indicates higher likelihood. In the binary case, the model may output a probability for
each class: $P(Y=1|X)$ and $P(Y=0|X)$. Instead of simply taking the higher probability, there may be some cases where
the model might need to be tuned so that it only predicts a class when the probability is very high (e.g. only block a
credit card transaction if the model predicts fraud with >90% probability). Therefore, there is a prediction *threshold*
which determines what the predicted class will be based on the probabilities that the model outputs.

Tuning the prediction threshold will change the precision and recall of the model and is an important part of model
optimization. In order to visualize how precision, recall, and other metrics change as a function of the threshold it is
common practice to plot competing metrics against one another, parameterized by threshold. A P-R curve plots (precision,
recall) points for different threshold values, while a
[receiver operating characteristic](https://en.wikipedia.org/wiki/Receiver_operating_characteristic), or ROC, curve
plots (recall, false positive rate) points.

**Available metrics**

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Precision (Positive Predictive Value)</td>
      <td>$PPV=\frac{TP}{TP + FP}$</td>
    </tr>
    <tr>
      <td>Recall (True Positive Rate)</td>
      <td>$TPR=\frac{TP}{P}=\frac{TP}{TP + FN}$</td>
    </tr>
    <tr>
      <td>F-measure</td>
      <td>$F(\beta) = \left(1 + \beta^2\right) \cdot \left(\frac{PPV \cdot TPR}
          {\beta^2 \cdot PPV + TPR}\right)$</td>
    </tr>
    <tr>
      <td>Receiver Operating Characteristic (ROC)</td>
      <td>$FPR(T)=\int^\infty_{T} P_0(T)\,dT \\ TPR(T)=\int^\infty_{T} P_1(T)\,dT$</td>
    </tr>
    <tr>
      <td>Area Under ROC Curve</td>
      <td>$AUROC=\int^1_{0} \frac{TP}{P} d\left(\frac{FP}{N}\right)$</td>
    </tr>
    <tr>
      <td>Area Under Precision-Recall Curve</td>
      <td>$AUPRC=\int^1_{0} \frac{TP}{TP+FP} d\left(\frac{TP}{P}\right)$</td>
    </tr>
  </tbody>
</table>


**Examples**

<div class="codetabs">
The following code snippets illustrate how to load a sample dataset, train a binary classification algorithm on the
data, and evaluate the performance of the algorithm by several binary evaluation metrics.

<div data-lang="scala" markdown="1">
Refer to the [`LogisticRegressionWithLBFGS` Scala docs](api/scala/index.html#org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS) and [`BinaryClassificationMetrics` Scala docs](api/scala/index.html#org.apache.spark.mllib.evaluation.BinaryClassificationMetrics) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/BinaryClassificationMetricsExample.scala %}

</div>

<div data-lang="java" markdown="1">
Refer to the [`LogisticRegressionModel` Java docs](api/java/org/apache/spark/mllib/classification/LogisticRegressionModel.html) and [`LogisticRegressionWithLBFGS` Java docs](api/java/org/apache/spark/mllib/classification/LogisticRegressionWithLBFGS.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaBinaryClassificationMetricsExample.java %}

</div>

<div data-lang="python" markdown="1">
Refer to the [`BinaryClassificationMetrics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.evaluation.BinaryClassificationMetrics) and [`LogisticRegressionWithLBFGS` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.classification.LogisticRegressionWithLBFGS) for more details on the API.

{% include_example python/mllib/binary_classification_metrics_example.py %}
</div>
</div>


### Multiclass classification

A [multiclass classification](https://en.wikipedia.org/wiki/Multiclass_classification) describes a classification
problem where there are $M \gt 2$ possible labels for each data point (the case where $M=2$ is the binary
classification problem). For example, classifying handwriting samples to the digits 0 to 9, having 10 possible classes.

For multiclass metrics, the notion of positives and negatives is slightly different. Predictions and labels can still
be positive or negative, but they must be considered under the context of a particular class. Each label and prediction
take on the value of one of the multiple classes and so they are said to be positive for their particular class and negative
for all other classes. So, a true positive occurs whenever the prediction and the label match, while a true negative
occurs when neither the prediction nor the label take on the value of a given class. By this convention, there can be
multiple true negatives for a given data sample. The extension of false negatives and false positives from the former
definitions of positive and negative labels is straightforward.

#### Label based metrics

Opposed to binary classification where there are only two possible labels, multiclass classification problems have many
possible labels and so the concept of label-based metrics is introduced. Overall precision measures precision across all
labels -  the number of times any class was predicted correctly (true positives) normalized by the number of data
points. Precision by label considers only one class, and measures the number of time a specific label was predicted
correctly normalized by the number of times that label appears in the output.

**Available metrics**

Define the class, or label, set as

$$L = \{\ell_0, \ell_1, \ldots, \ell_{M-1} \} $$

The true output vector $\mathbf{y}$ consists of $N$ elements

$$\mathbf{y}_0, \mathbf{y}_1, \ldots, \mathbf{y}_{N-1} \in L $$

A multiclass prediction algorithm generates a prediction vector $\hat{\mathbf{y}}$ of $N$ elements

$$\hat{\mathbf{y}}_0, \hat{\mathbf{y}}_1, \ldots, \hat{\mathbf{y}}_{N-1} \in L $$

For this section, a modified delta function $\hat{\delta}(x)$ will prove useful

$$\hat{\delta}(x) = \begin{cases}1 & \text{if $x = 0$}, \\ 0 & \text{otherwise}.\end{cases}$$

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Confusion Matrix</td>
      <td>
        $C_{ij} = \sum_{k=0}^{N-1} \hat{\delta}(\mathbf{y}_k-\ell_i) \cdot \hat{\delta}(\hat{\mathbf{y}}_k - \ell_j)\\ \\
         \left( \begin{array}{ccc}
         \sum_{k=0}^{N-1} \hat{\delta}(\mathbf{y}_k-\ell_1) \cdot \hat{\delta}(\hat{\mathbf{y}}_k - \ell_1) & \ldots &
         \sum_{k=0}^{N-1} \hat{\delta}(\mathbf{y}_k-\ell_1) \cdot \hat{\delta}(\hat{\mathbf{y}}_k - \ell_N) \\
         \vdots & \ddots & \vdots \\
         \sum_{k=0}^{N-1} \hat{\delta}(\mathbf{y}_k-\ell_N) \cdot \hat{\delta}(\hat{\mathbf{y}}_k - \ell_1) & \ldots &
         \sum_{k=0}^{N-1} \hat{\delta}(\mathbf{y}_k-\ell_N) \cdot \hat{\delta}(\hat{\mathbf{y}}_k - \ell_N)
         \end{array} \right)$
      </td>
    </tr>
    <tr>
      <td>Overall Precision</td>
      <td>$PPV = \frac{TP}{TP + FP} = \frac{1}{N}\sum_{i=0}^{N-1} \hat{\delta}\left(\hat{\mathbf{y}}_i -
        \mathbf{y}_i\right)$</td>
    </tr>
    <tr>
      <td>Overall Recall</td>
      <td>$TPR = \frac{TP}{TP + FN} = \frac{1}{N}\sum_{i=0}^{N-1} \hat{\delta}\left(\hat{\mathbf{y}}_i -
        \mathbf{y}_i\right)$</td>
    </tr>
    <tr>
      <td>Overall F1-measure</td>
      <td>$F1 = 2 \cdot \left(\frac{PPV \cdot TPR}
          {PPV + TPR}\right)$</td>
    </tr>
    <tr>
      <td>Precision by label</td>
      <td>$PPV(\ell) = \frac{TP}{TP + FP} =
          \frac{\sum_{i=0}^{N-1} \hat{\delta}(\hat{\mathbf{y}}_i - \ell) \cdot \hat{\delta}(\mathbf{y}_i - \ell)}
          {\sum_{i=0}^{N-1} \hat{\delta}(\hat{\mathbf{y}}_i - \ell)}$</td>
    </tr>
    <tr>
      <td>Recall by label</td>
      <td>$TPR(\ell)=\frac{TP}{P} =
          \frac{\sum_{i=0}^{N-1} \hat{\delta}(\hat{\mathbf{y}}_i - \ell) \cdot \hat{\delta}(\mathbf{y}_i - \ell)}
          {\sum_{i=0}^{N-1} \hat{\delta}(\mathbf{y}_i - \ell)}$</td>
    </tr>
    <tr>
      <td>F-measure by label</td>
      <td>$F(\beta, \ell) = \left(1 + \beta^2\right) \cdot \left(\frac{PPV(\ell) \cdot TPR(\ell)}
          {\beta^2 \cdot PPV(\ell) + TPR(\ell)}\right)$</td>
    </tr>
    <tr>
      <td>Weighted precision</td>
      <td>$PPV_{w}= \frac{1}{N} \sum\nolimits_{\ell \in L} PPV(\ell)
          \cdot \sum_{i=0}^{N-1} \hat{\delta}(\mathbf{y}_i-\ell)$</td>
    </tr>
    <tr>
      <td>Weighted recall</td>
      <td>$TPR_{w}= \frac{1}{N} \sum\nolimits_{\ell \in L} TPR(\ell)
          \cdot \sum_{i=0}^{N-1} \hat{\delta}(\mathbf{y}_i-\ell)$</td>
    </tr>
    <tr>
      <td>Weighted F-measure</td>
      <td>$F_{w}(\beta)= \frac{1}{N} \sum\nolimits_{\ell \in L} F(\beta, \ell)
          \cdot \sum_{i=0}^{N-1} \hat{\delta}(\mathbf{y}_i-\ell)$</td>
    </tr>
  </tbody>
</table>

**Examples**

<div class="codetabs">
The following code snippets illustrate how to load a sample dataset, train a multiclass classification algorithm on
the data, and evaluate the performance of the algorithm by several multiclass classification evaluation metrics.

<div data-lang="scala" markdown="1">
Refer to the [`MulticlassMetrics` Scala docs](api/scala/index.html#org.apache.spark.mllib.evaluation.MulticlassMetrics) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/MulticlassMetricsExample.scala %}

</div>

<div data-lang="java" markdown="1">
Refer to the [`MulticlassMetrics` Java docs](api/java/org/apache/spark/mllib/evaluation/MulticlassMetrics.html) for details on the API.

 {% include_example java/org/apache/spark/examples/mllib/JavaMulticlassClassificationMetricsExample.java %}

</div>

<div data-lang="python" markdown="1">
Refer to the [`MulticlassMetrics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.evaluation.MulticlassMetrics) for more details on the API.

{% include_example python/mllib/multi_class_metrics_example.py %}

</div>
</div>

### Multilabel classification

A [multilabel classification](https://en.wikipedia.org/wiki/Multi-label_classification) problem involves mapping
each sample in a dataset to a set of class labels. In this type of classification problem, the labels are not
mutually exclusive. For example, when classifying a set of news articles into topics, a single article might be both
science and politics.

Because the labels are not mutually exclusive, the predictions and true labels are now vectors of label *sets*, rather
than vectors of labels. Multilabel metrics, therefore, extend the fundamental ideas of precision, recall, etc. to
operations on sets. For example, a true positive for a given class now occurs when that class exists in the predicted
set and it exists in the true label set, for a specific data point.

**Available metrics**

Here we define a set $D$ of $N$ documents

$$D = \left\{d_0, d_1, ..., d_{N-1}\right\}$$

Define $L_0, L_1, ..., L_{N-1}$ to be a family of label sets and $P_0, P_1, ..., P_{N-1}$
to be a family of prediction sets where $L_i$ and $P_i$ are the label set and prediction set, respectively, that
correspond to document $d_i$.

The set of all unique labels is given by

$$L = \bigcup_{k=0}^{N-1} L_k$$

The following definition of indicator function $I_A(x)$ on a set $A$ will be necessary

$$I_A(x) = \begin{cases}1 & \text{if $x \in A$}, \\ 0 & \text{otherwise}.\end{cases}$$

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Precision</td><td>$\frac{1}{N} \sum_{i=0}^{N-1} \frac{\left|P_i \cap L_i\right|}{\left|P_i\right|}$</td>
    </tr>
    <tr>
      <td>Recall</td><td>$\frac{1}{N} \sum_{i=0}^{N-1} \frac{\left|L_i \cap P_i\right|}{\left|L_i\right|}$</td>
    </tr>
    <tr>
      <td>Accuracy</td>
      <td>
        $\frac{1}{N} \sum_{i=0}^{N - 1} \frac{\left|L_i \cap P_i \right|}
        {\left|L_i\right| + \left|P_i\right| - \left|L_i \cap P_i \right|}$
      </td>
    </tr>
    <tr>
      <td>Precision by label</td><td>$PPV(\ell)=\frac{TP}{TP + FP}=
          \frac{\sum_{i=0}^{N-1} I_{P_i}(\ell) \cdot I_{L_i}(\ell)}
          {\sum_{i=0}^{N-1} I_{P_i}(\ell)}$</td>
    </tr>
    <tr>
      <td>Recall by label</td><td>$TPR(\ell)=\frac{TP}{P}=
          \frac{\sum_{i=0}^{N-1} I_{P_i}(\ell) \cdot I_{L_i}(\ell)}
          {\sum_{i=0}^{N-1} I_{L_i}(\ell)}$</td>
    </tr>
    <tr>
      <td>F1-measure by label</td><td>$F1(\ell) = 2
                            \cdot \left(\frac{PPV(\ell) \cdot TPR(\ell)}
                            {PPV(\ell) + TPR(\ell)}\right)$</td>
    </tr>
    <tr>
      <td>Hamming Loss</td>
      <td>
        $\frac{1}{N \cdot \left|L\right|} \sum_{i=0}^{N - 1} \left|L_i\right| + \left|P_i\right| - 2\left|L_i
          \cap P_i\right|$
      </td>
    </tr>
    <tr>
      <td>Subset Accuracy</td>
      <td>$\frac{1}{N} \sum_{i=0}^{N-1} I_{\{L_i\}}(P_i)$</td>
    </tr>
    <tr>
      <td>F1 Measure</td>
      <td>$\frac{1}{N} \sum_{i=0}^{N-1} 2 \frac{\left|P_i \cap L_i\right|}{\left|P_i\right| \cdot \left|L_i\right|}$</td>
    </tr>
    <tr>
      <td>Micro precision</td>
      <td>$\frac{TP}{TP + FP}=\frac{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right|}
          {\sum_{i=0}^{N-1} \left|P_i \cap L_i\right| + \sum_{i=0}^{N-1} \left|P_i - L_i\right|}$</td>
    </tr>
    <tr>
      <td>Micro recall</td>
      <td>$\frac{TP}{TP + FN}=\frac{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right|}
        {\sum_{i=0}^{N-1} \left|P_i \cap L_i\right| + \sum_{i=0}^{N-1} \left|L_i - P_i\right|}$</td>
    </tr>
    <tr>
      <td>Micro F1 Measure</td>
      <td>
        $2 \cdot \frac{TP}{2 \cdot TP + FP + FN}=2 \cdot \frac{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right|}{2 \cdot
        \sum_{i=0}^{N-1} \left|P_i \cap L_i\right| + \sum_{i=0}^{N-1} \left|L_i - P_i\right| + \sum_{i=0}^{N-1}
        \left|P_i - L_i\right|}$
      </td>
    </tr>
  </tbody>
</table>

**Examples**

The following code snippets illustrate how to evaluate the performance of a multilabel classifier. The examples
use the fake prediction and label data for multilabel classification that is shown below.

Document predictions:

* doc 0 - predict 0, 1 - class 0, 2
* doc 1 - predict 0, 2 - class 0, 1
* doc 2 - predict none - class 0
* doc 3 - predict 2 - class 2
* doc 4 - predict 2, 0 - class 2, 0
* doc 5 - predict 0, 1, 2 - class 0, 1
* doc 6 - predict 1 - class 1, 2

Predicted classes:

* class 0 - doc 0, 1, 4, 5 (total 4)
* class 1 - doc 0, 5, 6 (total 3)
* class 2 - doc 1, 3, 4, 5 (total 4)

True classes:

* class 0 - doc 0, 1, 2, 4, 5 (total 5)
* class 1 - doc 1, 5, 6 (total 3)
* class 2 - doc 0, 3, 4, 6 (total 4)

<div class="codetabs">

<div data-lang="scala" markdown="1">
Refer to the [`MultilabelMetrics` Scala docs](api/scala/index.html#org.apache.spark.mllib.evaluation.MultilabelMetrics) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/MultiLabelMetricsExample.scala %}

</div>

<div data-lang="java" markdown="1">
Refer to the [`MultilabelMetrics` Java docs](api/java/org/apache/spark/mllib/evaluation/MultilabelMetrics.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaMultiLabelClassificationMetricsExample.java %}

</div>

<div data-lang="python" markdown="1">
Refer to the [`MultilabelMetrics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.evaluation.MultilabelMetrics) for more details on the API.

{% include_example python/mllib/multi_label_metrics_example.py %}

</div>
</div>

### Ranking systems

The role of a ranking algorithm (often thought of as a [recommender system](https://en.wikipedia.org/wiki/Recommender_system))
is to return to the user a set of relevant items or documents based on some training data. The definition of relevance
may vary and is usually application specific. Ranking system metrics aim to quantify the effectiveness of these
rankings or recommendations in various contexts. Some metrics compare a set of recommended documents to a ground truth
set of relevant documents, while other metrics may incorporate numerical ratings explicitly.

**Available metrics**

A ranking system usually deals with a set of $M$ users

$$U = \left\{u_0, u_1, ..., u_{M-1}\right\}$$

Each user ($u_i$) having a set of $N$ ground truth relevant documents

$$D_i = \left\{d_0, d_1, ..., d_{N-1}\right\}$$

And a list of $Q$ recommended documents, in order of decreasing relevance

$$R_i = \left[r_0, r_1, ..., r_{Q-1}\right]$$

The goal of the ranking system is to produce the most relevant set of documents for each user. The relevance of the
sets and the effectiveness of the algorithms can be measured using the metrics listed below.

It is necessary to define a function which, provided a recommended document and a set of ground truth relevant
documents, returns a relevance score for the recommended document.

$$rel_D(r) = \begin{cases}1 & \text{if $r \in D$}, \\ 0 & \text{otherwise}.\end{cases}$$

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th><th>Notes</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>
        Precision at k
      </td>
      <td>
        $p(k)=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{k} \sum_{j=0}^{\text{min}(\left|D\right|, k) - 1} rel_{D_i}(R_i(j))}$
      </td>
      <td>
        <a href="https://en.wikipedia.org/wiki/Information_retrieval#Precision_at_K">Precision at k</a> is a measure of
         how many of the first k recommended documents are in the set of true relevant documents averaged across all
         users. In this metric, the order of the recommendations is not taken into account.
      </td>
    </tr>
    <tr>
      <td>Mean Average Precision</td>
      <td>
        $MAP=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{\left|D_i\right|} \sum_{j=0}^{Q-1} \frac{rel_{D_i}(R_i(j))}{j + 1}}$
      </td>
      <td>
        <a href="https://en.wikipedia.org/wiki/Information_retrieval#Mean_average_precision">MAP</a> is a measure of how
         many of the recommended documents are in the set of true relevant documents, where the
        order of the recommendations is taken into account (i.e. penalty for highly relevant documents is higher).
      </td>
    </tr>
    <tr>
      <td>Normalized Discounted Cumulative Gain</td>
      <td>
        $NDCG(k)=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{IDCG(D_i, k)}\sum_{j=0}^{n-1}
          \frac{rel_{D_i}(R_i(j))}{\text{ln}(j+1)}} \\
        \text{Where} \\
        \hspace{5 mm} n = \text{min}\left(\text{max}\left(|R_i|,|D_i|\right),k\right) \\
        \hspace{5 mm} IDCG(D, k) = \sum_{j=0}^{\text{min}(\left|D\right|, k) - 1} \frac{1}{\text{ln}(j+1)}$
      </td>
      <td>
        <a href="https://en.wikipedia.org/wiki/Information_retrieval#Discounted_cumulative_gain">NDCG at k</a> is a
        measure of how many of the first k recommended documents are in the set of true relevant documents averaged
        across all users. In contrast to precision at k, this metric takes into account the order of the recommendations
        (documents are assumed to be in order of decreasing relevance).
      </td>
    </tr>
  </tbody>
</table>

**Examples**

The following code snippets illustrate how to load a sample dataset, train an alternating least squares recommendation
model on the data, and evaluate the performance of the recommender by several ranking metrics. A brief summary of the
methodology is provided below.

MovieLens ratings are on a scale of 1-5:

 * 5: Must see
 * 4: Will enjoy
 * 3: It's okay
 * 2: Fairly bad
 * 1: Awful

So we should not recommend a movie if the predicted rating is less than 3.
To map ratings to confidence scores, we use:

 * 5 -> 2.5
 * 4 -> 1.5
 * 3 -> 0.5
 * 2 -> -0.5
 * 1 -> -1.5.

This mappings means unobserved entries are generally between It's okay and Fairly bad. The semantics of 0 in this
expanded world of non-positive weights are "the same as never having interacted at all."

<div class="codetabs">

<div data-lang="scala" markdown="1">
Refer to the [`RegressionMetrics` Scala docs](api/scala/index.html#org.apache.spark.mllib.evaluation.RegressionMetrics) and [`RankingMetrics` Scala docs](api/scala/index.html#org.apache.spark.mllib.evaluation.RankingMetrics) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/RankingMetricsExample.scala %}

</div>

<div data-lang="java" markdown="1">
Refer to the [`RegressionMetrics` Java docs](api/java/org/apache/spark/mllib/evaluation/RegressionMetrics.html) and [`RankingMetrics` Java docs](api/java/org/apache/spark/mllib/evaluation/RankingMetrics.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaRankingMetricsExample.java %}

</div>

<div data-lang="python" markdown="1">
Refer to the [`RegressionMetrics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.evaluation.RegressionMetrics) and [`RankingMetrics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.evaluation.RankingMetrics) for more details on the API.

{% include_example python/mllib/ranking_metrics_example.py %}

</div>
</div>

## Regression model evaluation

[Regression analysis](https://en.wikipedia.org/wiki/Regression_analysis) is used when predicting a continuous output
variable from a number of independent variables.

**Available metrics**

<table class="table">
  <thead>
    <tr><th>Metric</th><th>Definition</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Mean Squared Error (MSE)</td>
      <td>$MSE = \frac{\sum_{i=0}^{N-1} (\mathbf{y}_i - \hat{\mathbf{y}}_i)^2}{N}$</td>
    </tr>
    <tr>
      <td>Root Mean Squared Error (RMSE)</td>
      <td>$RMSE = \sqrt{\frac{\sum_{i=0}^{N-1} (\mathbf{y}_i - \hat{\mathbf{y}}_i)^2}{N}}$</td>
    </tr>
    <tr>
      <td>Mean Absolute Error (MAE)</td>
      <td>$MAE=\sum_{i=0}^{N-1} \left|\mathbf{y}_i - \hat{\mathbf{y}}_i\right|$</td>
    </tr>
    <tr>
      <td>Coefficient of Determination $(R^2)$</td>
      <td>$R^2=1 - \frac{MSE}{\text{VAR}(\mathbf{y}) \cdot (N-1)}=1-\frac{\sum_{i=0}^{N-1}
        (\mathbf{y}_i - \hat{\mathbf{y}}_i)^2}{\sum_{i=0}^{N-1}(\mathbf{y}_i-\bar{\mathbf{y}})^2}$</td>
    </tr>
    <tr>
      <td>Explained Variance</td>
      <td>$1 - \frac{\text{VAR}(\mathbf{y} - \mathbf{\hat{y}})}{\text{VAR}(\mathbf{y})}$</td>
    </tr>
  </tbody>
</table>

**Examples**

<div class="codetabs">
The following code snippets illustrate how to load a sample dataset, train a linear regression algorithm on the data,
and evaluate the performance of the algorithm by several regression metrics.

<div data-lang="scala" markdown="1">
Refer to the [`RegressionMetrics` Scala docs](api/scala/index.html#org.apache.spark.mllib.evaluation.RegressionMetrics) for details on the API.

{% include_example scala/org/apache/spark/examples/mllib/RegressionMetricsExample.scala %}

</div>

<div data-lang="java" markdown="1">
Refer to the [`RegressionMetrics` Java docs](api/java/org/apache/spark/mllib/evaluation/RegressionMetrics.html) for details on the API.

{% include_example java/org/apache/spark/examples/mllib/JavaRegressionMetricsExample.java %}

</div>

<div data-lang="python" markdown="1">
Refer to the [`RegressionMetrics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.evaluation.RegressionMetrics) for more details on the API.

{% include_example python/mllib/regression_metrics_example.py %}

</div>
</div>

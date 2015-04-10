---
layout: global
title: Classification and Regression - MLlib
displayTitle: <a href="mllib-guide.md">MLlib</a> - Classification and Regression
---

MLlib supports various methods for 
[binary classification](http://en.wikipedia.org/wiki/Binary_classification),
[multiclass
classification](http://en.wikipedia.org/wiki/Multiclass_classification), and
[regression analysis](http://en.wikipedia.org/wiki/Regression_analysis). The table below outlines
the supported algorithms for each type of problem.

<table class="table">
  <thead>
    <tr><th>Problem Type</th><th>Supported Methods</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Binary Classification</td><td>linear SVMs, logistic regression, decision trees, random forests, gradient-boosted trees, naive Bayes</td>
    </tr>
    <tr>
      <td>Multiclass Classification</td><td>decision trees, random forests, naive Bayes</td>
    </tr>
    <tr>
      <td>Regression</td><td>linear least squares, Lasso, ridge regression, decision trees, random forests, gradient-boosted trees, isotonic regression</td>
    </tr>
  </tbody>
</table>

More details for these methods can be found here:

* [Linear models](mllib-linear-methods.md)
  * [binary classification (SVMs, logistic regression)](mllib-linear-methods.md#binary-classification)
  * [linear regression (least squares, Lasso, ridge)](mllib-linear-methods.md#linear-least-squares-lasso-and-ridge-regression)
* [Decision trees](mllib-decision-tree.md)
* [Ensembles of decision trees](mllib-ensembles.md)
  * [random forests](mllib-ensembles.md#random-forests)
  * [gradient-boosted trees](mllib-ensembles.md#gradient-boosted-trees-gbts)
* [Naive Bayes](mllib-naive-bayes.md)
* [Isotonic regression](mllib-isotonic-regression.md)

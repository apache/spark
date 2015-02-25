---
layout: global
title: Classification and Regression - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Classification and Regression
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
      <td>Binary Classification</td><td>linear SVMs, logistic regression, decision trees, naive Bayes</td>
    </tr>
    <tr>
      <td>Multiclass Classification</td><td>decision trees, naive Bayes</td>
    </tr>
    <tr>
      <td>Regression</td><td>linear least squares, Lasso, ridge regression, decision trees, isotonic regression</td>
    </tr>
  </tbody>
</table>

More details for these methods can be found here:

* [Linear models](mllib-linear-methods.html)
  * [binary classification (SVMs, logistic regression)](mllib-linear-methods.html#binary-classification)
  * [linear regression (least squares, Lasso, ridge)](mllib-linear-methods.html#linear-least-squares-lasso-and-ridge-regression)
* [Decision trees](mllib-decision-tree.html)
* [Naive Bayes](mllib-naive-bayes.html)
* [Isotonic regression](mllib-isotonic-regression.html)

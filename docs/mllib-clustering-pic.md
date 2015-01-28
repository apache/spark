---
layout: global
title: Clustering - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Power Iteration Clustering
---

* Table of contents
{:toc}


## Power Iteration Clustering

Power iteration clustering is a scalable and efficient algorithm for clustering points given pointwise mutual affinity values.  Internally the algorithm:

* computes the Gaussian distance between all pairs of points and represents these distances in an Affinity Matrix
* calculates a Normalized Affinity Matrix
* calculates the principal eigenvalue and eigenvector
* Clusters each of the input points according to their principal eigenvector component value

Details of this algorithm are found within [Power Iteration Clustering, Lin and Cohen]{www.icml2010.org/papers/387.pdf}

Example outputs for a dataset inspired by the paper - but with five clusters instead of three- have he following output from our implementation:

<p style="text-align: center;">
  <img src="img/PIClusteringFiveCirclesInputsAndOutputs.png"
       title="The Property Graph"
       alt="The Property Graph"
       width="50%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>
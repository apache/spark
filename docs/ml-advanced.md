---
layout: global
title: Advanced topics - spark.ml
displayTitle: Advanced topics - spark.ml
---

# Optimization of linear methods

The optimization algorithm underlying the implementation is called
[Orthant-Wise Limited-memory
QuasiNewton](http://research-srv.microsoft.com/en-us/um/people/jfgao/paper/icml07scalable.pdf)
(OWL-QN). It is an extension of L-BFGS that can effectively handle L1
regularization and elastic net.

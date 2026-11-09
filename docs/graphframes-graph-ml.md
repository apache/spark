---
layout: global
displayTitle: Graph Machine Learning
title: Graph Machine Learning
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

Graph machine learning (Graph ML) techniques are essential for analyzing and making predictions on graph-structured data. These methods enable tasks like node classification, link prediction, and community detection by learning representations that capture the structural properties of graphs.

## GraphFrames Approach to Graph ML

GraphFrames positions itself as a "middle-class" graph processing library that bridges the gap between different extremes in the graph ML ecosystem:

- **Lightweight single-node tools** (e.g., node2vec, DeepWalk): Excellent for small to medium-sized graphs but struggle with billion-scale datasets due to memory constraints and lack of distributed computing capabilities.
- **Specialized billion-scale frameworks** (e.g., PyTorch Geometric, PyTorch BigGraph): Highly scalable but complex to set up, requiring specialized infrastructure and deep expertise in distributed systems.

GraphFrames aims to provide **horizontal scalability** while running on existing Apache Spark infrastructure. This approach offers several advantages:

1. **Leverages existing Spark deployments**: No need for specialized hardware or complex setup
2. **Familiar APIs**: Builds on DataFrames and Spark ML for users already in the Spark ecosystem
3. **Automatic scaling**: Inherits Spark's ability to scale from single machines to large clusters
4. **Integration with data pipelines**: Seamlessly works with existing ETL and data processing workflows

## Vertex Representation Learning

Vertex Representation Learning is a graph machine learning technique for generating vector representations (embeddings) of nodes in a graph. These embeddings capture the structural and semantic properties of nodes, making them useful for various downstream tasks:

- **Node Classification**: Predicting labels or properties of nodes based on their structural position and neighborhood in the graph. Common applications include categorizing users in social networks, classifying products in e-commerce graphs, or identifying functional roles of proteins in biological networks.

- **Link Prediction**: Predicting missing or future edges between nodes. This is fundamental to recommendation systems, where the task can be framed as predicting connections in a bipartite graph between users and items. Other applications include predicting future collaborations in co-authorship networks or identifying potential fraud connections in financial transaction graphs.

- **Community Detection**: Identifying clusters or groups of related nodes. While dedicated community detection algorithms exist, embeddings can reveal latent community structures through clustering in the embedding space, useful for segmenting social networks, identifying functional modules in biological systems, or detecting topic groups in citation networks.

- **Anomaly Detection**: Identifying unusual patterns or outliers by detecting nodes with embedding vectors that are significantly different from their neighbors or the overall distribution.

### Random Walk Embeddings

GraphFrames implements a flexible two-stage embedding generation pipeline:

1. **Random Walk Generation**: Generate sequences of node visits by simulating random walks through the graph
2. **Sequence-to-Vector Conversion**: Transform these sequences into dense vector representations using either traditional Word2Vec or more scalable Hash2Vec

This approach contrasts with methods like Fast Random Projection (FastRP), which directly computes embeddings from adjacency matrices. The random walk approach offers greater flexibility:

- **Adjustable context**: Control walk length and restart probabilities to capture different graph properties
- **Model interchangeability**: Swap sequence models based on quality vs. performance requirements
- **Neighborhood aggregation**: Optionally incorporate aggregated neighbor embeddings for improved quality

#### Random Walk Configuration

Random walks are generated using the `RandomWalkWithRestart` algorithm, which includes these key parameters:

- **Restart Probability**: Controls how often walks restart at the starting node (higher values create more local exploration)
- **Number of Walks per Node**: How many independent walks to generate from each node
- **Walk Length**: Implicitly controlled by batch size and number of batches
- **Edge Direction**: Whether to follow edge directions or treat the graph as undirected
- **Maximum Neighbors**: Limits the branching factor for scalability

### Embedding Models

#### Word2Vec

Word2Vec is based on the skip-gram model, which provides high-quality embeddings by learning to predict context nodes from target nodes through gradient descent optimization.

**Strengths:**

- High-quality embeddings that capture semantic relationships
- Well-established model with proven performance across domains
- Smooth optimization through gradient descent

**Limitations:**

- Computationally expensive, requiring more memory and processing time
- Scales to approximately 20 million vertices maximum
- Requires building a vocabulary from all sequences
- Slow training due to gradient computation

#### Hash2Vec

Hash2Vec uses random projection hashing to create embeddings through a much faster, memory-efficient process. It employs feature hashing to map nodes to embedding dimensions.

**Strengths:**

- Extremely fast and memory-efficient
- Excellent horizontal scaling properties
- No vocabulary building required
- Suitable for graphs with billions of vertices

**Limitations:**

- Requires wider embedding dimensions (typically 512+) for good quality
- Lower quality compared to Word2Vec due to sparse, randomized projections
- Less ability to capture fine-grained semantic relationships

### Neighbor Aggregation

To bridge the quality gap between Hash2Vec and Word2Vec, GraphFrames offers optional neighbor aggregation. It works in a similar to the GraphSAGE way: sampling neighbors, aggregate their features. This technique:

1. Samples neighbors using min-hash sampling
2. Computes the average embedding of sampled neighbors
3. Concatenates this aggregated embedding with the node's own embedding

**Benefits:**

- Improves predictive power by over 20% in synthetic tests
- Particularly effective for Hash2Vec (adds neighborhood context that Word2Vec already captures through skip-gram)
- Computationally efficient, especially for Hash2Vec

**When to use:**

- Always enable for Hash2Vec embeddings
- Consider disabling for Word2Vec (redundant with skip-gram learning)

### Scalability Considerations

#### Graph Size Limits

- **Word2Vec**: Practical limit of ~20 million vertices due to vocabulary building and gradient computation
- **Hash2Vec**: Scales to billions of vertices with linear complexity
- **Random Walk Generation**: Scales linearly with number of vertices and walks

#### Performance Tradeoffs

| Aspect              | Word2Vec | Hash2Vec  |
| ------------------- | -------- | --------- |
| Quality             | High     | Moderate  |
| Memory Usage        | High     | Low       |
| Training Speed      | Slow     | Very Fast |
| Horizontal Scaling  | Limited  | Excellent |
| Embedding Dimension | 50-300   | 512+      |
| Out-of-core Support | Limited  | Good      |

#### Infrastructure Requirements

- **Spark Cluster**: Standard Spark deployment (no specialized hardware)
- **Storage**: Temporary storage for random walk sequences (local or distributed)
- **Checkpointing**: Recommended for iterative algorithms to manage lineage
- **Memory**: Hash2Vec works well with memory constraints; Word2Vec benefits from more memory

<div class="codetabs">

<div data-lang="python" markdown="1">

The Python API provides a builder pattern for configuring and running random walk embeddings.

#### Basic Usage

```python
from pyspark.graphframes import GraphFrame
from pyspark.graphframes.graphframe import RandomWalkEmbeddings

# Create your graph
v = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "name"])
e = spark.createDataFrame([(1, 2), (2, 3), (3, 1)], ["src", "dst"])
g = GraphFrame(v, e)

# Configure embeddings pipeline
embeddings = RandomWalkEmbeddings(g)

# Set random walk parameters
embeddings.set_rw_model(
    temporary_prefix="/tmp/rw",
    use_edge_direction=False,
    max_neighbors_per_vertex=50,
    num_walks_per_node=5,
    num_batches=5,
    walks_per_batch=10,
    restart_probability=0.1,
    seed=42
)

# Choose sequence model (Hash2Vec example)
embeddings.set_hash2vec(
    context_size=5,
    num_partitions=5,
    embeddings_dim=512,
    decay_function="gaussian",
    gaussian_sigma=1.0,
    hashing_seed=42,
    sign_seed=18,
    l2_norm=True,
    save_norm=True
)

# Enable neighbor aggregation (recommended for Hash2Vec)
embeddings.set_neighbors_aggregation(
    max_neighbors=50,
    seed=42
)

# Run and get embeddings
result = embeddings.run()
result.show()
```

#### Using Cached Random Walks

If you've previously generated random walks, you can reuse them:

```python
embeddings.use_cached_random_walks("/path/to/cached/walks.parquet")
# Then configure and run as above
```

#### Word2Vec Configuration

```python
embeddings.set_word2vec(
    max_iter=1,
    embeddings_dim=100,
    window_size=5,
    num_partitions=1,
    min_count=5,
    max_sentence_length=1000,
    seed=42,
    step_size=0.025
)

# For Word2Vec, neighbor aggregation may be redundant
embeddings.unset_neighbors_aggregation()
```
</div>

<div data-lang="scala" markdown="1">

The Scala API offers both a high-level builder pattern and direct access to underlying components.

#### Using the Builder Pattern

```scala
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.embeddings.RandomWalkEmbeddings
import org.apache.spark.graphframes.rw.RandomWalkWithRestart
import org.apache.spark.ml.feature.Word2Vec

// Create your graph
val vertices = spark.createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"))).toDF("id", "name")
val edges = spark.createDataFrame(Seq((1L, 2L), (2L, 3L), (3L, 1L))).toDF("src", "dst")
val graph = GraphFrame(vertices, edges)

// Configure random walks
val rwModel = new RandomWalkWithRestart()
  .setRestartProbability(0.15)
  .setMaxNbrsPerVertex(50)
  .setNumWalksPerNode(5)
  .setBatchSize(10)
  .setNumBatches(5)
  .setGlobalSeed(42L)
  .setTemporaryPrefix("/tmp/rw-data")

// Choose sequence model
val sequenceModel = Left(new Word2Vec().setVectorSize(128)) // Word2Vec
// OR
// val sequenceModel = Right(new Hash2Vec().setEmbeddingsDim(512)) // Hash2Vec

// Create and configure embeddings
val embeddings = new RandomWalkEmbeddings(graph)
  .setRandomWalks(rwModel)
  .setSequenceModel(sequenceModel)
  .setAggregateNeighbors(true)
  .setMaxNbrs(50)
  .setSeed(42L)
  .setUseEdgeDirections(false)

// Run and get results
val result = embeddings.run()
result.show()
```

#### Direct Component Usage

For advanced use cases, you can use components directly:

```scala
import org.apache.spark.graphframes.embeddings.Hash2Vec
import org.apache.spark.graphframes.rw.RandomWalkWithRestart

// Generate random walks
val walks = new RandomWalkWithRestart()
  .onGraph(graph)
  .setUseEdgeDirection(false)
  .setRestartProbability(0.2)
  .setGlobalSeed(42L)
  .setTemporaryPrefix("/tmp/walks")
  .run()

// Generate Hash2Vec embeddings
val embeddings = new Hash2Vec()
  .setContextSize(5)
  .setEmbeddingsDim(512)
  .setDoNormalization(true, true)
  .run(walks)

// Optionally aggregate neighbors
val withNeighbors = new SamplingConvolution()
  .onGraph(GraphFrame(embeddings, graph.edges))
  .setFeaturesCol("embedding")
  .setMaxNbrs(50)
  .setSeed(42L)
  .setConcatEmbeddings(true)
  .run()
```

#### Using Cached Walks

```scala
val embeddings = new RandomWalkEmbeddings(graph)
  .useCachedRandomWalks("/path/to/cached/walks")
  .setSequenceModel(Right(new Hash2Vec().setEmbeddingsDim(512)))
  .run()
```

For API details and advanced usecases see:

1. Top-level embeddings: [RandomWalkEmbeddings](api/scala/org/apache/spark/graphframes/embeddings/RandomWalkEmbeddings.html)
2. Hash2vec docs: [Hash2Vec](api/scala/org/apache/spark/graphframes/embeddings/Hash2Vec.html)
3. Random Walks docs: [RandomWalkBase](api/scala/org/apache/spark/graphframes/rw/RandomWalkBase.html)

An implementation of `RandomWalk` supports also continue from the batch. See reference docs for details.
</div>

</div>

### Parameters Reference

#### Random Walk Parameters

| Parameter                  | Type    | Default  | Description                                 |
| -------------------------- | ------- | -------- | ------------------------------------------- |
| `temporary_prefix`         | String  | Required | Path prefix for storing temporary walk data |
| `use_edge_direction`       | Boolean | `false`  | Whether to respect edge directions          |
| `max_neighbors_per_vertex` | Integer | `50`     | Maximum neighbors to consider per vertex    |
| `num_walks_per_node`       | Integer | `5`      | Number of walks to generate per node        |
| `num_batches`              | Integer | `5`      | Number of batches for walk generation       |
| `walks_per_batch`          | Integer | `10`     | Walks per batch (controls walk length)      |
| `restart_probability`      | Float   | `0.1`    | Probability of restarting walk at origin    |
| `seed`                     | Integer | `42`     | Random seed for reproducibility             |

#### Hash2Vec Parameters

| Parameter        | Type    | Default      | Description                                        |
| ---------------- | ------- | ------------ | -------------------------------------------------- |
| `context_size`   | Integer | `5`          | Size of context window for learning                |
| `num_partitions` | Integer | `5`          | Number of partitions for distributed processing    |
| `embeddings_dim` | Integer | `512`        | Dimension of output embeddings                     |
| `decay_function` | String  | `"gaussian"` | Distance decay function (`gaussian` or `constant`) |
| `gaussian_sigma` | Float   | `1.0`        | Sigma parameter for Gaussian decay                 |
| `hashing_seed`   | Integer | `42`         | Seed for feature hashing                           |
| `sign_seed`      | Integer | `18`         | Seed for sign hashing                              |
| `l2_norm`        | Boolean | `true`       | Whether to L2-normalize embeddings                 |
| `save_norm`      | Boolean | `true`       | Whether to preserve normalization safely           |

#### Word2Vec Parameters

| Parameter             | Type    | Default | Description                                |
| --------------------- | ------- | ------- | ------------------------------------------ |
| `max_iter`            | Integer | `1`     | Maximum iterations for training            |
| `embeddings_dim`      | Integer | `100`   | Dimension of output embeddings             |
| `window_size`         | Integer | `5`     | Size of context window                     |
| `num_partitions`      | Integer | `1`     | Number of partitions for training          |
| `min_count`           | Integer | `5`     | Minimum frequency for vocabulary inclusion |
| `max_sentence_length` | Integer | `1000`  | Maximum sequence length                    |
| `seed`                | Integer | `42`    | Random seed                                |
| `step_size`           | Float   | `0.025` | Learning rate for gradient descent         |

#### Neighbor Aggregation Parameters

| Parameter             | Type    | Default | Description                                 |
| --------------------- | ------- | ------- | ------------------------------------------- |
| `aggregate_neighbors` | Boolean | `true`  | Whether to aggregate neighbor embeddings    |
| `max_neighbors`       | Integer | `50`    | Maximum neighbors to sample for aggregation |
| `seed`                | Integer | `42`    | Random seed for neighbor sampling           |

### Recommendations

#### Model Selection Guide

1. **For graphs under 20M vertices with quality as priority**: Use Word2Vec without neighbor aggregation
2. **For large graphs (20M+ vertices) or memory constraints**: Use Hash2Vec with neighbor aggregation
3. **For balanced quality and performance**: Use Hash2Vec with neighbor aggregation and increased embedding dimensions (768+)
4. **When embeddings will be used for similarity search**: Always enable L2 normalization

#### Performance Optimization

1. **Use appropriate storage level**: `MEMORY_AND_DISK` for large graphs, `MEMORY_ONLY` for small graphs
2. **Enable checkpointing**: Set checkpoint interval to 2-5 for iterative algorithms
3. **Adjust partitions**: Match partitions to available cores (typically 2x cores)
4. **Cache intermediate results**: When running multiple experiments, cache the random walks
5. **Monitor memory usage**: Hash2Vec is memory-friendly; Word2Vec may require more heap space

#### Quality Tuning

1. **Increase walk length**: More steps capture longer-range dependencies
2. **Adjust restart probability**: Higher values focus on local neighborhoods
3. **Experiment with context size**: Larger context windows capture more structural information
4. **Try different decay functions**: Gaussian vs. linear for Hash2Vec
5. **Validate with downstream tasks**: Always evaluate embeddings on your specific use case

## References

- Grover, Aditya, and Jure Leskovec. "node2vec: Scalable feature learning for networks." Proceedings of the 22nd ACM SIGKDD international conference on Knowledge discovery and data mining. 2016.
- Perozzi, Bryan, Rami Al-Rfou, and Steven Skiena. "Deepwalk: Online learning of social representations." Proceedings of the 20th ACM SIGKDD international conference on Knowledge discovery and data mining. 2014.
- Lerer, Adam, et al. "Pytorch-biggraph: A large scale graph embedding system." Proceedings of Machine Learning and Systems 1 (2019): 120-131.
- Argerich, Luis, Joaquín Torré Zaffaroni, and Matías J. Cano. "Hash2vec, feature hashing for word embeddings." arXiv preprint arXiv:1608.08940 (2016).
- Mikolov, Tomas, et al. "Efficient estimation of word representations in vector space." arXiv preprint arXiv:1301.3781 (2013).

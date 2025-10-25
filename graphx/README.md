# GraphX

GraphX is Apache Spark's API for graphs and graph-parallel computation.

## Overview

GraphX unifies ETL (Extract, Transform, and Load), exploratory analysis, and iterative graph computation within a single system. It provides:

- **Graph Abstraction**: Efficient representation of property graphs
- **Graph Algorithms**: PageRank, Connected Components, Triangle Counting, and more
- **Pregel API**: For iterative graph computations
- **Graph Builders**: Tools to construct graphs from RDDs or files
- **Graph Operators**: Transformations and structural operations

## Key Concepts

### Property Graph

A directed multigraph with properties attached to each vertex and edge.

**Components:**
- **Vertices**: Nodes with unique IDs and properties
- **Edges**: Directed connections between vertices with properties
- **Triplets**: A view joining vertices and edges

```scala
import org.apache.spark.graphx._

// Create vertices RDD
val vertices: RDD[(VertexId, String)] = sc.parallelize(Array(
  (1L, "Alice"),
  (2L, "Bob"),
  (3L, "Charlie")
))

// Create edges RDD
val edges: RDD[Edge[String]] = sc.parallelize(Array(
  Edge(1L, 2L, "friend"),
  Edge(2L, 3L, "follow")
))

// Build the graph
val graph: Graph[String, String] = Graph(vertices, edges)
```

### Graph Structure

```
Graph[VD, ED]
  - vertices: VertexRDD[VD]  // Vertices with properties of type VD
  - edges: EdgeRDD[ED]        // Edges with properties of type ED
  - triplets: RDD[EdgeTriplet[VD, ED]]  // Combined view
```

## Core Components

### Graph Class

The main graph abstraction.

**Location**: `src/main/scala/org/apache/spark/graphx/Graph.scala`

**Key methods:**
- `vertices: VertexRDD[VD]`: Access vertices
- `edges: EdgeRDD[ED]`: Access edges
- `triplets: RDD[EdgeTriplet[VD, ED]]`: Access triplets
- `mapVertices[VD2](map: (VertexId, VD) => VD2)`: Transform vertex properties
- `mapEdges[ED2](map: Edge[ED] => ED2)`: Transform edge properties
- `subgraph(epred, vpred)`: Create subgraph based on predicates

### VertexRDD

Optimized RDD for vertex data.

**Location**: `src/main/scala/org/apache/spark/graphx/VertexRDD.scala`

**Features:**
- Fast lookups by vertex ID
- Efficient joins with edge data
- Reuse of vertex indices

### EdgeRDD

Optimized RDD for edge data.

**Location**: `src/main/scala/org/apache/spark/graphx/EdgeRDD.scala`

**Features:**
- Compact edge storage
- Fast filtering and mapping
- Efficient partitioning

### EdgeTriplet

Represents a edge with its source and destination vertex properties.

**Structure:**
```scala
class EdgeTriplet[VD, ED] extends Edge[ED] {
  var srcAttr: VD  // Source vertex property
  var dstAttr: VD  // Destination vertex property
  var attr: ED     // Edge property
}
```

## Graph Operators

### Property Operators

```scala
// Map vertex properties
val newGraph = graph.mapVertices((id, attr) => attr.toUpperCase)

// Map edge properties
val newGraph = graph.mapEdges(e => e.attr + "relationship")

// Map triplets (access to src and dst properties)
val newGraph = graph.mapTriplets(triplet => 
  (triplet.srcAttr, triplet.attr, triplet.dstAttr)
)
```

### Structural Operators

```scala
// Reverse edge directions
val reversedGraph = graph.reverse

// Create subgraph
val subgraph = graph.subgraph(
  epred = e => e.srcId != e.dstId,  // No self-loops
  vpred = (id, attr) => attr.length > 0  // Non-empty names
)

// Mask graph (keep only edges/vertices in another graph)
val maskedGraph = graph.mask(subgraph)

// Group edges
val groupedGraph = graph.groupEdges((e1, e2) => e1 + e2)
```

### Join Operators

```scala
// Join vertices with external data
val newData: RDD[(VertexId, NewType)] = ...
val newGraph = graph.joinVertices(newData) {
  (id, oldAttr, newAttr) => (oldAttr, newAttr)
}

// Outer join vertices
val newGraph = graph.outerJoinVertices(newData) {
  (id, oldAttr, newAttr) => newAttr.getOrElse(oldAttr)
}
```

## Graph Algorithms

GraphX includes several common graph algorithms.

**Location**: `src/main/scala/org/apache/spark/graphx/lib/`

### PageRank

Measures the importance of each vertex based on link structure.

```scala
import org.apache.spark.graphx.lib.PageRank

// Static PageRank (fixed iterations)
val ranks = graph.staticPageRank(numIter = 10)

// Dynamic PageRank (convergence-based)
val ranks = graph.pageRank(tol = 0.001)

// Get top ranked vertices
val topRanked = ranks.vertices.top(10)(Ordering.by(_._2))
```

**File**: `src/main/scala/org/apache/spark/graphx/lib/PageRank.scala`

### Connected Components

Finds connected components in the graph.

```scala
import org.apache.spark.graphx.lib.ConnectedComponents

// Find connected components
val cc = graph.connectedComponents()

// Count vertices in each component
val componentCounts = cc.vertices
  .map { case (id, component) => (component, 1) }
  .reduceByKey(_ + _)
```

**File**: `src/main/scala/org/apache/spark/graphx/lib/ConnectedComponents.scala`

### Triangle Counting

Counts triangles (3-cliques) in the graph.

```scala
import org.apache.spark.graphx.lib.TriangleCount

// Count triangles
val triCounts = graph.triangleCount()

// Get vertices with most triangles
val topTriangles = triCounts.vertices.top(10)(Ordering.by(_._2))
```

**File**: `src/main/scala/org/apache/spark/graphx/lib/TriangleCount.scala`

### Label Propagation

Community detection algorithm.

```scala
import org.apache.spark.graphx.lib.LabelPropagation

// Run label propagation
val communities = graph.labelPropagation(maxSteps = 5)

// Group vertices by community
val communityGroups = communities.vertices
  .map { case (id, label) => (label, Set(id)) }
  .reduceByKey(_ ++ _)
```

**File**: `src/main/scala/org/apache/spark/graphx/lib/LabelPropagation.scala`

### Strongly Connected Components

Finds strongly connected components in a directed graph.

```scala
import org.apache.spark.graphx.lib.StronglyConnectedComponents

// Find strongly connected components
val scc = graph.stronglyConnectedComponents(numIter = 10)
```

**File**: `src/main/scala/org/apache/spark/graphx/lib/StronglyConnectedComponents.scala`

### Shortest Paths

Computes shortest paths from source vertices to all reachable vertices.

```scala
import org.apache.spark.graphx.lib.ShortestPaths

// Compute shortest paths from vertices 1 and 2
val landmarks = Seq(1L, 2L)
val results = graph.shortestPaths(landmarks)

// Results contain distance to each landmark
results.vertices.foreach { case (id, distances) =>
  println(s"Vertex $id: $distances")
}
```

**File**: `src/main/scala/org/apache/spark/graphx/lib/ShortestPaths.scala`

## Pregel API

Bulk-synchronous parallel messaging abstraction for iterative graph algorithms.

```scala
def pregel[A: ClassTag](
  initialMsg: A,
  maxIterations: Int = Int.MaxValue,
  activeDirection: EdgeDirection = EdgeDirection.Either
)(
  vprog: (VertexId, VD, A) => VD,
  sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
  mergeMsg: (A, A) => A
): Graph[VD, ED]
```

**Example: Single-Source Shortest Path**

```scala
val sourceId: VertexId = 1L

// Initialize distances
val initialGraph = graph.mapVertices((id, _) =>
  if (id == sourceId) 0.0 else Double.PositiveInfinity
)

// Run Pregel
val sssp = initialGraph.pregel(Double.PositiveInfinity)(
  // Vertex program: update vertex value with minimum distance
  (id, dist, newDist) => math.min(dist, newDist),
  
  // Send message: send distance + edge weight to neighbors
  triplet => {
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  
  // Merge messages: take minimum distance
  (a, b) => math.min(a, b)
)
```

**File**: `src/main/scala/org/apache/spark/graphx/Pregel.scala`

## Graph Builders

### From Edge List

```scala
// Load edge list from file
val graph = GraphLoader.edgeListFile(sc, "path/to/edges.txt")

// Edge file format: source destination
// Example:
// 1 2
// 2 3
// 3 1
```

### From RDDs

```scala
val vertices: RDD[(VertexId, VD)] = ...
val edges: RDD[Edge[ED]] = ...

val graph = Graph(vertices, edges)

// With default vertex property
val graph = Graph.fromEdges(edges, defaultValue = "Unknown")

// From edge tuples
val edgeTuples: RDD[(VertexId, VertexId)] = ...
val graph = Graph.fromEdgeTuples(edgeTuples, defaultValue = 1)
```

## Partitioning Strategies

Efficient graph partitioning is crucial for performance.

**Available strategies:**
- `EdgePartition1D`: Partition edges by source vertex
- `EdgePartition2D`: 2D matrix partitioning
- `RandomVertexCut`: Random edge partitioning (default)
- `CanonicalRandomVertexCut`: Similar to RandomVertexCut but canonical

```scala
import org.apache.spark.graphx.PartitionStrategy

val graph = Graph(vertices, edges)
  .partitionBy(PartitionStrategy.EdgePartition2D)
```

**Location**: `src/main/scala/org/apache/spark/graphx/PartitionStrategy.scala`

## Performance Optimization

### Caching

```scala
// Cache graph in memory
graph.cache()

// Or persist with storage level
graph.persist(StorageLevel.MEMORY_AND_DISK)

// Unpersist when done
graph.unpersist()
```

### Partitioning

```scala
// Repartition for better balance
val partitionedGraph = graph
  .partitionBy(PartitionStrategy.EdgePartition2D, numPartitions = 100)
  .cache()
```

### Checkpointing

For iterative algorithms, checkpoint periodically:

```scala
sc.setCheckpointDir("hdfs://checkpoint")

var graph = initialGraph
for (i <- 1 to maxIterations) {
  // Perform iteration
  graph = performIteration(graph)
  
  // Checkpoint every 10 iterations
  if (i % 10 == 0) {
    graph.checkpoint()
  }
}
```

## Building and Testing

### Build GraphX Module

```bash
# Build graphx module
./build/mvn -pl graphx -am package

# Skip tests
./build/mvn -pl graphx -am -DskipTests package
```

### Run Tests

```bash
# Run all graphx tests
./build/mvn test -pl graphx

# Run specific test suite
./build/mvn test -pl graphx -Dtest=GraphSuite
```

## Source Code Organization

```
graphx/src/main/
├── scala/org/apache/spark/graphx/
│   ├── Graph.scala                     # Main graph class
│   ├── GraphOps.scala                  # Graph operations
│   ├── VertexRDD.scala                 # Vertex RDD
│   ├── EdgeRDD.scala                   # Edge RDD
│   ├── Edge.scala                      # Edge class
│   ├── EdgeTriplet.scala              # Edge triplet
│   ├── Pregel.scala                   # Pregel API
│   ├── GraphLoader.scala              # Graph loading utilities
│   ├── PartitionStrategy.scala        # Partitioning strategies
│   ├── impl/                          # Implementation details
│   │   ├── GraphImpl.scala           # Graph implementation
│   │   ├── VertexRDDImpl.scala       # VertexRDD implementation
│   │   ├── EdgeRDDImpl.scala         # EdgeRDD implementation
│   │   └── ReplicatedVertexView.scala # Vertex replication
│   ├── lib/                           # Graph algorithms
│   │   ├── PageRank.scala
│   │   ├── ConnectedComponents.scala
│   │   ├── TriangleCount.scala
│   │   ├── LabelPropagation.scala
│   │   ├── StronglyConnectedComponents.scala
│   │   └── ShortestPaths.scala
│   └── util/                          # Utilities
│       ├── BytecodeUtils.scala
│       └── GraphGenerators.scala      # Test graph generation
└── resources/
```

## Examples

See [examples/src/main/scala/org/apache/spark/examples/graphx/](../examples/src/main/scala/org/apache/spark/examples/graphx/) for complete examples.

**Key examples:**
- `PageRankExample.scala`: PageRank on social network
- `ConnectedComponentsExample.scala`: Finding connected components
- `SocialNetworkExample.scala`: Complete social network analysis

## Common Use Cases

### Social Network Analysis

```scala
// Load social network
val users: RDD[(VertexId, String)] = sc.textFile("users.txt")
  .map(line => (line.split(",")(0).toLong, line.split(",")(1)))

val relationships: RDD[Edge[String]] = sc.textFile("relationships.txt")
  .map { line =>
    val fields = line.split(",")
    Edge(fields(0).toLong, fields(1).toLong, fields(2))
  }

val graph = Graph(users, relationships)

// Find influential users (PageRank)
val ranks = graph.pageRank(0.001).vertices

// Find communities
val communities = graph.labelPropagation(5)

// Count mutual friends (triangles)
val triangles = graph.triangleCount()
```

### Web Graph Analysis

```scala
// Load web graph
val graph = GraphLoader.edgeListFile(sc, "web-graph.txt")

// Compute PageRank
val ranks = graph.pageRank(0.001)

// Find authoritative pages
val topPages = ranks.vertices.top(100)(Ordering.by(_._2))
```

### Road Network Analysis

```scala
// Vertices are intersections, edges are roads
val roadNetwork: Graph[String, Double] = ...

// Find shortest paths from landmarks
val landmarks = Seq(1L, 2L, 3L)
val distances = roadNetwork.shortestPaths(landmarks)

// Find highly connected intersections
val degrees = roadNetwork.degrees
val busyIntersections = degrees.top(10)(Ordering.by(_._2))
```

## Best Practices

1. **Partition carefully**: Use appropriate partitioning strategy for your workload
2. **Cache graphs**: Cache graphs that are accessed multiple times
3. **Avoid unnecessary materialization**: GraphX uses lazy evaluation
4. **Use GraphLoader**: For simple edge lists, use GraphLoader
5. **Monitor memory**: Graph algorithms can be memory-intensive
6. **Checkpoint long lineages**: Checkpoint periodically in iterative algorithms
7. **Consider edge direction**: Many operations respect edge direction

## Limitations and Considerations

- **No mutable graphs**: Graphs are immutable; modifications create new graphs
- **Memory overhead**: Vertex replication can increase memory usage
- **Edge direction**: Operations may behave differently on directed vs undirected graphs
- **Single-machine graphs**: For small graphs (< 1M edges), NetworkX or igraph may be faster

## Further Reading

- [GraphX Programming Guide](../docs/graphx-programming-guide.md)
- [GraphX Paper](http://www.vldb.org/pvldb/vol7/p1673-xin.pdf)
- [Pregel: A System for Large-Scale Graph Processing](https://kowshik.github.io/JPregel/pregel_paper.pdf)

## Contributing

For contributing to GraphX, see [CONTRIBUTING.md](../CONTRIBUTING.md).

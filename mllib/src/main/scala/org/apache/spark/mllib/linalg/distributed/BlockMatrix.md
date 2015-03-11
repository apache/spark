BlockMatrix.scala
==================

GridPartitioner
---------------

1. There is one class and one object both called "GridPartitioner"
2. The class GridPartitioner
  * It is a package-private class which can only be accessed under directory mllib. By default, classes (and its members) and objects are all public.
  * "val" means the variable cannot change its value after first assignment (like constant variable), while "var" means the variable can change its value many times (like dynamic variable).
  * You can extend a base scala class in similar way in Java but there are two restrictions: (i) method overriding requires the "override" keyword; (ii) only the primary constructor can pass parameters to the base constructor
  * This class extends a Spark base class called "Partitioner":
    + The class Partitioner is in path "core/src/main/scala/org/apache/spark/Partitioner.scala"
    + It is an abstract class that defines how the elements in a key-value pair RDD are partitioned by key. It maps each key to a partition ID, from 0 to numPartitions-1
    + It has two member methods:
    + Other existing classes in Spark extending Partitioner:
  * The object "GridPartitioner"

BlockMatrix
-----------

1. Extends "DistributedMatrix"
  
2. Variables
  * "blocks": key-value pair RDD.
    + key: (block index by row, block index by column)
    + value: block of sub-matrix
  * "rowsPerBlock"
  * "colsPerBlock"
	
3. Public Methods
  * "validate"
  * "cache", "persist"
  * "toCoordinateMatrix", "toIndexedRowMatrix", "toLocalMatrix"
  * "transpose"
  * "add"
  * "multiply": multiply two "BlockMatrix"s - This method leverages Iterator.tabulate()
    + Note that Scala defines both an Iterator trait and object. The multiply method uses an Iterator object. 
    + Iterator.tabulate(n)(i => f(i)) will iterate through the index i to compute f(i) for some function f and produce n items
      1. n = number of items in the returned collection
      2. f = a function applied to each item
      3. i = the index over which the function operates (producing one item per i)
    + In our case, Iterator.tabulate(numRowBlocks)(i => ((i, blockColIndex, blockRowIndex), block))
      1. n = other.numColBlocks
      2. f is a function that uses Scala collections notation. Collections in Scala can be created via parentheses. In this case, the collection is a set of pairs (one pair for each i) in which the first element of the pair is a triple. 

External package
----------------

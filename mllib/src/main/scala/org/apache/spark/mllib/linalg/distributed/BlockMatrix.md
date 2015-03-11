BlockMatrix.scala
==================

GridPartitioner
---------------

1. A class *GridPartitioner* and an object *GridPartitioner*

2. The class *GridPartitioner*

  1. Access modifiers
    * Here, *private[mllib]* is a package-private class which can only be accessed by all classes and objects within the directory mllib.
	* Members of packages, classes, or objects can be labeled with the access modifiers private and protected. These modifiers restrict accesses to the members to certain regions of code. Every member not labeled private or protected is public. There is no explicit modifier for public members.
	* Access modifiers in Scala can be augmented with qualifiers. A modifier of the form private[X] or protected[X] means that access is private or protected “up to” X, where X designates some enclosing package, class or singleton object.
	* This technique is quite useful in large projects that span several packages. It allows you to define things that are visible in several sub-packages of your project but that remain hidden from clients external to your project.
	
  2. Variable declaration and definition
    * Here, *val rows: Int* is a variable declaration in the constructor arguments of *GridPartitioner*
	
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

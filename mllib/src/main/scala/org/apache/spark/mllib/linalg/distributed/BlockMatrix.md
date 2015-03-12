BlockMatrix.scala
==================

GridPartitioner
---------------

1. A class *GridPartitioner* and an object *GridPartitioner*

2. The class *GridPartitioner*

  1. Access modifiers
    * Here, *private[mllib]* is a package-private class which can only be accessed by all classes and objects within the directory mllib.
	* Here， *private val rowPartitions* is a class-private variable (value) which can only be access within this class
	* Members of packages, classes, or objects can be labeled with the access modifiers private and protected. These modifiers restrict accesses to the members to certain regions of code. Every member not labeled private or protected is public. There is no explicit modifier for public members.
	* Access modifiers in Scala can be augmented with qualifiers. A modifier of the form private[X] or protected[X] means that access is private or protected “up to” X, where X designates some enclosing package, class or singleton object.
	* This technique is quite useful in large projects that span several packages. It allows you to define things that are visible in several sub-packages of your project but that remain hidden from clients external to your project.
	
  2. Variable declaration and definition
    * Here, *val rows: Int* is a variable declaration in the constructor arguments of *GridPartitioner*
	* Scala has two kinds of variables, vals (value) and vars (variable). A val is similar to a final variable in Java. Once initialized, a val can never be reassigned. A var, by contrast, is similar to a non-final variable in Java. A var can be reassigned throughout its lifetime.
	* For the variable definition, you can omit the type because Scala has the ability of type inference. When the Scala interpreter (or compiler) can infer types, it is often best to let it do so rather than fill the code with unnecessary, explicit type annotations.
	
  3. Constructor
	* Here, in the arguments of the class constructor, four private variable (value) are defined using very few code to get the same functionality as the more verbose Java version.
	* A constructor takes initial values for those variables defined in arguments as parameters.
	* For example, *class A(n: Int, m: Int)*, the identifiers *n* and *m* in the parentheses after the class name, *A*, are called class parameters. The Scala compiler will gather up these two class parameters and create a primary constructor that takes the same two parameters.
	* In Java, classes have constructors, which can take parameters, whereas in Scala, classes can take parameters directly. The Scala notation is more concise—class parameters can be used directly in the body of the class; there’s no need to define fields and write assignments that copy constructor parameters into fields
  
  4. Inheritance
    * Here, the class *GridPartitioner* extends the Spark abstract class *Partitioner*.
	* Private members of the superclass are not inherited in a subclass.
	* The member of the subclass overrides the member of the superclass with the same name.
  
  5. Spark class - *Partitioner*
    * Defined in `core/src/main/scala/org/apache/spark/Partitioner.scala`
    * An abstract class that defines how the elements in a key-value pair RDD are partitioned by key. It maps each key to a partition ID, from 0 to numPartitions-1.
    * It has two member methods: *def numPartitions: Int* and *def getPartition(key: Any): Int*
    * Other existing classes in Spark extending Partitioner:
      - The class *HashPartitioner*
``` scala
    def numPartitions = partitions
    def getPartition(key: Any): Int = key match {
      case null => 0
      case _ => Utils.nonNegativeMod(key.hashCode, numPartitions) }
```
	
  
  6. Checking preconditions
    * Here, four *require* functions are called inside the class body to check the validation of input values to the class parameters. Tt is the best way to approach the problem caused by the abbreviation of the primary constructor
    * The require method takes one boolean parameter. If the passed value is true, require will return normally. Otherwise, require will prevent the object from being constructed by throwing an IllegalArgumentException
    * The Scala compiler will compile any code you place in the class body, which isn’t part of a field or a method definition, into the primary constructor.
	
  7. Overriding
    * Here, *GridPartitioner* overrides the method in *Partitioner* via a field
	* Scala treats fields and methods more uniformly than Java. Fields and methods belong to the same namespace. This makes it possible for a field to override a parameterless method. For instance, you could change the implementation of contents in class *Partitioner* from a method to a field without having to modify the abstract method definition of contents in class *Element*.
	
  7. Summary

3. The object "GridPartitioner"

BlockMatrix
-----------

1. Extends "DistributedMatrix"
  
2. Variables
  1. "blocks": key-value pair RDD.
    * key: (block index by row, block index by column)
    * value: block of sub-matrix
  2. "rowsPerBlock"
  3. "colsPerBlock"
	
3. Public Methods
  1. "validate"
  2. "cache", "persist"
  3. "toCoordinateMatrix", "toIndexedRowMatrix", "toLocalMatrix"
  4. "transpose"
  5. "add"
  6. "multiply": multiply two "BlockMatrix"s - This method leverages Iterator.tabulate()
    * Note that Scala defines both an Iterator trait and object. The multiply method uses an Iterator object. 
    * Iterator.tabulate(n)(i => f(i)) will iterate through the index i to compute f(i) for some function f and produce n items
      - n = number of items in the returned collection
      - f = a function applied to each item
      - i = the index over which the function operates (producing one item per i)
    * In our case, Iterator.tabulate(numRowBlocks)(i => ((i, blockColIndex, blockRowIndex), block))
      - n = other.numColBlocks
      - f is a function that uses Scala collections notation. Collections in Scala can be created via parentheses. In this case, the collection is a set of pairs (one pair for each i) in which the first element of the pair is a triple. 

External package
----------------

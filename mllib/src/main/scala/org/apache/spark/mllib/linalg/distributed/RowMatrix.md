RowMatrix.scala
==================

RowMatrix
---------------

1. A class ***RowMatrix*** and an singleton object ***RowMatrix***

2. The class ***RowMatrix***

  1. Variable declaration and definition
    * rows is a **RDD of Vectors**
    * nRows is Long, which is determined by the number of records in RDD
    * nCols is Int, which is determined by the number of columns in the first row
    
  2. Constructor
	* Default constructor requires only the RDD as argument. It leaves nRows and nCols to be zero and then determined later 
	* Otherwise RDD nRows and nCols are required for constructing
	
  3. Inheritance
    * ***RowMatrix*** implements the scala trait ***DistributedMatrix***.
    * Private members of the superclass are not inherited in a subclass.
	* The member of the subclass overrides the member of the superclass with the same name.

  4. Methods
    * ***multiply*** require (this) object to have the same columns as the right matrix B's rows. B is required to be a dense matrix. B is broadcasted as Array. Row vector from (this) object and B's dot product.
    
        
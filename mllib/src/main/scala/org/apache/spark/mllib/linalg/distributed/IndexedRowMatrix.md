reading IndexedRowMatrix.scala
--------------------------------

1. A class ***IndexedRowMatrix*** and a case class ***IndexedRow***.

2. The class ***IndexedRowMatrix***

  1. Parameter
    *val rows is a type RDD[IndexedRow], it seems to be used to do pattern matching.
    *nRows is long integer type. It is variable, not value. 
    *nCols is integer type. It is also variable, not value.
    
  2. Constructor
	*  There are two constructor. 
    ** One requires user passes in values for nRows and nCols.
    ** The other only requires the RDD and it will pass 0L(long inteteger zero) and 0 in for nRows and nCols.
	
  3. Inheritance
    * ***IndexedRowMatrix*** inherits from ***DistributedMatrix***.



  4. Methods
    * There are two functions that inherited from the base class overriden. numCols and numRows. Their funtionalities are return their parameters nCols and nRows except throwing exception when nCols or nRows is less than 0.
    * ***toRowMatrix()*** will return a new RowMatrix with 0 rows but nCols columns.
    * ***toBlockMatrix()*** returns a BlockMatrix with 1024 rows and 1024 columns.It uses a method toBlockMatrix(n,m), which take two parameters. This methos is defined a below which calls the methods with same name in CoordinateMatrix.scala.
    *  

    
        
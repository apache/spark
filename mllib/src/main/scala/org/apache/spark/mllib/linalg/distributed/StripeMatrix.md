StripeMatrix.scala
====================

Class StripeMatrix
--------------------

1.	Fields

  1. Input parameters
    * val stripes: RDD[(Int, XXXMatrix)]
    * var rowsPerStripe: Int
	
  2. Public val or var
  
  3. Private val or var

2.	Methods

  1. Constructors

  2. Overriding functions
    * numRows
    * numCols

  3. Private helper functions
    * createPartitioner
    * estimateDim
  
  4. Validation function
    * validate
		
  5. Cache and persist functions
    * cache
    * persist
	
  6. Conversion functions
    * toCoordinateMatrix
    * toIndexdedRowMatrix
    * toLocalMatrix
    * toBreeze

  7. Operation functions
    * def transpose: StripeMatrix
	
      (rowStripeIndex, rowStripeMatrix)  flapMap by splitting rowStripeMatrix =>  [(colStripeIndex1, colBlockMatrix1), ..., (colStripeIndexN, colBlockMatrixN)]  reduceByKey by transposing and catenating colBlockMatrix => (colStripeIndex, T(colBlockMatrix1)::T(colBlockMatrix2)::...::T(colBlockMatrixN))
    
	* def transpose(rowsPerStripe: Int): StripeMatrix
    * def add(other: StripeMatrix): StripeMatrix
	
      (rowStripeIndex, rowStripeMatrix1), (rowStripeIndex, rowStripeMatrix2)  cogroup based on the same partitioner =>  (rowStripeIndex, (rowStripeMatrix1, rowStripeMatrix2))  map by adding two local matrices =>  (rowStripeIndex, rowStripeMatrix1 + rowStripeMatrix2)
	
    * def multiply(other: StripeMatrix): StripeMatrix
	
      * get the look-up table of the left matrix
	  
        (rowStripeIndex, rowStripeMatrixLeft)  map =>  (rowStripeIndex, RS2CLookup)   transpose =>   (colStripeIndex, RS2CLookup)
		
	  * send the right matrix
	  
        (colStripeIndexLeft, RS2CLookup), (rowStripeIndexRight, rowStripeMatrixRight)  cogroup =>  (colStripeIndexLeft, (RS2CLookup, rowStripeMatrixRight))  flapMap by splitting RS2CLookup and reducing rowStripeMatrixRight =>  [(rowStripeIndexLeft1, (colStripeIndexLeft, reducedrowStripeMatrixRight1)), ..., (rowStripeIndexLeftN, (colStripeIndexLeft, reducedrowStripeMatrixRightN))]
	  
	  * multiply the left matrix
	  
        How to reuse the previous partitioner on rowStripeIndex for the one on (rowStripeIndex, colStripeIndex)
	
Class XXXMatrix
----------------
	
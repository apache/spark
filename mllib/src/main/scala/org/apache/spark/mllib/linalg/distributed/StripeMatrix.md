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
    * def transpose(rowsPerStripe: Int): StripeMatrix
      (stripeIndex, XXXMatrix)
	  
    * def add
    * def multiply
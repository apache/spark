import org.apache.spark.SparkContext

import org.jblas.{DoubleMatrix, Singular, MatrixFunctions}

// arguments
val MIN_SVALUE = 0.01 // minimum singular value to recover
val m = 100000
val n = 10
// and a 1-indexed spase matrix.

// TODO: check min svalue
// TODO: check dimensions

// Load and parse the data file
/*val rawdata = sc.textFile("mllib/data/als/test.data")
val data = rawdata.map { line =>
  val parts = line.split(',')
  ((parts(0).toInt, parts(1).toInt), parts(2).toDouble)
}*/

val data = sc.makeRDD(Array.tabulate(m,n){ (a,b)=> ((a+1,b+1),a*b%37) }.flatten )


// Compute A^T A, assuming rows are sparse enough to fit in memory
val rows = data.map(entry =>
	(entry._1._1, (entry._1._2, entry._2))).groupByKey().cache()
val emits = rows.flatMap{ case (rowind, cols)  =>
  cols.flatMap{ case (colind1, mval1) =>
		cols.map{ case (colind2, mval2) =>
			((colind1, colind2), mval1*mval2) } }
}.reduceByKey(_+_)


// Constructi jblas A^T A locally
val ata = DoubleMatrix.zeros(n, n)
for(entry <- emits.toArray) {
  ata.put(entry._1._1-1, entry._1._2-1, entry._2)
}

// Since A^T A is small, we can compute its SVD directly
val svd = Singular.sparseSVD(ata)
val V = svd(0)
val sigma = MatrixFunctions.sqrt(svd(1)).toArray.filter(x => x >= MIN_SVALUE)

// threshold s values
if(sigma.isEmpty) {
	// TODO: return empty
}

// prepare V for returning
val retV = sc.makeRDD(
	Array.tabulate(V.rows, sigma.length){ (i,j) =>
		((i+1, j+1), V.get(i,j)) }.flatten)

val retS = sc.makeRDD(sigma)


// Compute U as U = A V S^-1
// turn V S^-1 into an RDD as a sparse matrix and cache it
val vsirdd = sc.makeRDD(Array.tabulate(V.rows, sigma.length)
		{ (i,j) => ((i+1, j+1), V.get(i,j)/sigma(j))  }.flatten).cache()

// Multiply A by VS^-1
val aCols = data.map(entry => (entry._1._2, (entry._1._1, entry._2)))
val bRows = vsirdd.map(entry => (entry._1._1, (entry._1._2, entry._2))) 
val retU = aCols.join(bRows).map( {case (key, ( (rowInd, rowVal), (colInd, colVal)) ) 
	=> ((rowInd, colInd), rowVal*colVal)}).reduceByKey(_+_)


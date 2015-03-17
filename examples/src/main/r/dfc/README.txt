First, run R. Within the R interpreter, run:

install.packages("Rcpp")
install.packages("svd")

You are now ready to run our implementation of DFC with SparkR! 
Type the following into the command line:

./SparkR DFC.R <master>[<slices>] <slices> <masked_matrix> <iter> <rand>

	<masked_matrix> Should be in matrix market format. We have included
					a sample matrix file, called "example.mat", which is a
					100x100 entry noisy gaussain matrix with 10% revealed.

	<iter> is the number of iterations

	<rand> = T if you want to use the randomized projection, 
			 F otherwise.


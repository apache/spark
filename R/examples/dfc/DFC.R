# Import required matrix and C interface packages
library(SparkR)
library(MASS)
library('Matrix')
library(Rcpp)
library(svd)

# Get the command line arguments
args <- commandArgs(trailing = TRUE)

if (length(args) < 1) {
	print("Usage: DFC.R <master>[<slices>] <slices> <masked_file> <iterations> <randProject=T/F>")
	q("no")
}

# Takes a pair (matrix, iterations) and applies the base factorization algorithm for the specified iterations
factorCols <- function(itersAndMat) {
	iters <- itersAndMat[[1]][[1]]
	mat <- itersAndMat[[1]][[2]]
	UV <- apgBase(mat,iters)
	list(UV)
}

# Fast rBind for a list of matrices by pre-allocating
fastRowBind <- function(matrixList) {
	rows <- sum(unlist(lapply(matrixList, function(mat) dim(mat)[1])))
	cols <- dim(matrixList[[1]])[2]
	mat <- matrix(0.0,nrow=rows,ncol=cols)
	rowIdx <- 1
	for (M in matrixList) {
		Mrows <- dim(M)[1]
		mat[rowIdx:(rowIdx + Mrows - 1),] <- as.matrix(M)
		rowIdx <- rowIdx + Mrows
	}
	mat
}

# Fast cBind for a list of matrices by pre-allocating
fastColBind <- function(matrixList) {
	cols <- sum(unlist(lapply(matrixList, function(mat) dim(mat)[2])))
	rows <- dim(matrixList[[1]])[1]
	mat <- matrix(0.0,nrow=rows,ncol=cols)
	colIdx <- 1
	for (M in matrixList) {
		Mcols <- dim(M)[2]
		mat[,colIdx:(colIdx + Mcols - 1)] <- as.matrix(M)
		colIdx = colIdx + Mcols
	}
	mat
}

# Takes a list of factors of submatrices and projects them
# onto the column space of the first submatrix
# The factors (U,V) should be m-by-r and n-by-r respectively
dfcProject <- function(factorList) {
	tproj <- proc.time()
	U_1 <- factorList[[1]][[1]]
	V_1 <- factorList[[1]][[2]]
	#pseudoinverses
	U_1pinv <- ginv(U_1) 
	V_1pinv <- ginv(V_1)
	numParts <- length(factorList)
	partSize <- dim(U_1)[1] %/% numParts
	r <- dim(U_1)[2]
	# To be returned
	X_A <- U_1
	X_B <- V_1

	for (pair in tail(factorList,-1)){
		U_i <- pair[[1]]
		V_i <- pair[[2]]
		# We want to have U_1*Vhat_i = U_i*V_i, so we basically just solve
		Vhat_i <- t(((V_1pinv %*% V_1)%*%(U_1pinv %*% U_i)) %*% t(V_i))
		X_B <- rBind(X_B, Vhat_i)
	}
	projTime <- as.numeric((proc.time() - tproj)["elapsed"])
	list(X_A, X_B,projTime)
}

# Randomized Projection method for the C step of DFC
dfcRandProject <- function(factorList) {
	trproj <- proc.time()
	V_1 <- factorList[[1]][[2]]
	slices <- length(factorList)
	partSize <- dim(V_1)[1]
	n <- sum(unlist(lapply(factorList, function(UV) dim(UV[[2]])[1])))
	k <- dim(V_1)[2]
	
	# random projection default parameters
	p <- 5
	q <- 2
	
	# Random Gaussian matrix, break into chunks for simpler processing
	G <- Matrix(rnorm(n*(k+p),mean = 0,sd = 1),n,k+p)
	Glist <- lapply(1:slices, function(i) G[(1 + floor((i-1)*n/slices)):floor(i*n/slices),,drop=FALSE])
	
	# Initial QR factorization
	# Y = AG then factor Y = QR
	Ylist <- mapply(function(UV,G) UV[[1]] %*% (t(UV[[2]]) %*% G),factorList,Glist,SIMPLIFY = F)
	Y <- fastColBind(Ylist)
	QR <- qr(Y)
	Q <- qr.Q(QR)
	
	for (j in 1:q) {
		# Yhat = A'*Q then factor Yhat = Qhat*Rhat
		YhatList <- lapply(factorList, function(UV) UV[[2]] %*% (t(UV[[1]]) %*% Q))
		Yhat <- fastRowBind(YhatList)
		QRhat <- qr(Yhat)
		Qhat <- qr.Q(QRhat)
		QhatList <- lapply(1:slices, function(i) Qhat[(1 + floor((i-1)*n/slices)):floor(i*n/slices),,drop=FALSE])
		
		# Y = A*Qhat then factor Y = Q*R
		Ylist <- mapply(function(UV,Qhat) UV[[1]] %*% (t(UV[[2]]) %*% Qhat),factorList,QhatList,SIMPLIFY =F)
		Y <- Reduce('+',Ylist)
		QR <- qr(Y)
		Q <- qr.Q(QR)
	}
	
	# Take only the first k columns of Q
	Q <- Q[,1:k]
	
	# Finally project (Q*Q^+)*M
	Qpinv <- ginv(Q)
	Vlist <- lapply(factorList, function(UV) (Qpinv %*% UV[[1]]) %*% t(UV[[2]]))
	V <- t(fastColBind(Vlist))
	randprojTime <- as.numeric((proc.time() - trproj)["elapsed"])
	list(Q,V,randprojTime) 
}

# Accelerated Proximal Gradient algorithm for factoring.
apgBase <- function(mat,maxiter) {
	
	tbase <- proc.time() # Timing code
	
	# load required packages
	library('Matrix')
	library(Rcpp)
	library(svd)
	
	# Load and compile the fast C++ code
	sourceCpp('maskUV.cpp')
	
	######## Set Initial Parameters #####################################
	m <- dim(mat)[1]
	n <- dim(mat)[2]
	
	IIJJ <- which(mat != 0,arr.ind = T) # list of nonzero indices
	II <- IIJJ[,1] # nonzero row indices
	JJ <- IIJJ[,2] # nonzero col indices
	
	L <- 1 # Lipschitz constant for 1/2*||Ax - b||_2^2
	t <- 1 
	told <- t
	beta <- 0 # beta = (told - 1)/t
	num_sv <- 5 # number of SV to look at
	num_pos_sv <- 5 # initial number of positive singular values
	
	U <- Matrix(0,m,1) # Factor of mat
	Uold <- U
	V <- Matrix(0,n,1) # Factor of mat
	Vold <- V
	mX <- sparseMatrix(m,n,x=0) # Sparse matrix approximating mat
	mXold <- mX # mX of previous iteration
	mY <- mX # Sparse matrix "average" of Xold and X
	
	# SVD Truncation Parameters
	mu0 <- norm(mat,type="F")
	mu <- 0.1*mu0
	muTarget <- 10^(-4)*mu0
	cat("mu :", mu, "\n")
	######################################################################

	for(iter in 1:maxiter) {
		cat("iteration: ",iter,"\n")
		# Get query access to G = Y - 1/L*Grad
		Grad <- mY - mat
		# query oracle to Gk
		f <- function(z) as.numeric((1+beta)*(U %*% (t(V) %*% z)) - beta*(Uold %*% (t(Vold) %*% z)) - 1/L*(Grad %*% z))
		# query oracle to Gk'
		tf <- function(z) as.numeric((1+beta)*(V %*% (t(U) %*% z)) - beta*(Vold %*% (t(Uold) %*% z)) - 1/L*(t(Grad) %*% z))
		
		# Create External Matrix
		G <- extmat(f, tf, m, n)
		
		# Compute partial SVD
		svd <- propack.svd(G, neig = num_sv)
		
		# Update Params
		Uold <- U
		Vold <- V
		mXold <- mX
		told <- t
		
		# Truncate the SV's and update the number of SV's to look at
		s <- svd$d
		Shlf <- sqrt(s[which(s > mu/L)])
		if(num_sv == num_pos_sv) {
			num_sv <- num_pos_sv + 5
		}
		else {
			num_sv <- num_pos_sv + 1
		}
		cat("num sv: ",num_sv,"\n")
		# update number of positive singular values of X^k AFTER the above test
		num_pos_sv <- length(Shlf)
		
		# Compute the factors U and V from the SVD
		Sig <- diag(x = Shlf,num_pos_sv,num_pos_sv)
		U <- (svd$u[,1:num_pos_sv] %*% Sig)
		V <- (svd$v[,1:num_pos_sv] %*% Sig)
		
		# Compute mX = UV' using fast loops in C
		msUV <- maskUV(as.matrix(U),as.matrix(V),II,JJ) # Call into C++ code
		mX <- sparseMatrix(i=II,j=JJ,x=msUV,dims=c(m,n))
		
		# Update Parameters
		t <- (1+sqrt(1+4*t^2))/2
		beta <- (told - 1)/t
		mY <- (1+beta)*mX - beta*mXold	
		mu <- max(0.7*mu,muTarget)
		cat("mu: ",mu,"\n")
	}
	# Output
	apgtime <- as.numeric((proc.time() - tbase)["elapsed"]) #Timing Code
	cat("U: ", dim(U),"\n")
	cat("V: ", dim(V),"\n")
	cat("RMSE for submatrix: ",errorCal(mat,U,V),"\n")

	list(U,V,apgtime)
}
	

# Base stochastic gradient descent algorithm for matrix completion
sgdBase <- function(mat) {

	# Set Parameters
	m <- dim(mat)[1]
	n <- dim(mat)[2]
	lrate <- .04 # learning rate
	k <- .04 # parameter used to minimize over-fitting
	min_impr <- .001 # min improvement
	init <- 0.2 # initial value for features
	rank <- 10 # rank of feature vector
	min_itrs <- 10

	# Initialize
	minval <- min(mat)
	maxval <- max(mat)
	row_feats <- matrix(rnorm(rank*m,mean=0,sd = 0.2/sqrt(sqrt(rank))),rank,m)
	col_feats <- matrix(rnorm(rank*m,mean = 0,sd = 0.2/sqrt(sqrt(rank))),rank,n)
	rmse <- 2.0 # set rmse
	rmse_prev <- 2.0 # set previous rmse

	# Find nonzero entries
	nonzero_rowscols <- which(mat != 0,arr.ind = T)
	nonzero_rows <- nonzero_rowscols[,1]
	nonzero_cols <- nonzero_rowscols[,2]
	nonzero_entries <- mat[nonzero_rowscols]
	num_nonzeros <- length(nonzero_entries)
	
	# Each iterate descends in the space of rank i matrices
	for(i in 1:rank) {
		cat("rank: ", i, "\n")
		t <- 0
		impr <- 0.0
		# Descend as long as we can make improvements
		while(t < min_itrs || impr > min_impr) {
			sq_err <- 0.0
			
			for(j in 1:num_nonzeros) {
				# find predicted val
				predval <- t(row_feats[,nonzero_rows[j] ]) %*% col_feats[,nonzero_cols[j] ]
				
				# apply cut off
				if(predval < minval) { predval <- minval }
				if(predval > maxval) { predval <- maxval }
				
				# Find Error
				err <- nonzero_entries[j] - predval
				sq_err <- sq_err + err*err + k/2.0 * ((row_feats[i, nonzero_rows[j] ])^2) * ((col_feats[i, nonzero_cols[j] ])^2)
				
				# Update row and col features
				new_row_feat <- (1-lrate*k)*row_feats[i, nonzero_rows[j] ] + lrate*err*col_feats[i, nonzero_cols[j] ]
				new_col_feat <- (1-lrate*k)*col_feats[i, nonzero_cols[j] ] + lrate*err*row_feats[i, nonzero_rows[j] ]
				row_feats[i, nonzero_rows[j] ] <- new_row_feat
				col_feats[i, nonzero_cols[j] ] <- new_col_feat
			}
			# Calculate RMSE and improvement
			rmse_prev <- rmse
			rmse <- sqrt(sq_err/num_nonzeros)
			cat("root mean squared error: ",rmse)
			cat("\n")
			impr <- rmse_prev - rmse
			t <- t + 1
		}
	}
	cat("RMSE for submatrix: ",rmse,"\n")
	list(row_feats,col_feats)
}

# Calculate the root mean squared error of the matrix 
# factorization UV' on the non-zero entries of mat
errorCal <- function(mat, U, V){
	# Find nonzero entries
	IIJJ <- which(mat != 0,arr.ind = T)
	numNonzero <- nnzero(mat)
	II <- IIJJ[,1]
	JJ <- IIJJ[,2]
	mX <- sparseMatrix(i=II,j=JJ,x=maskUV(as.matrix(U),as.matrix(V),II,JJ),dims=dim(mat)) # Call into C++ code
	# Frobenius norm/sqrt(num_nonzero) = root mean squared error
	rmse <- norm(mat - mX,type = 'F')/sqrt(numNonzero)
	rmse
}

# Divide factor combine
dfc <- function(mat, sc, slices, iters, randProject=TRUE) {
	sourceCpp('maskUV.cpp')

	# Cut the matrix by columns into several submatrices, one for each slice of computation
	cols <- dim(mat)[2]	
	listMat <- lapply(1:slices, function(i) list(iters,mat[,(1 + floor((i-1)*cols/slices)):floor(i*cols/slices),drop=FALSE]))
	
	tover <- proc.time() # Timing Code
	
	# Create the RDD
	subMatRDD <- parallelize(sc,listMat,slices)
		
	overhead <- as.numeric((proc.time() - tover)["elapsed"]) # Timing Code
	
	# factor each slice
	factorsRDD <- lapplyPartition(subMatRDD,factorCols)
	
	# collect the results
	factorList <- collect(factorsRDD)
	matrixList <- lapply(seq(1,length(factorList)), function(i) list(factorList[[i]][[1]],factorList[[i]][[2]]))
	subTimeList <- lapply(seq(1,length(factorList)), function(i) factorList[[i]][[3]])
		
	# Timing Code
	subTime <- max(unlist(subTimeList))
	cat("Time for subproblems: \n")
	print(subTimeList)

	# Collect the results and project them onto a low rank matrix
	if(randProject) {
		cat("Doing random projection to combine submatrices...\n")
		result <- dfcRandProject(matrixList)
	} else { 
		cat("Projecting all submatrices onto first submatrix...\n")
		result <- dfcProject(matrixList)
	}
	
	# Timing Code
	projTime <- result[[3]]
	cat("Time for collection: ",projTime,"\n")				
		
	# Compute the error in the prediction
	error <- errorCal( mat, result[[1]], result[[2]])
	list(error,overhead,subTime,projTime)
}

# Driver Code #############################################

# Initialize the spark context
sc <- sparkR.init(args[[1]], "DFCR")
slices <- ifelse(length(args) > 1, as.integer(args[[2]]),2)

# Read matrix from file
maskedFile <- args[[3]]
maskedM <- readMM(maskedFile)
dims <- dim(maskedM)[1]
revealedEntries <- nnzero(maskedM)

# Number of Iterations to run base algorithm
iterations <- args[[4]]

# Determine Projection method
randProj <- T
if(length(args) > 4) {
	if(args[[5]] == 'F') {
		randProj <- F
	}
}

ttot <- proc.time() # Timing Code

# Run DFC
outs <- dfc(maskedM, sc, slices, iterations, randProject=randProj)

totalTime <- as.numeric((proc.time() - ttot)["elapsed"]) # Timing Code

cat("RMSE for the entire matrix: ",outs[[1]],"\n")
cat("Total time for DFC: ",totalTime,"\n")
outs

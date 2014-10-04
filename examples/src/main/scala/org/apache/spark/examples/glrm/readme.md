# LowRankModels.jl

[![Build Status](https://travis-ci.org/madeleineudell/LowRankModels.jl.svg?branch=master)](https://travis-ci.org/madeleineudell/LowRankModels.jl)

LowRankModels.jl is a julia package for modeling and fitting generalized low rank models (GLRMs).
GLRMs model a data array by a low rank matrix, and
include many well known models in data analysis, such as 
principal components analysis (PCA), matrix completion, robust PCA,
nonnegative matrix factorization, k-means, and many more.

For more information on GLRMs, see [our paper](http://www.stanford.edu/~udell/doc/glrm.pdf).

LowRankModels.jl makes it easy to mix and match loss functions and regularizers
to construct a model suitable for a particular data set.
In particular, it supports 

* using different loss functions for different columns of the data array, 
  which is useful when data types are heterogeneous 
  (eg, real, boolean, and ordinal columns);
* fitting the model to only *some* of the entries in the talbe, which is useful for data tables with many missing (unobserved) entries; and
* adding offsets and scalings to the model without destroying sparsity,
  which is useful when the data is poorly scaled.

## Installation

To install, just call
```
Pkg.install("https://github.com/madeleineudell/LowRankModels.jl.git")
```
at the julia prompt.

# Generalized Low Rank Models

GLRMs form a low rank model for tabular data `A` with `m` rows and `n` columns, 
which can be input as an array or any array-like object (for example, a data frame).
It is fine if only some of the entries have been observed 
(i.e., the others are missing or `NA`); the GLRM will only be fit on the observed entries `obs`.
The desired model is specified by choosing a rank `k` for the model,
an array of loss functions `losses`, and two regularizers, `rx` and `ry`.
The data is modeled as `XY`, where `X` is a `m`x`k` matrix and `Y` is a `k`x`n` matrix.
`X` and `Y` are found by solving the optimization problem
<!--``\mbox{minimize} \quad \sum_{(i,j) \in \Omega} L_{ij}(x_i y_j, A_{ij}) + \sum_{i=1}^m r_i(x_i) + \sum_{j=1}^n \tilde r_j(y_j)``-->

	minimize sum_{(i,j) in obs} losses[j](x[i,:] y[:,j], A[i,j]) + sum_i rx(x[i,:]) + sum_j ry(y[:,j])

The basic type used by LowRankModels.jl is the GLRM. To form a GLRM,
the user specifies

* the data `A`
* the observed entries `obs`
* the array of loss functions `losses`
* the regularizers `rx` and `ry`
* the rank `k`

`obs` is a list of tuples of the indices of the observed entries in the matrix,
and may be omitted if all the entries in the matrix have been observed.

Losses and regularizers must be of type `Loss` and `Regularizer`, respectively,
and may be chosen from a list of supported losses and regularizers, which include

* quadratic loss `quadratic`
* hinge loss `hinge`
* l1 loss `l1`
* ordinal hinge loss `ordinal_hinge`
* quadratic regularization `quadreg`
* no regularization `zeroreg`
* nonnegative constraint `nonnegative` (eg, for nonnegative matrix factorization)
* 1-sparse constraint `onesparse` (eg, for k-means)

Users may also implement their own losses and regularizers; 
see `loss_and_reg.jl` for more details.

For example, the following code forms a k-means model with `k=5` on the matrix `A`:

	using GLRM
	m,n,k = 100,100,5
	Y = randn(k,n)
	A = zeros(m,n)
	for i=1:m
		A[i,:] = Y[mod(i,k)+1,:]
	end
	losses = fill(quadratic(),n)
	rt = zeroreg()
	r = onesparse() 
	glrm = GLRM(A,losses,rt,r,k)

For more examples, see `examples/simple_glrms.jl`.

To fit the model, call

	X,Y,ch = fit(glrm)

which runs an alternating directions proximal gradient method on `glrm` to find the 
`X` and `Y` minimizing the objective function.
(`ch` gives the convergence history; see 
[Technical details](https://github.com/madeleineudell/LowRankModels.jl#technical-details) 
below for more information.)

# Missing data

If not all entries are present in your data table, just tell the GLRM
which observations to fit the model to by listing their indices in `obs`.
Then initialize the model using

	GLRM(A,obs,losses,rt,r,k)

If `A` is a DataFrame and you just want the model to ignore 
any entry that is of type `NA`, you can use

	obs = observations(A)

# Scaling and offsets

LowRankModels.jl is capable of adding offsets to your model, and of scaling the loss 
functions so all columns have the same pull in the model.
(For more about what these functions do, see the code or the paper.)
Starting with loss functions `losses` and regularizers `r` and `rt`:

* Add an offset to the model (by applying no regularization to the last row 
  of the matrix `Y`) using

	  r, rt = add_offset(r, rt)

* Scale the loss functions `losses` by calling

      equilibrate_variance!(losses, A)

Then form your scaled, offset GLRM with `glrm = GLRM(A,losses,rt,r,k)`

# Fitting DataFrames

Perhaps all this sounds like too much work. Perhaps you happen to have a 
[DataFrame](https://github.com/JuliaStats/DataFrames.jl) `df` lying around 
that you'd like a low rank (eg, `k=2`) model for. For example,

	using RDatasets
	df = RDatasets.dataset("psych", "msq")

Never fear! Just call

	glrm, labels = GLRM(df,2)
	X, Y, ch = fit(glrm)

This will fit a GLRM to your data, using a quadratic loss for real valued columns,
hinge loss for boolean columns, and ordinal hinge loss for integer columns.
(Right now, all other data types are ignored, as are `NA`s.)
It returns the column labels for the columns it fit, along with the model.

You can use the model to get some intuition for the data set. For example,
try plotting the columns of `Y` with the labels; you might see
that similar features are close to each other!

# Technical details

## Optimization

The function `fit` uses an alternating directions proximal gradient method
to minimize the objective. This method is *not* guaranteed to converge to 
the optimum, or even to a local minimum. If your code is not converging
or is converging to a model you dislike, there are a number of parameters you can tweak.

### Warm start

The algorithm starts with `glrm.X` and `glrm.Y` as the initial estimates
for `X` and `Y`. If these are not given explicitly, they will be initialized randomly.
If you have a good guess for a model, try setting them explicitly.
If you think that you're getting stuck in a local minimum, try reinitializing your
GLRM (so as to construct a new initial random point) and see if the model you obtain improves.

You can use the function `fit!` to set the fields `glrm.X` and `glrm.Y` 
after fitting the model. This is particularly useful if you want to use 
the model you generate as a warm start for further iterations.

You can even start with an easy-to-optimize loss function, run `fit!`,
change the loss function (`glrm.losses = newlosses`), 
and keep going from your warm start by calling `fit!` again to fit 
the new loss functions.

### Parameters

Parameters are encoded in a `Parameter` type, which sets the step size `stepsize`,
number of rounds `max_iter` of alternating proximal gradient,
and the convergence tolerance `convergence_tol`.

* The step size controls the speed of convergence. Small step sizes will slow convergence,
while large ones will cause divergence. `stepsize` should be of order 1;
`autoencode` scales it by the maximum number of entries per column or row
so that step *lengths* remain of order 1.
* The algorithm stops when the decrease in the objective per iteration 
is less than `convergence_tol*length(obs)`, 
* or when the maximum number of rounds `max_iter` has been reached.

By default, the parameters are set to use a step size of 1, a maximum of 100 iterations, and a convergence tolerance of .001:

	Params(1,100,.001)

### Convergence
`ch` gives the convergence history so that the success of the optimization can be monitored;
`ch.objective` stores the objective values, and `ch.times` captures the times these objective values were achieved.
Try plotting this to see if you just need to increase `max_iter` to converge to a better model.

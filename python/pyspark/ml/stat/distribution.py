#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.ml.linalg import DenseVector, DenseMatrix, Vector
import numpy as np

__all__ = ['MultivariateGaussian']



class MultivariateGaussian():
    """
    This class provides basic functionality for a Multivariate Gaussian (Normal) Distribution. In
     the event that the covariance matrix is singular, the density will be computed in a
    reduced dimensional subspace under which the distribution is supported.
    (see [[http://en.wikipedia.org/wiki/Multivariate_normal_distribution#Degenerate_case]])

    mu The mean vector of the distribution
    sigma The covariance matrix of the distribution


    >>> mu = Vectors.dense([0.0, 0.0])
    >>> sigma= DenseMatrix(2, 2, [1.0, 1.0, 1.0, 1.0])
    >>> x = Vectors.dense([1.0, 1.0])
    >>> m = MultivariateGaussian(mu, sigma)
    >>> m.pdf(x)
    0.0682586811486

    """

    def __init__(self, mu, sigma):
        """
        __init__(self, mu, sigma)

        mu The mean vector of the distribution
        sigma The covariance matrix of the distribution

        mu and sigma must be instances of DenseVector and DenseMatrix respectively.

        """


        assert (isinstance(mu, DenseVector)), "mu must be a DenseVector Object"
        assert (isinstance(sigma, DenseMatrix)), "sigma must be a DenseMatrix Object"

        sigma_shape=sigma.toArray().shape
        assert (sigma_shape[0]==sigma_shape[1]) , "Covariance matrix must be square"
        assert (sigma_shape[0]==mu.size) , "Mean vector length must match covariance matrix size"

        # initialize eagerly precomputed attributes
        
        self.mu=mu

        # storing sigma as numpy.ndarray
        # furthur calculations are done ndarray only
        self.sigma=sigma.toArray()
        

        # initialize attributes to be computed later

        self.prec_U = None
        self.log_det_cov = None

        # compute distribution dependent constants
        self.__calculateCovarianceConstants()


    def pdf(self,x):
        """
        Returns density of this multivariate Gaussian at a point given by Vector x
        """
        assert (isinstance(x, Vector)), "x must be of Vector Type"
        return float(self.__pdf(x))

    def logpdf(self,x):
        """
        Returns the log-density of this multivariate Gaussian at a point given by Vector x
        """
        assert (isinstance(x, Vector)), "x must be of Vector Type"
        return float(self.__logpdf(x))

    def __calculateCovarianceConstants(self):
        """
        Calculates distribution dependent components used for the density function
        based on scipy multivariate library
        refer https://github.com/scipy/scipy/blob/master/scipy/stats/_multivariate.py
        tested with precision of 9 significant digits(refer testcase)
        

        """

        try :
            # pre-processing input parameters
            # throws ValueError with invalid inputs
            self.dim, self.mu, self.sigma = self.__process_parameters(None, self.mu, self.sigma)

            # return the eigenvalues and eigenvectors 
            # of a Hermitian or symmetric matrix.
            # s =  eigen values
            # u = eigen vectors
            s, u = np.linalg.eigh(self.sigma)

            #Singular values are considered to be non-zero only if 
            #they exceed a tolerance based on machine precision, matrix size, and
            #relation to the maximum singular value (same tolerance used by, e.g., Octave).

            # calculation for machine precision
            t = u.dtype.char.lower()
            factor = {'f': 1E3, 'd': 1E6}
            cond = factor[t] * np.finfo(t).eps

            eps = cond * np.max(abs(s))

            # checkng whether covariance matrix has any non-zero singular values
            if np.min(s) < -eps:
                raise ValueError

            #computing the pseudoinverse
            s_pinv = self.__pinv_1d(s, eps)

            # prec_U ndarray
            # A decomposition such that np.dot(prec_U, prec_U.T)
            # is the precision matrix, i.e. inverse of the covariance matrix.
            self.prec_U = np.multiply(u, np.sqrt(s_pinv))

            #log_det_cov : float
            #Logarithm of the determinant of the covariance matrix
            self.log_det_cov = np.sum(np.log(s[s > eps]))

        except ValueError :
            raise ValueError("Covariance matrix has no non-zero singular values")

    def __pdf(self,x):
        """
        Calculates density at point x using precomputed Constants
        """
        return np.exp(self.__logpdf(x))

    def __logpdf(self,x) :
        """
        Calculates log-density at point x using precomputed Constants

        x  Points at which to evaluate the log of the probability
            density function
        log_det_cov : float
            Logarithm of the determinant of the covariance matrix

        prec_U ndarray
            A decomposition such that np.dot(prec_U, prec_U.T)
            is the precision matrix, i.e. inverse of the covariance matrix.

        """
        x = self.__process_quantiles(x, self.dim)
        dim = x.shape[-1]
        delta = x - self.mu
        maha = np.sum(np.square(np.dot(delta, self.prec_U)), axis=-1)
        return -0.5 * (dim * np.log(2 * np.pi) + self.log_det_cov + maha)


     

    def __process_parameters(self, dim, mean, cov):
        """
        Helper funtion to process input values, based on scipy multivariate

        Infer dimensionality from mean or covariance matrix, ensure that
        mean and covariance are full vector resp. matrix.

        """

        # Try to infer dimensionality
        if dim is None:
            if mean is None:
                if cov is None:
                    dim = 1
                else:
                    cov = np.asarray(cov, dtype=float)
                    if cov.ndim < 2:
                        dim = 1
                    else:
                        dim = cov.shape[0]
            else:
                mean = np.asarray(mean, dtype=float)
                dim = mean.size
        else:
            if not np.isscalar(dim):
                raise ValueError("Dimension of random variable must be a scalar.")

        # Check input sizes and return full arrays for mean and cov if necessary
        if mean is None:
            mean = np.zeros(dim)
        mean = np.asarray(mean, dtype=float)

        if cov is None:
            cov = 1.0
        cov = np.asarray(cov, dtype=float)

        if dim == 1:
            mean.shape = (1,)
            cov.shape = (1, 1)

        if mean.ndim != 1 or mean.shape[0] != dim:
            raise ValueError("Array 'mean' must be vector of length %d." % dim)
        if cov.ndim == 0:
            cov = cov * np.eye(dim)
        elif cov.ndim == 1:
            cov = np.diag(cov)
        else:
            if cov.shape != (dim, dim):
                raise ValueError("Array 'cov' must be at most two-dimensional,"
                                     " but cov.ndim = %d" % cov.ndim)

        return dim, mean, cov

    def __process_quantiles(self, x, dim):
        """
        Helper funtion to process quantiles, based on scipy multivariate

        Adjust quantiles array so that last axis labels the components of
        each data point.
        
        """
        x = np.asarray(x, dtype=float)

        if x.ndim == 0:
            x = x[np.newaxis]
        elif x.ndim == 1:
            if dim == 1:
                x = x[:, np.newaxis]
            else:
                x = x[np.newaxis, :]

        return x


    def __pinv_1d(self, v, eps=1e-5):
        """
        A helper function for computing the pseudoinverse, based on scipy multivariate 

        v : iterable of numbers
            This may be thought of as a vector of eigenvalues or singular values.
        eps : float
            Elements of v smaller than eps are considered negligible.
        returns  1d float ndarray
            A vector of pseudo-inverted numbers. 
        """
        return np.array([0 if abs(x) < eps else 1/x for x in v], dtype=float)



if __name__ == '__main__':
    pass


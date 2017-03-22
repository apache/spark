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
import numpy as np
from pyspark.ml.linalg import DenseVector, DenseMatrix, Vector, Vectors

__all__ = ['MultivariateGaussian']


class MultivariateGaussian():
    """
    This class provides basic functionality for a Multivariate Gaussian (Normal) Distribution. In
     the event that the covariance matrix is singular, the density will be computed in a
    reduced dimensional subspace under which the distribution is supported.
    (see `http://en.wikipedia.org/wiki/Multivariate_normal_distribution#Degenerate_case` )

    mu The mean vector of the distribution
    sigma The covariance matrix of the distribution

    >>> mu = Vectors.dense([0.0, 0.0])
    >>> sigma= DenseMatrix(2, 2, [1.0, 1.0, 1.0, 1.0])
    >>> x = Vectors.dense([1.0, 1.0])
    >>> m = MultivariateGaussian(mu, sigma)
    >>> m.pdf(x)
    0.06825868114860217

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
        assert (sigma.numRows == sigma.numCols), "Covariance matrix must be square"
        assert (sigma.numRows == mu.size), "Mean vector length must match covariance matrix size"

        # initialize eagerly precomputed attributes
        self.mu = mu

        # storing sigma as numpy.ndarray
        # further calculations are done on ndarray only
        self.sigma = sigma.toArray()

        # initialize attributes to be computed later
        self.prec_U = None
        self.log_det_cov = None

        # compute distribution dependent constants
        self.__calculateCovarianceConstants()

    def pdf(self, x):
        """
        Returns density of this multivariate Gaussian at a point given by Vector x
        """
        assert (isinstance(x, Vector)), "x must be of Type Vector"
        assert (self.mu.size == x.size), "Length of vector x must match that of Mean"
        return float(self.__pdf(x))

    def logpdf(self, x):
        """
        Returns the log-density of this multivariate Gaussian at a point given by Vector x
        """
        assert (isinstance(x, Vector)), "x must be of Vector Type"
        assert (self.mu.size == x.size), "Length of vector x must match that of Mean"
        return float(self.__logpdf(x))

    def __calculateCovarianceConstants(self):
        """
        Calculates distribution dependent components used for the density function
        based on scipy multivariate library.
        For further  understanding on covariance constants and calculations,
        see `https://github.com/scipy/scipy/blob/master/scipy/stats/_multivariate.py`
        """
        # calculating the eigenvalues and eigenvectors of covariance matrix
        # s =  eigen values
        # u = eigen vectors
        s, u = np.linalg.eigh(self.sigma)

        # Singular values are considered to be non-zero only if
        # they exceed a tolerance based on machine precision, matrix size, and
        # relation to the maximum singular value (same tolerance used by, e.g., Octave).

        # calculation for machine precision
        t = u.dtype.char.lower()
        factor = {'f': 1E3, 'd': 1E6}
        cond = factor[t] * np.finfo(t).eps

        eps = cond * np.max(abs(s))

        # checking whether covariance matrix has any non-zero singular values
        if np.min(s) < -eps:
            raise ValueError("Covariance matrix has no non-zero singular values")

        # computing the pseudoinverse of s (creates a copy)
        # elements of vector s smaller than eps are considered negligible
        # while remaining elements are inverted
        s_pinv = np.array([0 if abs(x) < eps else 1/x for x in s], dtype=float)

        # prec_U ndarray
        # A decomposition such that np.dot(prec_U, prec_U.T)
        # is the precision matrix, i.e. inverse of the covariance matrix.
        self.prec_U = u * np.sqrt(s_pinv)

        # log_det_cov : float
        # Logarithm of the determinant of the covariance matrix
        self.log_det_cov = np.sum(np.log(s[s > eps]))

    def __pdf(self, x):
        """
        Calculates density at point x using precomputed Constants
        x  Points at which to evaluate the probability density function
        """
        return np.exp(self.__logpdf(x))

    def __logpdf(self, x):
        """
        Calculates log-density at point x using precomputed Constants
        x  Points at which to evaluate the log of the probability
            density function
        """
        dim = x.size
        delta = x - self.mu
        maha = np.sum(np.square(np.dot(delta, self.prec_U)), axis=-1)
        return -0.5 * (dim * np.log(2 * np.pi) + self.log_det_cov + maha)

if __name__ == '__main__':
    import doctest
    import pyspark.ml.stat.distribution
    from pyspark.sql import SparkSession
    globs = pyspark.ml.stat.distribution.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder\
        .master("local[2]")\
        .appName("ml.stat.distribution tests")\
        .getOrCreate()
    sc = spark.sparkContext
    globs['sc'] = sc
    globs['spark'] = spark
    import tempfile
    temp_path = tempfile.mkdtemp()
    globs['temp_path'] = temp_path
    try:
        (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
        spark.stop()
    finally:
        from shutil import rmtree
        try:
            rmtree(temp_path)
        except OSError:
            pass
    if failure_count:
        exit(-1)

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

from collections import namedtuple

__all__ = ['MultivariateGaussian']


class MultivariateGaussian(namedtuple('MultivariateGaussian', ['mu', 'sigma'])):

    """Represents a (mu, sigma) tuple

    >>> m = MultivariateGaussian(Vectors.dense([11,12]),DenseMatrix(2, 2, (1.0, 3.0, 5.0, 2.0)))
    >>> (m.mu, m.sigma.toArray())
    (DenseVector([11.0, 12.0]), array([[ 1., 5.],[ 3., 2.]]))
    >>> (m[0], m[1])
    (DenseVector([11.0, 12.0]), array([[ 1., 5.],[ 3., 2.]]))
    """

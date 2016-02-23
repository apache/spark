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

from __future__ import print_function

# $example on$
import numpy as np
import scipy.sparse as sps

from pyspark.mllib.linalg import Vectors
# $example off$

if __name__ == "__main__":

    # $example on$
    # Use a NumPy array as a dense vector.
    dv1 = np.array([1.0, 0.0, 3.0])
    # Use a Python list as a dense vector.
    dv2 = [1.0, 0.0, 3.0]
    # Create a SparseVector.
    sv1 = Vectors.sparse(3, [0, 2], [1.0, 3.0])
    # Use a single-column SciPy csc_matrix as a sparse vector.
    sv2 = sps.csc_matrix((np.array([1.0, 3.0]), np.array([0, 2]), np.array([0, 2])), shape=(3, 1))
    # $example off$

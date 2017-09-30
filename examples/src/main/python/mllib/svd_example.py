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

from pyspark import SparkContext
# $example on$
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import RowMatrix
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PythonSVDExample")

    # $example on$
    rows = sc.parallelize([
        Vectors.sparse(5, {1: 1.0, 3: 7.0}),
        Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
        Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    ])

    mat = RowMatrix(rows)

    # Compute the top 5 singular values and corresponding singular vectors.
    svd = mat.computeSVD(5, computeU=True)
    U = svd.U       # The U factor is a RowMatrix.
    s = svd.s       # The singular values are stored in a local dense vector.
    V = svd.V       # The V factor is a local dense matrix.
    # $example off$
    collected = U.rows.collect()
    print("U factor is:")
    for vector in collected:
        print(vector)
    print("Singular values are: %s" % s)
    print("V factor is:\n%s" % V)
    sc.stop()

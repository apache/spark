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
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
# $example off$

if __name__ == "__main__":

    # $example on$
    # Create a labeled point with a positive label and a dense feature vector.
    pos = LabeledPoint(1.0, [1.0, 0.0, 3.0])

    # Create a labeled point with a negative label and a sparse feature vector.
    neg = LabeledPoint(0.0, SparseVector(3, [0, 2], [1.0, 3.0]))
    # $example off$

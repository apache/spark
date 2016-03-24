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

from pyspark import SparkContext
# $example on$
from pyspark.mllib.stat import KernelDensity
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="KernelDensityEstimationExample")  # SparkContext

    # $example on$
    # an RDD of sample data
    data = sc.parallelize([1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 6.0, 7.0, 8.0, 9.0, 9.0])

    # Construct the density estimator with the sample data and a standard deviation for the Gaussian
    # kernels
    kd = KernelDensity()
    kd.setSample(data)
    kd.setBandwidth(3.0)

    # Find density estimates for the given values
    densities = kd.estimate([-1.0, 2.0, 5.0])
    # $example off$

    print(densities)

    sc.stop()

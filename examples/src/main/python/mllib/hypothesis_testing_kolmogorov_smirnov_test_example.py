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
from pyspark.mllib.stat import Statistics
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="HypothesisTestingKolmogorovSmirnovTestExample")

    # $example on$
    parallelData = sc.parallelize([0.1, 0.15, 0.2, 0.3, 0.25])

    # run a KS test for the sample versus a standard normal distribution
    testResult = Statistics.kolmogorovSmirnovTest(parallelData, "norm", 0, 1)
    # summary of the test including the p-value, test statistic, and null hypothesis
    # if our p-value indicates significance, we can reject the null hypothesis
    # Note that the Scala functionality of calling Statistics.kolmogorovSmirnovTest with
    # a lambda to calculate the CDF is not made available in the Python API
    print(testResult)
    # $example off$

    sc.stop()

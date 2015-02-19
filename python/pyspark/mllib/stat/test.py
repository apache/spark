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

from pyspark.mllib.common import JavaModelWrapper


__all__ = ["ChiSqTestResult"]


class ChiSqTestResult(JavaModelWrapper):
    """
    .. note:: Experimental

    Object containing the test results for the chi-squared hypothesis test.
    """
    @property
    def method(self):
        """
        Name of the test method
        """
        return self._java_model.method()

    @property
    def pValue(self):
        """
        The probability of obtaining a test statistic result at least as
        extreme as the one that was actually observed, assuming that the
        null hypothesis is true.
        """
        return self._java_model.pValue()

    @property
    def degreesOfFreedom(self):
        """
        Returns the degree(s) of freedom of the hypothesis test.
        Return type should be Number(e.g. Int, Double) or tuples of Numbers.
        """
        return self._java_model.degreesOfFreedom()

    @property
    def statistic(self):
        """
        Test statistic.
        """
        return self._java_model.statistic()

    @property
    def nullHypothesis(self):
        """
        Null hypothesis of the test.
        """
        return self._java_model.nullHypothesis()

    def __str__(self):
        return self._java_model.toString()

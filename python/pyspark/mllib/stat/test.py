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

from typing import Generic, Tuple, TypeVar

from pyspark.mllib.common import inherit_doc, JavaModelWrapper


__all__ = ["ChiSqTestResult", "KolmogorovSmirnovTestResult"]

DF = TypeVar("DF", int, float, Tuple[int, ...], Tuple[float, ...])


class TestResult(JavaModelWrapper, Generic[DF]):
    """
    Base class for all test results.
    """

    @property
    def pValue(self) -> float:
        """
        The probability of obtaining a test statistic result at least as
        extreme as the one that was actually observed, assuming that the
        null hypothesis is true.
        """
        return self._java_model.pValue()

    @property
    def degreesOfFreedom(self) -> DF:
        """
        Returns the degree(s) of freedom of the hypothesis test.
        Return type should be Number(e.g. Int, Double) or tuples of Numbers.
        """
        return self._java_model.degreesOfFreedom()

    @property
    def statistic(self) -> float:
        """
        Test statistic.
        """
        return self._java_model.statistic()

    @property
    def nullHypothesis(self) -> str:
        """
        Null hypothesis of the test.
        """
        return self._java_model.nullHypothesis()

    def __str__(self) -> str:
        return self._java_model.toString()


@inherit_doc
class ChiSqTestResult(TestResult[int]):
    """
    Contains test results for the chi-squared hypothesis test.
    """

    @property
    def method(self) -> str:
        """
        Name of the test method
        """
        return self._java_model.method()


@inherit_doc
class KolmogorovSmirnovTestResult(TestResult[int]):
    """
    Contains test results for the Kolmogorov-Smirnov test.
    """

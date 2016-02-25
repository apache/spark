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

from pyspark import SparkContext, since
from pyspark.mllib.common import inherit_doc, JavaModelWrapper
from pyspark.streaming.dstream import DStream


__all__ = ["ChiSqTestResult", "KolmogorovSmirnovTestResult", "BinarySample", "StreamingTest",
           "StreamingTestResult"]


class TestResult(JavaModelWrapper):
    """
    Base class for all test results.
    """

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


@inherit_doc
class ChiSqTestResult(TestResult):
    """
    Contains test results for the chi-squared hypothesis test.
    """

    @property
    def method(self):
        """
        Name of the test method
        """
        return self._java_model.method()


@inherit_doc
class KolmogorovSmirnovTestResult(TestResult):
    """
    Contains test results for the Kolmogorov-Smirnov test.
    """


@since('2.0.0')
class BinarySample(namedtuple("BinarySample", ["isExperiment", "value"])):
    """
    Represents a (isExperiment, value) tuple.

    .. versionadded:: 2.0.0
    """

    def __reduce__(self):
        return BinarySample, (bool(self.isExperiment), float(self.value))


@since('2.0.0')
class StreamingTestResult(namedtuple("StreamingTestResult",
                                     ["pValue", "degreesOfFreedom", "statistic", "method",
                                      "nullHypothesis"])):
    """
    Contains test results for StreamingTest.

    .. versionadded:: 2.0.0
    """

    def __reduce__(self):
        return StreamingTestResult, (float(self.pValue),
                                     float(self.degreesOfFreedom), float(self.statistic),
                                     str(self.method), str(self.nullHypothesis))


@since('2.0.0')
class StreamingTest(object):
    """
    .. note:: Experimental

    Online 2-sample significance testing for a stream of (Boolean, Double) pairs. The Boolean
    identifies which sample each observation comes from, and the Double is the numeric value of the
    observation.

    To address novelty affects, the `peacePeriod` specifies a set number of initial RDD batches of
    the DStream to be dropped from significance testing.

    The `windowSize` sets the number of batches each significance test is to be performed over. The
    window is sliding with a stride length of 1 batch. Setting windowSize to 0 will perform
    cumulative processing, using all batches seen so far.

    Different tests may be used for assessing statistical significance depending on assumptions
    satisfied by data. For more details, see StreamingTestMethod. The `testMethod` specifies
    which test will be used.

    .. versionadded:: 2.0.0
    """

    def __init__(self):
        self._peacePeriod = 0
        self._windowSize = 0
        self._testMethod = "welch"

    @since('2.0.0')
    def setPeacePeriod(self, peacePeriod):
        """
        Update peacePeriod
        :param peacePeriod:
          Set number of initial RDD batches of the DStream to be dropped from significance testing.
        """
        self._peacePeriod = peacePeriod

    @since('2.0.0')
    def setWindowSize(self, windowSize):
        """
        Update windowSize
        :param windowSize:
          Set the number of batches each significance test is to be performed over.
        """
        self._windowSize = windowSize

    @since('2.0.0')
    def setTestMethod(self, testMethod):
        """
        Update test method
        :param testMethod:
          Currently supported tests: `welch`, `student`.
        """
        assert(testMethod in ("welch", "student"),
               "Currently supported tests: \"welch\", \"student\"")
        self._testMethod = testMethod

    @since('2.0.0')
    def registerStream(self, data):
        """
        Register a data stream to get its test result.

        :param data:
          The input data stream, each element is a BinarySample instance.
        """
        self._validate(data)
        sc = SparkContext._active_spark_context

        streamingTest = sc._jvm.org.apache.spark.mllib.stat.test.StreamingTest()
        streamingTest.setPeacePeriod(self._peacePeriod)
        streamingTest.setWindowSize(self._windowSize)
        streamingTest.setTestMethod(self._testMethod)

        javaDStream = sc._jvm.SerDe.pythonToJava(data._jdstream, True)
        testResult = streamingTest.registerStream(javaDStream)
        pythonTestResult = sc._jvm.SerDe.javaToPython(testResult)

        pyResult = DStream(pythonTestResult, data._ssc, data._jrdd_deserializer)

        return pyResult

    @classmethod
    def _validate(cls, samples):
        if isinstance(samples, DStream):
            pass
        else:
            raise TypeError("BinarySample should be represented by a DStream, "
                            "but got %s." % type(samples))

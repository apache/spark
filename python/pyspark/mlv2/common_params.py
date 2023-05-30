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

from pyspark.ml.param import Param, Params, TypeConverters


class HasNumTrainWorkers(Params):

    numTrainWorkers: "Param[int]" = Param(
        Params._dummy(),
        "numTrainWorkers",
        "number of training workers",
        typeConverter=TypeConverters.toInt,
    )

    def __init__(self) -> None:
        super(HasNumTrainWorkers, self).__init__()

    def getNumTrainWorkers(self) -> int:
        """
        Gets the value of numTrainWorkers or its default value.
        """
        return self.getOrDefault(self.numTrainWorkers)


class HasBatchSize(Params):

    batchSize: "Param[int]" = Param(
        Params._dummy(),
        "batchSize",
        "number of training batch size",
        typeConverter=TypeConverters.toInt,
    )

    def __init__(self) -> None:
        super(HasBatchSize, self).__init__()

    def getBatchSize(self) -> int:
        """
        Gets the value of numBatchSize or its default value.
        """
        return self.getOrDefault(self.batchSize)


class HasLearningRate(Params):

    learningRate: "Param[float]" = Param(
        Params._dummy(),
        "learningRate",
        "learning rate for training",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self) -> None:
        super(HasLearningRate, self).__init__()

    def getLearningRate(self) -> int:
        """
        Gets the value of learningRate or its default value.
        """
        return self.getOrDefault(self.learningRate)


class HasMomentum(Params):

    momentum: "Param[float]" = Param(
        Params._dummy(),
        "momentum",
        "momentum for training optimizer",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self) -> None:
        super(HasMomentum, self).__init__()

    def getMomentum(self) -> int:
        """
        Gets the value of momentum or its default value.
        """
        return self.getOrDefault(self.momentum)

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

import itertools

__all__ = ['ParamGridBuilder']


class ParamGridBuilder(object):
    """
    Builder for a param grid used in grid search-based model selection.

    >>> from classification import LogisticRegression
    >>> lr = LogisticRegression()
    >>> output = ParamGridBuilder().baseOn({lr.labelCol: 'l'}) \
            .baseOn([lr.predictionCol, 'p']) \
            .addGrid(lr.regParam, [1.0, 2.0, 3.0]) \
            .addGrid(lr.maxIter, [1, 5]) \
            .addGrid(lr.featuresCol, ['f']) \
            .build()
    >>> expected = [ \
{lr.regParam: 1.0, lr.featuresCol: 'f', lr.maxIter: 1, lr.labelCol: 'l', lr.predictionCol: 'p'}, \
{lr.regParam: 2.0, lr.featuresCol: 'f', lr.maxIter: 1, lr.labelCol: 'l', lr.predictionCol: 'p'}, \
{lr.regParam: 3.0, lr.featuresCol: 'f', lr.maxIter: 1, lr.labelCol: 'l', lr.predictionCol: 'p'}, \
{lr.regParam: 1.0, lr.featuresCol: 'f', lr.maxIter: 5, lr.labelCol: 'l', lr.predictionCol: 'p'}, \
{lr.regParam: 2.0, lr.featuresCol: 'f', lr.maxIter: 5, lr.labelCol: 'l', lr.predictionCol: 'p'}, \
{lr.regParam: 3.0, lr.featuresCol: 'f', lr.maxIter: 5, lr.labelCol: 'l', lr.predictionCol: 'p'}]
    >>> len(output) == len(expected)
    True
    >>> all([m in expected for m in output])
    True
    """

    def __init__(self):
        self._param_grid = {}

    def addGrid(self, param, values):
        """
        Sets the given parameters in this grid to fixed values.
        """
        self._param_grid[param] = values

        return self

    def baseOn(self, *args):
        """
        Sets the given parameters in this grid to fixed values.
        Accepts either a parameter dictionary or a list of (parameter, value) pairs.
        """
        if isinstance(args[0], dict):
            self.baseOn(*args[0].items())
        else:
            for (param, value) in args:
                self.addGrid(param, [value])

        return self

    def build(self):
        """
        Builds and returns all combinations of parameters specified
        by the param grid.
        """
        keys = self._param_grid.keys()
        grid_values = self._param_grid.values()
        return [dict(zip(keys, prod)) for prod in itertools.product(*grid_values)]


if __name__ == "__main__":
    import doctest
    doctest.testmod()
